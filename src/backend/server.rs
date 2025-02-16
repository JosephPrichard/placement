use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use axum::extract::{State, WebSocketUpgrade};
use axum::extract::ws::{Message, WebSocket};
use axum::response::IntoResponse;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use redis::{Client, Connection};
use tokio::sync::{broadcast, Mutex};
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;
use tracing::log::{debug, error, info, warn};
use crate::backend::broadcast::broadcast_message;
use crate::backend::cache::get_tile_group;
use crate::backend::models::{Input, TextOutput, DrawMsg, GroupKey, ServiceError, BinaryOutput};
use crate::backend::query::QueryStore;

#[derive(Clone)]
pub struct Server {
    pub tx: Arc<broadcast::Sender<DrawMsg>>,
    pub client: Arc<Client>,
    pub query: Arc<QueryStore>,
}

async fn handle_recv(server: Server, mut rx: SplitStream<WebSocket>, tx: Arc<Mutex<SplitSink<WebSocket, Message>>>) -> JoinHandle<()> {
    tokio::spawn(async move {
        info!("Starting the recv loop");
        let conn = &mut server.client.get_connection().unwrap();
        while let Some(result) = rx.next().await {
            let in_msg = match result {
                Ok(msg) => msg,
                Err(err) => {
                    error!("Failed to recv a buffer from socket: {}", err);
                    continue;
                },
            };
            let in_str = match in_msg.to_text() {
                Ok(text) => text,
                Err(err) => {
                    error!("Failed to convert binary buffer to utf8 encoding: {}", err);
                    continue;
                },
            };
            let input: Input = match serde_json::from_str(in_str) {
                Ok(structure) => structure,
                Err(err) => {
                    error!("Failed to deserialize an input structure from recv buffer: {}", err);
                    continue;
                },
            };
            let result = match input {
                Input::DrawTile(draw) => {
                    info!("Received a draw_msg={:?} input", draw);
                    handle_draw_tile(&server, conn, draw).await
                },
                Input::GetGroup(key) => {
                    info!("Received a get_group={:?} input", key);
                    handle_get_group(&server, conn, tx.as_ref(), key).await
                },
                Input::GetTileInfo((x, y)) => {
                    info!("Received a get_tile_info={:?} input", (x, y));
                    handle_get_tile_info(&server, tx.as_ref(), x, y).await
                },
            };
            if let Err(err) = result {
                error!("Error occurred while processing an input: {:?}", err)
            }
        }
    })
}

async fn handle_draw_tile(server: &Server, conn: &mut Connection, draw: DrawMsg) -> Result<(), ServiceError> {
    server.query.update_tile_now(draw.x, draw.y, draw.rgb, IpAddr::V4(Ipv4Addr::new(127, 0, 0, 0))).await?;
    broadcast_message(conn, draw)?;
    Ok(())
}

async fn handle_get_group(server: &Server, conn: &mut Connection, tx: &Mutex<SplitSink<WebSocket, Message>>, key: GroupKey) -> Result<(), ServiceError> {
    let group_opt = get_tile_group(conn, key)?;
    let group = match group_opt {
        None => server.query.get_tile_group(key).await?,
        Some(group) => group
    };

    let output = BinaryOutput::Group(group);
    let buffer = bincode::serialize(&output).map_err(|e| ServiceError::handle_fatal(e, "while serializing canvas output group"))?;
    
    let mut tx = tx.lock().await;
    tx.send(Message::from(buffer)).await.map_err(|e| ServiceError::handle_fatal(e, "failed to send tile group buffer over socket:"))?;
    
    Ok(())
}

async fn handle_get_tile_info(server: &Server, tx: &Mutex<SplitSink<WebSocket, Message>>, x: i32, y: i32) -> Result<(), ServiceError> {
    match server.query.get_one_tile(x, y).await {
        Ok(tile) => {
            let output = TextOutput::TileInfo(tile);
            let str = serde_json::to_string(&output).map_err(|e| ServiceError::handle_fatal(e, "while serializing canvas output tile info"))?;

            let mut tx = tx.lock().await;
            debug!("Sending canvas output tile info str={} over socket", str);
            tx.send(Message::from(str)).await.map_err(|e| ServiceError::handle_fatal(e, "failed to send tile info buffer over socket:"))?;

            Ok(())
        },
        Err(ServiceError::NotFoundError(str)) => {
            warn!("Not found: {}", str);

            let output = TextOutput::Err(str);
            let str = serde_json::to_string(&output).map_err(|e| ServiceError::handle_fatal(e, "while serializing canvas output err"))?;

            let mut tx = tx.lock().await;
            debug!("Sending canvas output err str={} over socket", str);
            tx.send(Message::from(str)).await.map_err(|e| ServiceError::handle_fatal(e, "failed to send error buffer over socket:"))?;

            Ok(())
        },
        Err(err) => Err(err)
    }
}

async fn handle_send(server: Server, tx: Arc<Mutex<SplitSink<WebSocket, Message>>>) -> JoinHandle<()> {
    tokio::spawn(async move {
        info!("Starting the send loop");
        let mut rx = server.tx.subscribe();
        loop {
            let draw = match rx.recv().await {
                Ok(draw) => draw,
                Err(RecvError::Closed) => break,
                Err(RecvError::Lagged(count)) => {
                    warn!("Broadcast receiver lagged, missing {} messages", count);
                    continue;
                }
            };
            
            let output = TextOutput::DrawEvent(draw);
            let str = match serde_json::to_string(&output) {
                Ok(buffer) => buffer,
                Err(err) => {
                    error!("Failed to serialize a draw_msg={:?}: {}", draw, err);
                    continue;
                }
            };
            
            let mut tx = tx.lock().await;
            debug!("Sending canvas output draw event str={} over socket", str);
            if let Err(err) = tx.send(Message::from(str)).await {
                error!("Failed to send serialized draw msg over socket: {}", err);
            }
        }
    })
}

async fn handle_socket(socket: WebSocket, server: Server) {
    info!("Websocket connection established with backend");
    
    let (tx, rx) = socket.split();
    let tx = Arc::new(Mutex::new(tx));
    let mut listener = handle_recv(server.clone(), rx, tx.clone()).await;
    let mut sender = handle_send(server, tx).await;
    
    tokio::select! {
        _ = &mut listener => {
            info!("Listener task ended, aborting sender task");
            sender.abort()
        },
        _ = &mut sender => {
            error!("Sender task ended, this should not happen");
            listener.abort()
        }
    }
}

pub async fn ws_handler(ws: WebSocketUpgrade, State(server): State<Server>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, server))
}
