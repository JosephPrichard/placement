use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use axum::extract::{State, WebSocketUpgrade};
use axum::extract::ws::{Message, WebSocket};
use axum::response::IntoResponse;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, Stream, StreamExt};
use redis::{Client, Connection};
use tokio::sync::{broadcast, Mutex};
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;
use tracing::log::{error, info, warn};
use crate::server::cache::get_tile_group;
use crate::server::models::{CanvasInput, CanvasOutput, DrawMsg, GroupKey, ServiceError, TileGroup};
use crate::server::query::QueryStore;

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
            let inbuf: Vec<u8> = match result {
                Ok(msg) => msg.into(),
                Err(err) => {
                    error!("Failed to recv a buffer from socket: {}", err);
                    continue;
                },
            };
            let input: CanvasInput = match bincode::deserialize(inbuf.as_slice()) {
                Ok(structure) => structure,
                Err(err) => {
                    error!("Failed to deserialize an input structure from recv buffer: {}", err);
                    continue;
                },
            };
            let result = match input {
                CanvasInput::DrawTile(draw) => {
                    info!("Received a draw_msg={:?} input", draw);
                    server.query.update_tile_now(draw.x, draw.y, draw.rgb, IpAddr::V4(Ipv4Addr::new(127, 0, 0, 0))).await
                },
                CanvasInput::GetGroup(key) => {
                    info!("Received a get_group={:?} input", key);
                    handle_get_group(&server, conn, tx.as_ref(), key).await
                },
                CanvasInput::GetTileInfo((x, y)) => {
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

async fn handle_get_group(server: &Server, conn: &mut Connection, tx: &Mutex<SplitSink<WebSocket, Message>>, key: GroupKey) -> Result<(), ServiceError> {
    let group_opt = get_tile_group(conn, key)?;
    let group = match group_opt {
        None => server.query.get_tile_group(key).await?,
        Some(group) => group
    };

    let output = CanvasOutput::Group(group);
    let buffer = bincode::serialize(&output).map_err(|e| ServiceError::handle_fatal(e, "while serializing canvas output group"))?;
    
    let mut tx = tx.lock().await;
    if let Err(err) = tx.send(Message::from(buffer)).await {
        error!("Failed to send tile group buffer over socket: {}", err);
    }
    
    Ok(())
}

async fn handle_get_tile_info(server: &Server, tx: &Mutex<SplitSink<WebSocket, Message>>, x: i32, y: i32) -> Result<(), ServiceError> {
    let tile = server.query.get_one_tile(x, y).await?;

    let output = CanvasOutput::TileInfo(tile);
    let buffer = bincode::serialize(&output).map_err(|e| ServiceError::handle_fatal(e, "while serializing canvas output tile info"))?;

    let mut tx = tx.lock().await;
    if let Err(err) = tx.send(Message::from(buffer)).await {
        error!("Failed to send tile group buffer over socket: {}", err);
    }
    Ok(())
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
            
            let output = CanvasOutput::DrawEvent(draw);
            let buffer = match bincode::serialize(&output) {
                Ok(buffer) => buffer,
                Err(err) => {
                    error!("Failed to recv serialize a draw msg: {}", err);
                    continue;
                }
            };
            
            let mut tx = tx.lock().await;
            if let Err(err) = tx.send(Message::from(buffer)).await {
                error!("Failed to send serialized draw msg over socket: {}", err);
            }
        }
    })
}

async fn handle_socket(socket: WebSocket, server: Server) {
    info!("Websocket connection established with server");
    
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
