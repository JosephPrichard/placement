use crate::backend::models::{BinaryOutput, DrawEvent, GroupKey, Input, RespErr, ServiceError};
use crate::backend::models::TextOutput;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{ConnectInfo, Query, State, WebSocketUpgrade};
use axum::response::IntoResponse;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use axum::http::{HeaderMap, StatusCode};
use bb8_redis::bb8::Pool;
use bb8_redis::RedisConnectionManager;
use serde::Deserialize;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;
use tracing::log::{error, info, warn};
use crate::backend::broadcast::broadcast_message;
use crate::backend::cache::{get_cached_tile_group, set_cached_tile_group, update_tile_in_cached_group};
use crate::backend::query::QueryStore;

pub type SocketReader = SplitStream<WebSocket>;
pub type SocketWriter = Arc<Mutex<SplitSink<WebSocket, Message>>>;

#[derive(Clone)]
pub struct ServerState {
    pub broadcast_tx: broadcast::Sender<DrawEvent>,
    pub redis: Pool<RedisConnectionManager>,
    pub query: Arc<QueryStore>,
}

async fn handle_recv(server: ServerState, mut rx: SocketReader, tx: SocketWriter, ip: IpAddr) -> JoinHandle<()> {
    tokio::spawn(async move {
        info!("Starting the recv loop for ip={}", ip);
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
            let result = handle_input(&server, tx.clone(), ip, input).await;
            if let Err(err) = result {
                error!("Error occurred while processing an input: {:?}", err)
            }
        }
    })
}

async fn handle_input(server: &ServerState, tx: SocketWriter, ip: IpAddr, input: Input) -> Result<(), ServiceError> {
    match input {
        Input::DrawTile(draw) => {
            info!("Received a draw_msg={:?} input", draw);
            draw_tile(server, draw, ip).await
        },
        Input::GetGroup(key) => {
            info!("Received a get_group={:?} input", key);
            let buffer = get_group(server, key).await?;

            let mut tx = tx.lock().await;
            tx.send(Message::from(buffer)).await.unwrap();
            Ok(())
        },
    }
}

async fn draw_tile(server: &ServerState, draw: DrawEvent, ip: IpAddr) -> Result<(), ServiceError> {
    server.query.update_tile_now(draw.x, draw.y, draw.rgb, ip).await?;

    let conn = &mut server.redis.get().await
        .map_err(|e| ServiceError::handle_fatal(e, "while getting a redis connection"))?;
    update_tile_in_cached_group(conn, draw).await?;
        
    broadcast_message(&server.redis, draw).await?;
    
    Ok(())
}

async fn get_group(server: &ServerState, key: GroupKey) -> Result<Vec<u8>, ServiceError> {
    let conn = &mut server.redis.get().await
        .map_err(|e| ServiceError::handle_fatal(e, "while getting a redis connection"))?;
    
    let group_opt = get_cached_tile_group(conn, key).await?;
    let group = match group_opt {
        None => server.query.get_tile_group(key).await?,
        Some(group) => group
    };

    set_cached_tile_group(conn, &group).await?;

    let output = BinaryOutput::Group(group);
    let buffer = bincode::serialize(&output).map_err(|e| ServiceError::handle_fatal(e, "while serializing canvas output group"))?;

    Ok(buffer)
}

async fn handle_send(broadcast_tx: broadcast::Sender<DrawEvent>, tx: Arc<Mutex<SplitSink<WebSocket, Message>>>, ip: IpAddr) -> JoinHandle<()> {
    tokio::spawn(async move {
        info!("Starting the send loop for ip={}", ip);
        let mut rx = broadcast_tx.subscribe();
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
            info!("Sending str={} over socket", str);
            if let Err(err) = tx.send(Message::from(str)).await {
                error!("Failed to send serialized draw msg over socket: {}", err);
            }
        }
    })
}

pub async fn ws_handler(ws: WebSocketUpgrade, headers: HeaderMap, State(server): State<ServerState>, ConnectInfo(addr): ConnectInfo<SocketAddr>) -> impl IntoResponse {
    let ip = match headers.get("X-Forwarded-For") {
        None => addr.ip(),
        Some(header) => {
            let ip_str = match header.to_str() {
                Ok(ip_str) => ip_str,
                Err(_) => return String::from("Failed to convert X-Forwarded-For header to string").into_response()
            };
            match IpAddr::from_str(ip_str) {
                Ok(ip) => ip,
                Err(_) => return String::from("Failed to parse ip address in X-Forwarded-For header").into_response()
            }
        }
    };
    ws.on_upgrade(move |socket| async move {
        info!("Websocket connection established with server for ip={}", ip);

        let (tx, rx) = socket.split();
        let tx = Arc::new(Mutex::new(tx));
        let mut listener = handle_recv(server.clone(), rx, tx.clone(), ip).await;
        let mut sender = handle_send(server.broadcast_tx.clone(), tx, ip).await;

        tokio::select! {
            _ = &mut listener => {
                info!("Listener task ended, aborting sender task for ip={}", ip);
                sender.abort()
            },
            _ = &mut sender => {
                error!("Sender task ended, this should not happen for ip={}", ip);
                listener.abort()
            }
        }
    })
}

#[derive(Deserialize)]
pub struct GetTileInfoQuery {
    x: i32,
    y: i32,
}

pub async fn get_tile_info(Query(query): Query<GetTileInfoQuery>, State(server): State<ServerState>) -> impl IntoResponse {
    match server.query.get_one_tile(query.x, query.y).await {
        Ok(tile) => {
            match serde_json::to_string(&tile) {
                Ok(str) => (StatusCode::OK, str),
                Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "An unexpected error has occurred".to_string())
            }
        },
        Err(ServiceError::NotFoundError(str)) => {
            let code = StatusCode::NOT_FOUND;
            let output = RespErr{ code: code.as_u16(), err: str };
            match serde_json::to_string(&output) {
                Ok(str) => (code, str),
                Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "An unexpected error has occurred".to_string())
            }
        },
        Err(err) => {
            error!("Error occurred during GET tile_info request: {:?}", err);
            (StatusCode::INTERNAL_SERVER_ERROR, "An unexpected error has occurred".to_string())
        }
    }
}