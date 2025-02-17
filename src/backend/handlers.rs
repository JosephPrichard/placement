use std::convert::Infallible;
use crate::backend::models::{DrawEvent, GroupKey, ServiceError};
use axum::extract::ws::Message;
use axum::extract::{ConnectInfo, Query, State, WebSocketUpgrade};
use axum::response::{IntoResponse, Sse};
use futures_util::{SinkExt, Stream, StreamExt};
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use axum::http::{HeaderMap, StatusCode};
use axum::http::header::CONTENT_TYPE;
use axum::Json;
use axum::response::sse::{Event, KeepAlive};
use bb8_redis::bb8::Pool;
use bb8_redis::RedisConnectionManager;
use serde::Deserialize;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast};
use tracing::log::{error, info, warn};
use crate::backend::query::QueryStore;
use crate::backend::service::{draw_tile, get_group};

#[derive(Clone)]
pub struct ServerState {
    pub broadcast_tx: broadcast::Sender<DrawEvent>,
    pub redis: Pool<RedisConnectionManager>,
    pub query: Arc<QueryStore>,
}

pub async fn handle_sse(State(server): State<ServerState>) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let mut rx = server.broadcast_tx.subscribe();

    info!("Creating a stream for the broadcast subscriber");
    let stream = async_stream::stream! {
        loop {
            let draw = match rx.recv().await {
                Ok(draw) => draw,
                Err(RecvError::Closed) => break,
                Err(RecvError::Lagged(count)) => {
                    warn!("Broadcast receiver lagged, missing {} messages", count);
                    continue;
                }
            };

            let str = match serde_json::to_string(&draw) {
                Ok(buffer) => buffer,
                Err(err) => {
                    error!("Failed to serialize a draw_msg={:?}: {}", draw, err);
                    continue;
                }
            };

            yield Ok(Event::default().data(str))
        }
    };
    
    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(1))
            .text("keep-alive"))
}

#[derive(Debug, Deserialize)]
pub struct PointQuery {
    x: i32,
    y: i32,
}

pub async fn handle_get_tile(Query(query): Query<PointQuery>, State(server): State<ServerState>) -> impl IntoResponse {
    info!("Handling GET tile info request query={:?}", query);
    
    let result = server.query.get_one_tile(query.x, query.y).await;
    match result {
        Ok(tile) => match serde_json::to_string(&tile) {
            Ok(json) =>
                (StatusCode::OK, [(CONTENT_TYPE, "application/json")], json),
            Err(err) => {
                error!("Error occurred while serializing tile response: {:?}", err);
                (StatusCode::INTERNAL_SERVER_ERROR, [(CONTENT_TYPE, "text/plain")], "An unexpected error has occurred".to_string())
            }
        },
        Err(ServiceError::NotFoundError(str)) => (StatusCode::NOT_FOUND, [(CONTENT_TYPE, "text/plain")], str),
        Err(err) => {
            error!("Error occurred during get one tile operation: {:?}", err);
            (StatusCode::INTERNAL_SERVER_ERROR, [(CONTENT_TYPE, "text/plain")], "An unexpected error has occurred".to_string())
        }
    }
}

pub async fn handle_post_tile(State(server): State<ServerState>, ConnectInfo(addr): ConnectInfo<SocketAddr>, headers: HeaderMap, Json(body): Json<DrawEvent>) -> impl IntoResponse {
    info!("Handling POST tile request body={:?}", body);

    let resp_headers = [(CONTENT_TYPE, "text/plain")];

    let ip = match headers.get("X-Forwarded-For") {
        None => addr.ip(),
        Some(header) => {
            let ip_str = match header.to_str() {
                Ok(ip_str) => ip_str,
                Err(err) => {
                    error!("Failed to convert a header to a string: {}", err);
                    return (StatusCode::BAD_REQUEST, resp_headers, "Failed to convert X-Forwarded-For header to string")
                }
            };
            match IpAddr::from_str(ip_str) {
                Ok(ip) => ip,
                Err(err) => {
                    error!("Failed to convert a header to a string: {}", err);
                    return (StatusCode::BAD_REQUEST, resp_headers, "Failed to parse ip address in X-Forwarded-For header")
                }
            }
        }
    };
    
    let result = draw_tile(&server, body, ip).await;
    match result {
        Ok(()) => (StatusCode::OK, resp_headers, "Successfully drew the tile"),
        Err(err) => {
            error!("Failed to draw the tile, event={:?} err={:?}", body, err);
            (StatusCode::INTERNAL_SERVER_ERROR, resp_headers, "An unexpected error has occurred")
        }
    }
}

pub async fn handle_get_group(Query(query): Query<PointQuery>, State(server): State<ServerState>) -> impl IntoResponse {
    info!("Handling GET group request query={:?}", query);
    
    let key = GroupKey(query.x, query.y);
    
    let result = get_group(&server, key).await;
    match result {
        Ok(buffer) => (StatusCode::OK, [(CONTENT_TYPE, "application/octet-stream")], buffer),
        Err(err) => {
            error!("Failed to get the group for key={:?} err={:?}", key, err);
            (StatusCode::INTERNAL_SERVER_ERROR, [(CONTENT_TYPE, "text/plain")], "An unexpected error has occurred".as_bytes().to_vec())
        }
    }
}
