use std::convert::Infallible;
use crate::backend::models::{DrawEvent, GroupKey, Placement, ServiceError};
use axum::extract::{ConnectInfo, Query, State};
use axum::response::{IntoResponse, Sse};
use futures_util::Stream;
use std::net::{IpAddr, SocketAddr};
use std::ops::Add;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use axum::http::{HeaderMap, StatusCode};
use axum::http::header::CONTENT_TYPE;
use axum::Json;
use axum::response::sse::{Event, KeepAlive};
use bb8_redis::bb8::Pool;
use bb8_redis::RedisConnectionManager;
use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use serde::Deserialize;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast};
use tracing::{info_span, Instrument};
use tracing::log::{error, info, warn};
use crate::backend::broadcast::broadcast_message;
use crate::backend::cache::{acquire_conn, get_cached_group, set_cached_group, upsert_cached_group};
use crate::backend::query::QueryStore;

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
    
    let result = server.query.get_one_tile(query.x, query.y).instrument(info_span!("Get tile")).await;
    match result {
        Ok(tile) => match serde_json::to_string(&tile) {
            Ok(json) =>
                (StatusCode::OK, [(CONTENT_TYPE, "application/json")], json),
            Err(err) => {
                error!("Error occurred while serializing tile response: {:?}", err);
                (StatusCode::INTERNAL_SERVER_ERROR, [(CONTENT_TYPE, "text/plain")], "An unexpected error has occurred".to_string())
            }
        },
        Err(ServiceError::NotFound(str)) => (StatusCode::NOT_FOUND, [(CONTENT_TYPE, "text/plain")], str),
        Err(err) => {
            error!("Error occurred during get one tile operation: {:?}", err);
            (StatusCode::INTERNAL_SERVER_ERROR, [(CONTENT_TYPE, "text/plain")], "An unexpected error has occurred".to_string())
        }
    }
}

async fn draw_tile(server: &ServerState, draw: DrawEvent, ip: IpAddr) -> Result<(), ServiceError> {
    server.query.batch_upsert_tile(draw.x, draw.y, draw.rgb, ip, SystemTime::now()).instrument(info_span!("Batch update tile")).await?;

    // run these in a background task, we don't need to report to client if this fails.
    let server = server.clone();
    tokio::spawn(async move {
        if let Err(err) = async move {
            let conn = &mut acquire_conn(&server.redis).await?;
            upsert_cached_group(conn, draw).instrument(info_span!("Update cached group")).await?;
            broadcast_message(&server.redis, draw).await?;
            Ok::<(), ServiceError>(())
        }.await {
            error!("Failed in draw tile background task: {:?}", err);
        };
    });
    Ok(())
}

fn get_ip_address(headers: HeaderMap, addr: SocketAddr) -> Result<IpAddr, &'static str> {
    match headers.get("X-Forwarded-For") {
        None => Ok(addr.ip()),
        Some(header) => {
            match header.to_str() {
                Ok(ip_str) => match IpAddr::from_str(ip_str) {
                    Ok(ip) => Ok(ip),
                    Err(err) => {
                        error!("Failed to convert a header to a string: {}", err);
                        Err("Failed to parse ip address in X-Forwarded-For header")
                    }
                },
                Err(err) => {
                    error!("Failed to convert a header to a string: {}", err);
                    Err("Failed to convert X-Forwarded-For header to string")
                }
            }
        }
    }
}

pub async fn handle_post_tile(State(server): State<ServerState>, ConnectInfo(addr): ConnectInfo<SocketAddr>, headers: HeaderMap, Json(body): Json<DrawEvent>) -> impl IntoResponse {
    info!("Handling POST tile request body={:?}", body);

    let ip = match get_ip_address(headers, addr) {
        Ok(ip) => ip,
        Err(str) => return (StatusCode::INTERNAL_SERVER_ERROR, [(CONTENT_TYPE, "text/plain")], str)
    };
    
    let result = draw_tile(&server, body, ip).instrument(info_span!("Draw tile")).await;
    match result {
        Ok(()) => (StatusCode::OK, [(CONTENT_TYPE, "text/plain")], "Successfully drew the tile"),
        Err(err) => {
            error!("Failed to draw the tile, event={:?} err={:?}", body, err);
            (StatusCode::INTERNAL_SERVER_ERROR, [(CONTENT_TYPE, "text/plain")], "An unexpected error has occurred")
        }
    }
}

async fn get_group(server: &ServerState, key: GroupKey) -> Result<Vec<u8>, ServiceError> {
    let conn = &mut acquire_conn(&server.redis).await?;

    let group_opt = get_cached_group(conn, key).await?;
    let group = match group_opt {
        None => server.query.get_tile_group(key).await?,
        Some(group) => group
    };

    set_cached_group(conn, key, &group).await?;

    let buffer = group.0;
    Ok(buffer)
}

pub async fn handle_get_group(Query(query): Query<PointQuery>, State(server): State<ServerState>) -> impl IntoResponse {
    info!("Handling GET group request query={:?}", query);
    
    let key = GroupKey(query.x, query.y);
    
    let result = get_group(&server, key).instrument(info_span!("Get group")).await;
    match result {
        Ok(buffer) => (StatusCode::OK, [(CONTENT_TYPE, "application/octet-stream")], buffer),
        Err(err) => {
            error!("Failed to get the group for key={:?} err={:?}", key, err);
            (StatusCode::INTERNAL_SERVER_ERROR, [(CONTENT_TYPE, "text/plain")], "An unexpected error has occurred".as_bytes().to_vec())
        }
    }
}

async fn get_placements(server: &ServerState, timestamp: DateTime<Utc>)-> Result<Vec<Placement>, ServiceError> {
    static MAX_COUNT: i32 = 20;

    let mut placements = vec![];
    server.query.get_placements_in_day(timestamp, MAX_COUNT, &mut placements).await?;
    
    let mut remaining = MAX_COUNT - placements.len() as i32;

    // fetch until we reach the maximum count or run out of elements
    while remaining > 0 {
        // this is the end of the day - fetch more by rounding to the next day
        let timestamp = timestamp + TimeDelta::milliseconds(1);
        server.query.get_placements_in_day(timestamp, MAX_COUNT, &mut placements).await?;

        let next_remaining = MAX_COUNT - placements.len() as i32;
        if next_remaining == remaining {
            // no more elements means this is end of the placement log
            break
        }
        remaining = next_remaining;
    }

    Ok(placements)
}

#[derive(Debug, Deserialize)]
pub struct TimeQuery {
    timestamp: String,
}

pub async fn handle_get_placements(Query(query): Query<TimeQuery>, State(server): State<ServerState>) -> impl IntoResponse {
    info!("Handling GET placements request query={:?}", query);

    let timestamp = match DateTime::parse_from_rfc3339(query.timestamp.as_str()) {
        Ok(timestamp) => Utc.from_utc_datetime(&timestamp.naive_utc()),
        Err(err) => {
            error!("Failed to parse timestamp={:?} err={:?}", query.timestamp, err);
            return (StatusCode::BAD_REQUEST, [(CONTENT_TYPE, "text/plain")], "Invalid input: timestamp should be parseable ISO format".to_string())
        },
    };

    let result = server.query.get_placements_in_day(timestamp, 1000).await.instrument(info_span!("Get placements")).await;
    match result {
        Ok(placements) => match serde_json::to_string(&placements) {
            Ok(json) => (StatusCode::OK, [(CONTENT_TYPE, "application/octet-stream")], json),
            Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, [(CONTENT_TYPE, "text/plain")], "An unexpected error has occurred".to_string()),
        },
        Err(err) => {
            error!("Failed to get the placements after timestamp={:?} err={:?}", timestamp, err);
            (StatusCode::INTERNAL_SERVER_ERROR, [(CONTENT_TYPE, "text/plain")], "An unexpected error has occurred".to_string())
        }
    }
}
