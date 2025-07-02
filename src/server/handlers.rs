use crate::server::models::{DrawEvent, GroupKey, ServiceError};
use crate::server::service::{draw_tile, get_group, ServerState};
use crate::server::utils::epoch_to_day;
use axum::extract::{ConnectInfo, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive};
use axum::response::{IntoResponse, Sse};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::DateTime;
use futures_util::Stream;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::fmt::Debug;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::time::{Duration, SystemTime};
use tokio::sync::broadcast::error::RecvError;
use tracing::log::{error, info, warn};
use tracing::{info_span, Instrument};

pub fn app(server: ServerState) -> Router {
    Router::new()
        .route("/canvas/sse", get(handle_sse))
        .route("/tile", get(handle_get_tile))
        .route("/tile", post(handle_post_tile))
        .route("/group", get(handle_get_group))
        .route("/placements", get(handle_get_placements))
        .with_state(server)
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

#[derive(Serialize)]
pub enum ServiceResp<'a, D: Serialize> {
    #[serde(rename = "data")]
    Data(D),
    #[serde(rename = "error")]
    DynError(String),
    #[serde(rename = "error")]
    StaticError(&'a str),
}

impl<'a, D: Serialize> IntoResponse for ServiceResp<'a, D> {
    fn into_response(self) -> axum::response::Response {
        Json(self).into_response()
    }
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
                (StatusCode::OK, ServiceResp::Data(json)),
            Err(err) => {
                error!("Error occurred while serializing tile response: {:?}", err);
                (StatusCode::INTERNAL_SERVER_ERROR, ServiceResp::StaticError("An unexpected error has occurred"))
            }
        },
        Err(ServiceError::NotFound(str)) => (StatusCode::NOT_FOUND, ServiceResp::DynError(str)),
        Err(err) => {
            error!("Error occurred during get one tile operation: {:?}", err);
            (StatusCode::INTERNAL_SERVER_ERROR, ServiceResp::StaticError("An unexpected error has occurred"))
        }
    }
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
        Err(str) => return (StatusCode::INTERNAL_SERVER_ERROR, ServiceResp::DynError(str.to_string()))
    };
    
    let result = draw_tile(&server, body, ip).instrument(info_span!("Draw tile")).await;
    match result {
        Ok(()) => (StatusCode::OK, ServiceResp::Data("Successfully drew the tile".to_string())),
        Err(ServiceError::Forbidden(msg)) => (StatusCode::BAD_REQUEST, ServiceResp::DynError(msg)),
        Err(err) => {
            error!("Failed to draw the tile, event={:?} err={:?}", body, err);
            (StatusCode::INTERNAL_SERVER_ERROR, ServiceResp::StaticError("An unexpected error has occurred"))
        }
    }
}

pub async fn handle_get_group(Query(query): Query<PointQuery>, State(server): State<ServerState>) -> impl IntoResponse {
    info!("Handling GET group request query={:?}", query);

    let key = GroupKey(query.x, query.y);

    let result = get_group(&server, key).instrument(info_span!("Get group")).await;
    match result {
        Ok(buffer) => (StatusCode::OK, buffer),
        Err(err) => {
            error!("Failed to get the group for key={:?} err={:?}", key, err);
            (StatusCode::INTERNAL_SERVER_ERROR, "An unexpected error has occurred".as_bytes().to_vec())
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct TimeQuery {
    days_ago: Option<i64>,
    timestamp_after: Option<String>, // RFC 3339 timestamp
}

pub async fn handle_get_placements(Query(query): Query<TimeQuery>, State(server): State<ServerState>) -> impl IntoResponse {
    info!("Handling GET placements request query={:?}", query);

    let days_ago = query.days_ago.unwrap_or(0);
    if days_ago < 0 {
        return (StatusCode::BAD_REQUEST, ServiceResp::StaticError("Days ago query must be a positive integer"))
    }
    let epoch_after = match query.timestamp_after {
        None => Duration::from_secs(SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs()),
        Some(time_str) => match DateTime::parse_from_rfc3339(time_str.as_str()) {
            Ok(date_time) => Duration::from_secs(date_time.timestamp() as u64),
            Err(err) => {
                warn!("Failed to parse timestamp_after={} field: {:?}", time_str, err);
                return (StatusCode::BAD_REQUEST, ServiceResp::StaticError("Could not parse timestamp_after field, must be in RFC 3339 timestamp format"))
            }
        }
    };

    let duration_now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
    let day_now = epoch_to_day(duration_now);
    let day = day_now - days_ago;

    if day < 0 {
        return (StatusCode::OK, ServiceResp::Data(vec![]))
    }

    let result = server.query.get_placements(day, epoch_after).instrument(info_span!("Get placements")).await;
    match result {
        Ok(placements) => (StatusCode::OK, ServiceResp::Data(placements)),
        Err(err) => {
            error!("Failed to get the placements after day={:?} err={:?}", day, err);
            (StatusCode::INTERNAL_SERVER_ERROR, ServiceResp::StaticError("An unexpected error has occurred"))
        }
    }
}