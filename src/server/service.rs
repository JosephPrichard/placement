use crate::server::broadcast::broadcast_message;
use crate::server::dict::{get_cached_group, get_conn, set_cached_group, update_placement, upsert_cached_group};
use crate::server::models::{DrawEvent, GroupKey, ServiceError};
use crate::server::query::QueryStore;
use deadpool_redis::{Config, Pool, Runtime};
use scylla::{Session, SessionBuilder};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::broadcast;
use tracing::log::{error, info};
use tracing::{info_span, Instrument};

#[derive(Clone)]
pub struct ServerState {
    pub broadcast_tx: broadcast::Sender<DrawEvent>,
    pub redis: Pool,
    pub query: Arc<QueryStore>,
}

pub async fn init_server(scylla_uri: String, redis_url: String) -> ServerState {
    let session: Session = SessionBuilder::new()
        .known_node(scylla_uri)
        .build()
        .await
        .unwrap();
    let query = Arc::new(QueryStore::init_queries(session).await.unwrap());
    let redis = Config::from_url(redis_url).create_pool(Some(Runtime::Tokio1)).unwrap();
    let (tx, _) = broadcast::channel(1000);

    ServerState { broadcast_tx: tx, query, redis }
}

pub async fn get_group(server: &ServerState, key: GroupKey) -> Result<Vec<u8>, ServiceError> {
    let conn = &mut get_conn(&server.redis).await?;

    let group_opt = get_cached_group(conn, key).await?;
    let group = match group_opt {
        None => server.query.get_tile_group(key).await?,
        Some(group) => group
    };

    set_cached_group(conn, key, &group).await?;

    let buffer = group.0;
    Ok(buffer)
}

const DRAW_PERIOD: Duration = Duration::from_secs(60); // the minimum time between successful draw tile operations for each ipaddress

fn get_remaining_time(placement_epoch: Duration, duration_now: Duration) -> Result<Duration, ServiceError> {
    if placement_epoch > duration_now {
        let err = format!("Invariant is false: placement_epoch={:?} is not smaller than duration_now={:?}", placement_epoch, duration_now);
        error!("{}", err);
        return Err(ServiceError::Fatal(err));
    }
    let difference = duration_now - placement_epoch;

    let remaining = DRAW_PERIOD - difference;
    if difference > DRAW_PERIOD {
        let err = format!("Invariant is false: difference={:?} is not smaller than DRAW_PERIOD={:?}", placement_epoch, duration_now);
        error!("{}", err);
        return Err(ServiceError::Fatal(err));
    }
    Ok(remaining)
}

pub async fn draw_tile(server: &ServerState, draw: DrawEvent, ip: IpAddr) -> Result<(), ServiceError> {
    let duration_now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();

    let conn = &mut get_conn(&server.redis).await?;
    let ret = update_placement(conn, ip.to_string(), duration_now, duration_now - DRAW_PERIOD).await?;

    if ret >= 0 {
        let placement_epoch = Duration::from_millis(ret as u64);
        let remaining = get_remaining_time(placement_epoch, duration_now)?;
        
        let time_str = format!("{:02}:{:02}", remaining.as_secs() / 60,  remaining.as_secs() % 60);
    
        info!("Did not draw tile for ip={} with last_placement={} and remaining_time={} at time_now={}", ip, placement_epoch.as_secs(), time_str, duration_now.as_secs());
        return Err(ServiceError::Forbidden(format!("{} minutes remaining until you can draw another tile.", time_str)))   
    }
    
    info!("Draw tile for ip={} at time_now={}", ip, duration_now.as_secs());
    
    server.query.batch_upsert_tile(draw.x, draw.y, draw.rgb, ip, SystemTime::now()).instrument(info_span!("Batch update tile")).await?;
    
    upsert_cached_group(conn, draw).instrument(info_span!("Update cached group")).await?;
    broadcast_message(conn, draw).await?;

    Ok(())
}