use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use deadpool_redis::Pool;
use tokio::sync::broadcast;
use tracing::{info_span, Instrument};
use tracing::log::{error, info};
use crate::backend::broadcast::broadcast_message;
use crate::backend::dict::{get_cached_group, get_conn, get_placement_time, set_cached_group, set_placement_time, upsert_cached_group, RedisLock};
use crate::backend::models::{DrawEvent, GroupKey, ServiceError};
use crate::backend::query::QueryStore;

#[derive(Clone)]
pub struct ServerState {
    pub broadcast_tx: broadcast::Sender<DrawEvent>,
    pub redis: Pool,
    pub query: Arc<QueryStore>,
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

pub async fn draw_tile(server: &ServerState, draw: DrawEvent, ip: IpAddr) -> Result<(), ServiceError> {
    async fn atomic_draw_tile(server: &ServerState, draw: DrawEvent, ip: IpAddr) -> Result<(), ServiceError>  {
        static DRAW_PERIOD: Duration = Duration::from_secs(60); // the minimum time between successful draw tile operations for each ipaddress

        let conn = &mut get_conn(&server.redis).await?;
        let _guard = RedisLock::acquire(get_conn(&server.redis).await?, ip).await?; // prevents a race condition where a client can get around the time placement limit

        let placement_epoch = Duration::from_millis(get_placement_time(conn, ip).await?);
        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();

        info!("Draw tile with ip={}, last_placement={}, at time_now={}", ip, placement_epoch.as_secs(), now.as_secs());

        if (now - DRAW_PERIOD) > placement_epoch {
            server.query.batch_upsert_tile(draw.x, draw.y, draw.rgb, ip, SystemTime::now()).instrument(info_span!("Batch update tile")).await?;
            set_placement_time(conn, ip, now.as_millis()).await?;
            Ok(())
        } else {
            let remaining = DRAW_PERIOD - (now - placement_epoch);
            let time_str = format!("{:02}:{:02}", remaining.as_secs() / 60,  remaining.as_secs() % 60);
            Err(ServiceError::Restricted(format!("{} minutes remaining until you can draw another tile.", time_str)))
        }
    }

    async fn draw_tile_task(redis: &Pool, draw: DrawEvent) -> Result<(), ServiceError> {
        let conn = &mut get_conn(redis).await?;
        upsert_cached_group(conn, draw).instrument(info_span!("Update cached group")).await?;
        broadcast_message(conn, draw).await?;
        Ok(())
    }

    atomic_draw_tile(server, draw, ip).instrument(info_span!("Atomic task")).await?;

    let server = server.clone();
    tokio::spawn(async move {
        if let Err(err) = draw_tile_task(&server.redis, draw).instrument(info_span!("Background task")).await {
            error!("Failed in draw tile background task: {:?}", err);
        };
    });
    Ok(())
}