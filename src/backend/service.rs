use crate::backend::broadcast::broadcast_message;
use crate::backend::cache::{get_cached_group, set_cached_group, update_cached_group};
use crate::backend::handlers::ServerState;
use crate::backend::models::{DrawEvent, GroupKey, ServiceError};
use std::net::IpAddr;

pub async fn get_group(server: &ServerState, key: GroupKey)-> Result<Vec<u8>, ServiceError> {
    let conn = &mut server.redis.get().await
        .map_err(|e| ServiceError::map_fatal(e, "while getting a redis connection"))?;

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
    server.query.update_tile_now(draw.x, draw.y, draw.rgb, ip).await?;

    let conn = &mut server.redis.get().await
        .map_err(|e| ServiceError::map_fatal(e, "while getting a redis connection"))?;
    update_cached_group(conn, draw).await?;

    broadcast_message(&server.redis, draw).await?;

    Ok(())
}
