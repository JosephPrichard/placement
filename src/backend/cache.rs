use bb8_redis::bb8::Pool;
use bb8_redis::bb8::PooledConnection;
use crate::backend::models::{DrawEvent, GroupKey, ServiceError, TileGroup, GROUP_LEN};
use bb8_redis::redis::AsyncCommands;
use bb8_redis::RedisConnectionManager;
use tracing::log::info;

pub async fn set_cached_tile_group<'a>(conn: &mut PooledConnection<'a, RedisConnectionManager>, group: &TileGroup) -> Result<(), ServiceError> {
    let key_str = format!("({},{})", group.0, group.1);
    let value_bytes = group.2.as_slice();
    
    let () = conn.set(key_str, value_bytes) // store the group buffer into the slice
        .await
        .map_err(|e| ServiceError::handle_fatal(e, "while setting tile group in cache"))?;
    
    info!("Set tile group with key={:?} into the cache", (group.0, group.1));
    
    Ok(())
}

pub async fn get_cached_tile_group<'a>(conn: &mut PooledConnection<'a, RedisConnectionManager>, key: GroupKey) -> Result<Option<TileGroup>, ServiceError> {
    let key_str = format!("({},{})", key.0, key.1);
    let buffer: Vec<u8> = conn.get(key_str)
        .await
        .map_err(|e| ServiceError::handle_fatal(e, "while getting tile group in cache"))?;
    
    info!("Get tile group with key={:?} from the cache", key);
    
    if buffer.len() == 0 {
        Ok(None)
    } else if buffer.len() != GROUP_LEN {
        Err(ServiceError::FatalError(format!("Tile group buffer length was of length {}, should be {}", buffer.len(), GROUP_LEN)))
    } else {
        Ok(Some(TileGroup(key.0, key.1, buffer)))
    }
}

pub async fn update_tile_in_cached_group<'a>(conn: &mut PooledConnection<'a, RedisConnectionManager>, draw: DrawEvent) -> Result<(), ServiceError> {
    let key = GroupKey::from_point(draw.x, draw.y);

    let key_str = format!("({},{})", key.0, key.1);
    let exists: bool = conn.exists(&key_str)
        .await
        .map_err(|e| ServiceError::handle_fatal(e, "while getting tile group in cache using key"))?;
    
    let (x_offset, y_offset) = ((draw.x - key.0) as usize, (draw.y - key.1) as usize);
    
    if exists {
        // if it exists, then write the rgb value into the group directly
        let byte_offset = TileGroup::get_offset(x_offset, y_offset);
        let value_bytes = vec![draw.rgb.0 as u8, draw.rgb.1 as u8, draw.rgb.2 as u8];
        let () = conn.setrange(key_str, byte_offset as isize, value_bytes.as_slice())
            .await
            .map_err(|e| ServiceError::handle_fatal(e, "while performing setrange operation on group"))?;
    } else {
        // if it does not exist, create a new tile group, write the value into it, then write it into the cache
        let mut group = TileGroup::empty(key.0, key.1);
        group.set(x_offset, y_offset, draw.rgb);
        set_cached_tile_group(conn, &group).await?;
    }

    Ok(())
}