use bb8_redis::bb8::PooledConnection;
use crate::backend::models::{DrawEvent, GroupKey, ServiceError, TileGroup, GROUP_LEN};
use bb8_redis::redis::AsyncCommands;
use bb8_redis::RedisConnectionManager;
use tracing::log::{error, info};

pub async fn set_cached_group<'a>(conn: &mut PooledConnection<'a, RedisConnectionManager>, key: GroupKey, group: &TileGroup) -> Result<(), ServiceError> {
    let key_str = format!("({},{})", key.0, key.1);
    let value_bytes = group.0.as_slice();
    
    let () = conn.set(key_str, value_bytes) // store the group buffer into the slice
        .await
        .map_err(|e| ServiceError::map_fatal(e, "while setting tile group in cache"))?;
    
    info!("Set tile group with key={:?} into the cache", key);
    
    Ok(())
}

pub async fn get_cached_group<'a>(conn: &mut PooledConnection<'a, RedisConnectionManager>, key: GroupKey) -> Result<Option<TileGroup>, ServiceError> {
    let key_str = format!("({},{})", key.0, key.1);
    let buffer: Vec<u8> = conn.get(key_str)
        .await
        .map_err(|e| ServiceError::map_fatal(e, "while getting tile group in cache"))?;
    
    info!("Got tile group with key={:?} from the cache", key);
    
    if buffer.len() == 0 {
        Ok(None)
    } else if buffer.len() != GROUP_LEN {
        error!("Tile group buffer length was of length {}, should be {}", buffer.len(), GROUP_LEN);
        Err(ServiceError::Fatal)
    } else {
        Ok(Some(TileGroup(buffer)))
    }
}

pub async fn update_cached_group<'a>(conn: &mut PooledConnection<'a, RedisConnectionManager>, draw: DrawEvent) -> Result<(), ServiceError> {
    let key = GroupKey::from_point(draw.x, draw.y);

    let key_str = format!("({},{})", key.0, key.1);
    let exists: bool = conn.exists(&key_str)
        .await
        .map_err(|e| ServiceError::map_fatal(e, "while getting tile group in cache using key"))?;

    info!("Checking if tile group with key={:?} exists: {}", key, exists);
    
    let x_offset = (draw.x - key.0).abs() as usize;
    let y_offset = (draw.y - key.1).abs() as usize;
    
    if exists {
        // if it exists, then write the rgb value into the group directly
        let byte_offset = TileGroup::get_offset(x_offset, y_offset);
        let value_bytes = [draw.rgb.0, draw.rgb.1, draw.rgb.2];
        
        let () = conn.setrange(key_str, byte_offset as isize, &value_bytes)
            .await
            .map_err(|e| ServiceError::map_fatal(e, "while performing setrange operation on group"))?;

        info!("Updated tile group using setrange with key={:?}", key);
    } else {
        // if it does not exist, create a new tile group, write the value into it, then write it into the cache
        let mut group = TileGroup::empty();
        group.set(x_offset, y_offset, draw.rgb);
        
        set_cached_group(conn, key, &group).await?;
        info!("Updated using set tile group with key={:?}", key);
    }

    info!("Updated tile group with key={:?} in the cache", key);

    Ok(())
}