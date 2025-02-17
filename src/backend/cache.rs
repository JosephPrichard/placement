use bb8_redis::bb8::Pool;
use crate::backend::models::{GroupKey, ServiceError, TileGroup, GROUP_LEN};
use bb8_redis::redis::AsyncCommands;
use bb8_redis::RedisConnectionManager;
use tracing::log::info;

pub async fn set_cached_tile_group(redis: &Pool<RedisConnectionManager>, key: GroupKey, group: &TileGroup) -> Result<(), ServiceError> {
    let mut conn = redis.get().await.unwrap();
    
    let key_str = format!("({},{})", key.0, key.1);
    conn.set::<_, _, ()>(key_str, group.2.as_slice())
        .await
        .map_err(|e| ServiceError::handle_fatal(e, "while setting tile group in cache using key"))?;
    
    info!("Set tile group with key={:?} into the cache", key);
    
    Ok(())
}

pub async fn get_cached_tile_group(redis: &Pool<RedisConnectionManager>, key: GroupKey) -> Result<Option<TileGroup>, ServiceError> {
    let mut conn = redis.get().await.unwrap();
    
    let key_str = format!("({},{})", key.0, key.1);
    let buffer: Vec<u8> = conn.get(key_str)
        .await
        .map_err(|e| ServiceError::handle_fatal(e, "while getting tile group in cache using key"))?;
    
    info!("Get tile group with key={:?} from the cache", key);
    
    if buffer.len() == 0 {
        Ok(None)
    } else if buffer.len() != GROUP_LEN {
        Err(ServiceError::FatalError(format!("Tile group buffer length was of length {}, should be {}", buffer.len(), GROUP_LEN)))
    } else {
        Ok(Some(TileGroup(key.0, key.1, buffer)))
    }
}