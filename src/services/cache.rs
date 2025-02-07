use redis::{Commands, Connection};
use tracing::log::info;
use crate::services::models::{GroupKey, ServiceError, TileGroup, GROUP_LEN};

pub fn set_tile_group(conn: &mut Connection, key: GroupKey, group: TileGroup) -> Result<(), ServiceError> {
    let key_str = format!("({},{})", key.0, key.1);
    conn.set(key_str, group.0.as_slice())
        .map_err(|e| ServiceError::handle_fatal(e, "while setting tile group in cache using key"))?;
    
    info!("Set tile group with key={:?} into the cache", key);
    
    Ok(())
}

pub fn get_tile_group(conn: &mut Connection, key: GroupKey) -> Result<Option<TileGroup>, ServiceError> {
    let key_str = format!("({},{})", key.0, key.1);
    let buffer: Vec<u8> = conn.get(key_str)
        .map_err(|e| ServiceError::handle_fatal(e, "while getting tile group in cache using key"))?;
    
    info!("Get tile group with key={:?} from the cache", key);
    
    if buffer.len() == 0 {
        Ok(None)
    } else if buffer.len() != GROUP_LEN {
        Err(ServiceError::FatalError(format!("Tile group buffer length was of length {}, should be {}", buffer.len(), GROUP_LEN)))
    } else {
        Ok(Some(TileGroup(buffer)))
    }
}