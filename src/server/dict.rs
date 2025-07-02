use crate::server::models::{DrawEvent, GroupKey, ServiceError, TileGroup, GROUP_LEN};
use deadpool_redis::redis::AsyncCommands;
use deadpool_redis::{Connection, Pool};
use std::time::Duration;
use tracing::log::{error, info};

pub async fn get_conn(redis: &Pool) -> Result<Connection, ServiceError> {
   redis.get().await.map_err(|e| ServiceError::map_fatal(e, "while getting a redis connection"))
}

pub async fn set_cached_group(conn: &mut Connection, key: GroupKey, group: &TileGroup) -> Result<(), ServiceError> {
    let key_str = format!("({},{})", key.0, key.1);
    let value_bytes = group.0.as_slice();
    
    let () = conn.set(key_str, value_bytes) // store the group buffer into the slice
        .await
        .map_err(|e| ServiceError::map_fatal(e, "while setting tile group in cache"))?;
    
    info!("Set tile group with key={:?} into the cache", key);
    Ok(())
}

pub async fn get_cached_group(conn: &mut Connection, key: GroupKey) -> Result<Option<TileGroup>, ServiceError> {
    let key_str = format!("({},{})", key.0, key.1);
    let buffer: Vec<u8> = conn.get(key_str)
        .await
        .map_err(|e| ServiceError::map_fatal(e, "while getting tile group in cache"))?;
    
    if buffer.len() == 0 {
        info!("Tile group with key={:?} was not in the cache", key);
        Ok(None)
    } else if buffer.len() != GROUP_LEN {
        error!("Tile group buffer length was of length {}, should be {}", buffer.len(), GROUP_LEN);
        Err(ServiceError::Fatal("Failed to get cached group due to invalid buffer length".to_string()))
    } else {
        info!("Got tile group with key={:?} from the cache", key);
        Ok(Some(TileGroup(buffer)))
    }
}

pub async fn init_cached_group(conn: &mut Connection, key: &str) -> Result<(), ServiceError> {
    static SCRIPT: &str = include_str!("../redis/ZEROINIT.lua");
    
    // set a byte array of zeros if the buffer has not been initialized yet
    let script = redis::Script::new(SCRIPT);
    let status: i32 = script.arg(key)
        .arg(GROUP_LEN)
        .invoke_async(conn)
        .await
        .map_err(|e| ServiceError::map_fatal(e, "while executing placement_time script"))?;

    info!("Tried to initialize a group with key={} status={}", key, if status > 0 { "write" } else { "noop" });
    Ok(())
}

pub async fn upsert_cached_group(conn: &mut Connection, draw: DrawEvent) -> Result<(), ServiceError> {
    let key = GroupKey::from_point(draw.x, draw.y);

    let key_str = format!("({},{})", key.0, key.1);

    init_cached_group(conn, key_str.as_str()).await?;
    
    let x_offset = (draw.x - key.0).abs() as usize;
    let y_offset = (draw.y - key.1).abs() as usize;
    
    let byte_offset = TileGroup::get_offset(x_offset, y_offset);
    let value_bytes = [draw.rgb.0, draw.rgb.1, draw.rgb.2];

    let () = conn.setrange(&key_str, byte_offset as isize, &value_bytes)
        .await
        .map_err(|e| ServiceError::map_fatal(e, "while performing setrange operation on group"))?;

    info!("Updated tile group using setrange with key={:?} in the cache", key);

    Ok(())
}

pub async fn update_placement(conn: &mut Connection, key: String, duration_now: Duration, duration_then: Duration) -> Result<i128, ServiceError> {
    static SCRIPT: &str = include_str!("../redis/UPDATELT.lua");
    
    let script = redis::Script::new(SCRIPT);
    let ret: i128 =  script.arg(&key)
        .arg(duration_now.as_millis() as u64)
        .arg(duration_then.as_millis() as u64)
        .invoke_async(conn)
        .await
        .map_err(|e| ServiceError::map_fatal(e, "while executing placement_time script"))?;

    info!("Updated placement with key={:?} and ret={}", key, ret);
    Ok(ret)
}