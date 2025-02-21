use std::net::IpAddr;
use std::str::FromStr;
use std::time::Duration;
use crate::backend::models::{DrawEvent, GroupKey, ServiceError, TileGroup, GROUP_LEN};
use deadpool_redis::{Connection, Pool};
use deadpool_redis::redis::{cmd, AsyncCommands, Cmd, ExistenceCheck, SetOptions};
use tokio::time::sleep;
use tracing::log::{error, info, warn};

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
        Err(ServiceError::Fatal)
    } else {
        info!("Got tile group with key={:?} from the cache", key);
        Ok(Some(TileGroup(buffer)))
    }
}

pub async fn upsert_cached_group(conn: &mut Connection, draw: DrawEvent) -> Result<(), ServiceError> {
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

        info!("Updated tile group using setrange with key={:?} in the cache", key);
    } else {
        // if it does not exist, create a new tile group, write the value into it, then write it into the cache
        let mut group = TileGroup::empty();
        group.set(x_offset, y_offset, draw.rgb);
        
        set_cached_group(conn, key, &group).await?;
        info!("Updated using set tile group with key={:?} in the cache", key);
    }

    Ok(())
}

pub async fn get_placement_time(conn: &mut Connection, ip: IpAddr)-> Result<u64, ServiceError> {
    let key = format!("placement_time_{}", ip);
    let epoch_opt: Option<String> = conn.get(key)
        .await
        .map_err(|e| ServiceError::map_fatal(e, "while getting tile group in cache"))?;
    match epoch_opt {
        None => Ok(0), // 0 is guaranteed to be outside the draw period limit
        Some(epoch) => {
            let epoch = u64::from_str(epoch.as_str()).map_err(|e| ServiceError::map_fatal(e, "while parsing placement time epoch"))?;
            Ok(epoch)
        }
    }
}

pub async fn set_placement_time(conn: &mut Connection, ip: IpAddr, epoch: u128)-> Result<(), ServiceError> {
    let key = format!("placement_time_{}", ip);
    let () = conn.set(key, epoch.to_string())
        .await
        .map_err(|e| ServiceError::map_fatal(e, "while getting tile group in cache"))?;
    Ok(())
}

pub struct RedisLock {
    key: String,
    conn: Option<Connection>
}

impl RedisLock {
    pub async fn acquire<Key: ToString>(mut conn: Connection, key: Key) -> Result<Self, ServiceError> {
        let key = format!("redis_lock_{}", key.to_string());
        for i in 0..100 {
            let success: bool = cmd("SET").arg(key.to_string()).arg("1").arg("NX").arg("EX").arg(5)
                .query_async(&mut conn)
                .await
                .map_err(|e| ServiceError::map_fatal(e, "while performing set nx in acquiring redis lock"))?;
            if success {
                info!("Acquired a redis lock for key={:?} after attempts={}", key, i);
                return Ok(Self{ key, conn: Option::from(conn) })
            }
            sleep(Duration::from_millis(50)).await;
        }
        error!("Failed to acquire a redis lock, timed out");
        Err(ServiceError::Fatal)
    }
}

impl Drop for RedisLock {
    fn drop(&mut self) {
        info!("Dropping redis lock for key {:?}", self.key);

        let key = self.key.clone();
        let mut conn = self.conn.take().unwrap();
        tokio::spawn(async move {
            let exists: bool = conn.del(key.as_str())
                .await
                .map_err(|e| ServiceError::map_fatal(e, "while performing del in releasing redis lock"))?;
            if !exists {
                warn!("Attempted to release a redis lock with key={:?} is not acquired", key);
            } else {
                info!("Released a redis lock with key={:?}", key);
            }
            Ok::<(), ServiceError>(())
        });
    }
}