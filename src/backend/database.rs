use std::future::Future;
use std::net::IpAddr;
use std::sync::Arc;
use bb8_redis::bb8::Pool;
use bb8_redis::RedisConnectionManager;
use tokio::sync::{broadcast};
use tracing::log::warn;
use crate::backend::broadcast::broadcast_message;
use crate::backend::cache::{get_cached_tile_group, set_cached_tile_group};
use crate::backend::models::{BinaryOutput, DrawMsg, GroupKey, ServiceError, TextOutput};
use crate::backend::query::QueryStore;

#[derive(Clone)]
pub struct Database {
    pub redis: Pool<RedisConnectionManager>,
    pub query: Arc<QueryStore>,
}

pub trait External: Send + Clone {
    fn draw_tile(&self, draw: DrawMsg, ip: IpAddr) -> impl Future<Output = Result<(), ServiceError>> + Send;

    fn get_group(&self, key: GroupKey) -> impl Future<Output = Result<Vec<u8>, ServiceError>> + Send;

    fn get_tile_info(&self, point: (i32, i32)) -> impl Future<Output = Result<String, ServiceError>> + Send;
}

impl External for Database {
    fn draw_tile(&self, draw: DrawMsg, ip: IpAddr) -> impl Future<Output = Result<(), ServiceError>> + Send {
       async move {
           self.query.update_tile_now(draw.x, draw.y, draw.rgb, ip).await?;
           broadcast_message(&self.redis, draw).await?;
           Ok(())
       }
    }

    fn get_group(&self, key: GroupKey) -> impl Future<Output = Result<Vec<u8>, ServiceError>> + Send {
        async move {
            let group_opt = get_cached_tile_group(&self.redis, key).await?;
            let group = match group_opt {
                None => self.query.get_tile_group(key).await?,
                Some(group) => group
            };

            set_cached_tile_group(&self.redis, key, &group).await?;

            let output = BinaryOutput::Group(group);
            let buffer = bincode::serialize(&output).map_err(|e| ServiceError::handle_fatal(e, "while serializing canvas output group"))?;

            Ok(buffer)
        }
    }

    fn get_tile_info(&self, (x, y): (i32, i32)) -> impl Future<Output = Result<String, ServiceError>> + Send {
        async move {
            match self.query.get_one_tile(x, y).await {
                Ok(tile) => {
                    let output = TextOutput::TileInfo(tile);
                    let str = serde_json::to_string(&output).map_err(|e| ServiceError::handle_fatal(e, "while serializing canvas output tile info"))?;
                    Ok(str)
                },
                Err(ServiceError::NotFoundError(str)) => {
                    warn!("Not found: {}", str);

                    let output = TextOutput::Err(str);
                    let str = serde_json::to_string(&output).map_err(|e| ServiceError::handle_fatal(e, "while serializing canvas output err"))?;
                    Ok(str)
                },
                Err(err) => Err(err)
            }
        }
    }
}