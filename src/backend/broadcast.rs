use crate::backend::models::{DrawEvent, ServiceError};
use bb8_redis::bb8::Pool;
use bb8_redis::redis::AsyncCommands;
use bb8_redis::{redis, RedisConnectionManager};
use bincode;
use futures_util::StreamExt;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tracing::log::{error, info, warn};

const CHANNEL_BUS_NM: &str = "message-bus";

pub fn create_message_subscriber(redis: redis::Client, tx: broadcast::Sender<DrawEvent>) -> JoinHandle<Result<(), ServiceError>> {
    tokio::spawn(async move {
        let mut pubsub = redis.get_async_pubsub()
            .await
            .map_err(|e| ServiceError::map_fatal(e, "while getting connection in message subscriber"))?;

        pubsub.subscribe(CHANNEL_BUS_NM)
            .await
            .map_err(|e| ServiceError::map_fatal(e, &format!("when subscribing from channel {}", CHANNEL_BUS_NM)))?;
        
        info!("Began listening to pubsub subscribing to channel={}", CHANNEL_BUS_NM);
        while let Some(msg) = pubsub.on_message().next().await {
            match msg.get_payload::<Vec<u8>>() {
                Err(err) => error!("Failed to get payload into bytes from message: {:?}", err),
                Ok(payload) => {
                    info!("Received a payload of size={} on channel={}", payload.len(), msg.get_channel_name());

                    match msg.get_channel_name() {
                        CHANNEL_BUS_NM => match bincode::deserialize::<DrawEvent>(&payload) {
                            Err(error) =>
                                error!("Failed to deserialize a message for channel={} with error={:?}", CHANNEL_BUS_NM, error),
                            Ok(draw_msg) => {
                                info!("Received a draw_msg={:?} on channel={}", draw_msg, msg.get_channel_name());
                                if let Err(err) = tx.send(draw_msg) {
                                    error!("Failed to send draw event to broadcast channel with error={:?}", err);
                                }
                            },
                        },
                        name => warn!("Unknown channel={} for message", name)
                    };
                }
            }
        }
        Ok::<(), ServiceError>(())
    })
}

pub async fn broadcast_message(redis: &Pool<RedisConnectionManager>, msg: DrawEvent) -> Result<(), ServiceError> {
    let mut conn = redis.get().await.unwrap();
    
    let bytes = bincode::serialize(&msg)
        .map_err(|e| ServiceError::map_fatal(e, "when serializing draw_msg"))?;
    
    conn.publish::<_, _, ()>(CHANNEL_BUS_NM, bytes)
        .await
        .map_err(|e| ServiceError::map_fatal(e, &format!("when publishing to channel {}", CHANNEL_BUS_NM)))?;
    
    info!("Broadcast a draw message {:?} onto channel {}", msg, CHANNEL_BUS_NM);
    Ok(())
}
