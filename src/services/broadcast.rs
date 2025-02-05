use std::sync::Arc;
use bincode;
use redis::{Commands, Connection, RedisResult};
use tokio::task::JoinHandle;
use serde::{Deserialize, Serialize};
use log::{error, info, warn};
use tokio::sync::broadcast;
use crate::services::models::{GroupKey, ServiceError, TileGroup};

const CHANNEL_BUS_NM: &str = "message-bus";

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct DrawMsg {
    x: i32,
    y: i32,
    color: i8,
}

pub fn create_message_subscriber(client: Arc<redis::Client>, tx: broadcast::Sender<DrawMsg>) -> JoinHandle<Result<(), ServiceError>> {
    tokio::spawn(async move {
        let mut conn = client.get_connection()
            .map_err(|e| ServiceError::handle_fatal(e, "while getting connection in message subscriber"))?;
        let mut pubsub = conn.as_pubsub();
        pubsub.subscribe(CHANNEL_BUS_NM)
            .map_err(|e| ServiceError::handle_fatal(e, &format!("when subscribing from channel {}", CHANNEL_BUS_NM)))?;
        
        loop {
            let msg = pubsub.get_message()
                .map_err(|e| ServiceError::handle_fatal(e, "while reading a message from pubsub"))?;
            let payload : Vec<u8> = msg.get_payload()
                .map_err(|e| ServiceError::handle_fatal(e, "while extracting binary payload from pubsub message"))?;
            
            match msg.get_channel_name() {
                CHANNEL_BUS_NM => match bincode::deserialize::<DrawMsg>(&payload) {
                    Err(error) => error!("Failed to deserialize a message for channel={} with error={:?}", CHANNEL_BUS_NM, error),
                    Ok(draw_msg) => {
                        info!("Received a draw_msg={:?} on channel={}", draw_msg, msg.get_channel_name());
                        tx.send(draw_msg)
                            .map_err(|e| ServiceError::handle_fatal(e, "while sending draw_msg to broadcast channel"))?;
                    },
                },
                name => warn!("Unknown channel={} for message", name)
            };
        }
        Ok::<(), ServiceError>(())
    })
}

pub fn publish_message(conn: &mut Connection, msg: DrawMsg) -> Result<(), ServiceError> {
    let bytes = bincode::serialize(&msg)
        .map_err(|e| ServiceError::handle_fatal(e, "when serializing draw_msg"))?;
    
    conn.publish(CHANNEL_BUS_NM, bytes)
        .map_err(|e| ServiceError::handle_fatal(e, &format!("when publishing to channel {}", CHANNEL_BUS_NM)))?;
    Ok(())
}

mod test {

}