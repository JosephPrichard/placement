use std::sync::Arc;
use bincode;
use futures_util::StreamExt;
use redis::{Commands, Connection};
use tokio::task::JoinHandle;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tracing::log::{error, info, warn};
use crate::services::models::{ServiceError, TileGroup};

const CHANNEL_BUS_NM: &str = "message-bus";

#[derive(Debug, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct DrawMsg {
    pub x: i32,
    pub y: i32,
    pub rgb: (i8, i8, i8),
}

pub fn create_message_subscriber(client: Arc<redis::Client>, tx: Arc<broadcast::Sender<DrawMsg>>) -> JoinHandle<Result<(), ServiceError>> {
    tokio::spawn(async move {
        let mut pubsub = client.get_async_pubsub()
            .await
            .map_err(|e| ServiceError::handle_fatal(e, "while getting connection in message subscriber"))?;

        pubsub.subscribe(CHANNEL_BUS_NM)
            .await
            .map_err(|e| ServiceError::handle_fatal(e, &format!("when subscribing from channel {}", CHANNEL_BUS_NM)))?;
        
        while let Some(msg) = pubsub.on_message().next().await {
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

pub fn broadcast_message(conn: &mut Connection, msg: DrawMsg) -> Result<(), ServiceError> {
    let bytes = bincode::serialize(&msg)
        .map_err(|e| ServiceError::handle_fatal(e, "when serializing draw_msg"))?;
    
    conn.publish(CHANNEL_BUS_NM, bytes)
        .map_err(|e| ServiceError::handle_fatal(e, &format!("when publishing to channel {}", CHANNEL_BUS_NM)))?;
    Ok(())
}
