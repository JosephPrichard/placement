use crate::server::models::{DrawEvent, ServiceError};
use bincode;
use deadpool_redis::redis::AsyncCommands;
use deadpool_redis::{redis, Connection};
use futures_util::StreamExt;
use redis::Msg;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tracing::log::{error, info, warn};

fn handle_redis_message(msg: Msg, draw_tx: &mut broadcast::Sender<DrawEvent>) {
    match msg.get_payload::<Vec<u8>>() {
        Err(err) => error!("Failed to get draw event payload into bytes from message: {:?}", err),
        Ok(payload) => match bincode::deserialize::<DrawEvent>(&payload) {
            Err(error) =>
                error!("Failed to deserialize a message for channel={} with error={:?}", "draw-message-bus", error),
            Ok(draw_msg) => {
                info!("Received a draw_msg={:?} on redis channel={}", draw_msg, msg.get_channel_name());
                info!("Broadcast channel receiver count={:?}", draw_tx.receiver_count());
                if draw_tx.receiver_count() >= 1 {
                    if let Err(err) = draw_tx.send(draw_msg) {
                        error!("Failed to send draw event to broadcast channel with error={:?}", err);
                    }
                    info!("Send for draw_msg={:?} to broadcast channel was successful", draw_msg);
                } else {
                    warn!("Skipped send to broadcast channel, no receivers are listening");
                }
            },
        }
    }
}

pub fn create_channel_subscriber(redis: redis::Client, mut draw_tx: broadcast::Sender<DrawEvent>) -> JoinHandle<Result<(), ServiceError>> {
    tokio::spawn(async move {
        let mut pubsub = redis.get_async_pubsub()
            .await
            .map_err(|e| ServiceError::map_fatal(e, "while getting async pubsub in message subscriber"))?;

        pubsub.subscribe("draw-message-bus")
            .await
            .map_err(|e| ServiceError::map_fatal(e, concat!("when subscribing to channel", "draw-message-bus")))?;

        info!("Began listening to pubsub subscribing to channel={}", "draw-message-bus");
        while let Some(msg) = pubsub.on_message().next().await {
            match msg.get_channel_name() {
                "draw-message-bus" => handle_redis_message(msg, &mut draw_tx),
                name => warn!("Unknown channel={} for message", name)
            }
        }
        Ok::<(), ServiceError>(())
    })
}

pub async fn broadcast_message(conn: &mut Connection, msg: DrawEvent) -> Result<(), ServiceError> {
    let bytes = bincode::serialize(&msg)
        .map_err(|e| ServiceError::map_fatal(e, "when serializing draw_msg"))?;

    conn.publish::<_, _, ()>("draw-message-bus", bytes)
        .await
        .map_err(|e| ServiceError::map_fatal(e, concat!("when publishing to channel", "draw-message-bus")))?;

    info!("Broadcast a draw message {:?} onto channel {}", msg, "draw-message-bus");
    Ok(())
}
