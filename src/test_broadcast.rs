use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::broadcast;
use tokio::time::sleep;
use tracing::log::{error, info};
use crate::services::broadcast::{create_message_subscriber, broadcast_message, DrawMsg};

mod services;

async fn test_broadcast() {
    let draw_msgs = vec![
        DrawMsg{ x: 0, y: 0, rgb: (0, 0, 0) },
        DrawMsg{ x: 1, y: 0, rgb: (1, 1, 1) },
        DrawMsg{ x: 0, y: 1, rgb: (2, 2, 2) },
        DrawMsg{ x: 1, y: 1, rgb: (3, 3, 3) }
    ];

    let client = Arc::new(redis::Client::open("redis://127.0.0.1:6380/").unwrap());

    let (tx, _) = broadcast::channel(16);
    let tx = Arc::new(tx);

    let subscriber_handle = create_message_subscriber(client.clone(), tx.clone());

    let mut recv_handles = vec![];
    for i in 0..3 {
        let draw_msgs = draw_msgs.clone();
        let mut rx = tx.subscribe();
        
        info!("Started task {}", i);
        let handle = tokio::spawn(async move {
            for draw_msg in draw_msgs {
                let d = rx.recv().await.unwrap();
                assert_eq!(d, draw_msg);
                info!("Task {} has received message={:?}", i, d);
            }
            info!("Task {} has received all messages", i);
        });
        recv_handles.push(handle)
    }

    let conn = &mut client.get_connection().unwrap();
    for draw_msg in draw_msgs {
        broadcast_message(conn, draw_msg).unwrap();
    }

    let recv_handle = tokio::spawn(async move {
        for handle in recv_handles {
            handle.await.unwrap();
        }
        info!("All recv handlers completed");
    });
    let late_handle = tokio::spawn(async move {
        sleep(Duration::from_secs(5)).await;
        info!("Timed out after 5 seconds");
    });

    tokio::select! {
        _ = recv_handle => subscriber_handle.abort(),
        _ = late_handle => {
            subscriber_handle.abort();
            panic!("Recv handlers did not finish within 5 seconds, is there a deadlock?")
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();

    info!("Starting broadcast integration test");

    test_broadcast().await;

    info!("Successfully passed broadcast integration test");

    let metrics = Handle::current().metrics();
    let n = metrics.num_alive_tasks();
    info!("Tokio has {} zombie tasks", n - 1);
}