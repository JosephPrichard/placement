use crate::backend::broadcast::{broadcast_message, create_message_subscriber};
use crate::backend::models::DrawEvent;
use bb8_redis::bb8::Pool;
use bb8_redis::RedisConnectionManager;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::broadcast;
use tokio::time::sleep;
use tracing::log::info;

static DRAW_MSGS: [DrawEvent; 4] = [
    // these are in sorted order in respect to the derived order of the 'Ord' trait
    // we need these sorted to be able to do unordered assertions against other vectors which will compare in arbitrary orders
    DrawEvent { x: 0, y: 0, rgb: (0, 0, 0) },
    DrawEvent { x: 0, y: 1, rgb: (1, 1, 1) },
    DrawEvent { x: 1, y: 0, rgb: (2, 2, 2) },
    DrawEvent { x: 1, y: 1, rgb: (3, 3, 3) }
];

async fn test_broadcast_messaging(client: redis::Client, pool: Pool<RedisConnectionManager>) {
    let (tx, _) = broadcast::channel(16);

    let subscriber_handle = create_message_subscriber(client, tx.clone());

    let mut recv_handles = vec![];
    for i in 0..3 {
        let mut rx = tx.subscribe();

        info!("Started task {}", i);
        let handle = tokio::spawn(async move {
            let mut msgs = vec![];
            for _ in 0..4 {
                let d = rx.recv().await.unwrap();
                msgs.push(d);
                info!("Task {} has received message={:?}", i, d);
            }
            info!("Task {} has received all messages={:?}", i, msgs);

            msgs.sort(); // we don't care about the order of the messages, just that we get the right ones
            assert_eq!(msgs, DRAW_MSGS);
        });
        recv_handles.push(handle)
    }
    
    for draw_msg in DRAW_MSGS {
        broadcast_message(&pool, draw_msg).await.unwrap();
    }

    let mut recv_handle = tokio::spawn(async move {
        for handle in recv_handles {
            handle.await.unwrap();
        }
        info!("All recv handlers completed");
    });
    let mut late_handle = tokio::spawn(async move {
        sleep(Duration::from_secs(5)).await;
        info!("Timed out after 5 seconds");
    });

    tokio::select! {
        _ = &mut recv_handle => {
            subscriber_handle.abort();
            late_handle.abort()
        },
        _ = &mut late_handle => {
            subscriber_handle.abort();
            recv_handle.abort();
            panic!("Recv handlers did not finish within 5 seconds, is there a deadlock?")
        }
    }
}

pub async fn test_broadcast() {
    let port = 6380;
    let redis_url = format!("redis://127.0.0.1:{}/", port);
    let client = redis::Client::open(redis_url.as_str()).unwrap();
    let redis = Pool::builder().build(RedisConnectionManager::new(redis_url).unwrap()).await.unwrap();
    
    test_broadcast_messaging(client, redis).await;

    let metrics = Handle::current().metrics();
    let n = metrics.num_alive_tasks();
    info!("Tokio has {} zombie tasks", if n >= 1 { n - 1 } else { n });
}