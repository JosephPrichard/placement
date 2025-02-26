use crate::tests::broadcast::test_broadcast;
use crate::tests::dict::test_cache;
use crate::tests::query::test_query;
use tracing::log::info;

mod server;
mod tests;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();

    info!("Starting integration tests");

    info!("Starting broadcast tests");
    test_broadcast().await;
    info!("Successfully passed broadcast tests");
    
    info!("Starting cache tests");
    test_cache().await;
    info!("Successfully passed cache tests");
    
    info!("Starting query tests");
    test_query().await;
    info!("Successfully passed query tests");

    info!("Successfully passed tests");
}