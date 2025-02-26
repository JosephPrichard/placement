use crate::server::query::{create_schema, QueryStore};
use scylla::SessionBuilder;
use tracing::log::info;

mod server;

#[tokio::main]
async fn main() {
    dotenv::dotenv().unwrap();
    tracing_subscriber::fmt()
        .init();

    let scylla_uri = std::env::var("SCYLLA_URI").expect("Missing SCYLLA_URI env variable");
    
    let session = SessionBuilder::new()
        .known_node(scylla_uri)
        .build()
        .await
        .unwrap();
    info!("Started the scylla session");

    session.query_unpaged("DROP KEYSPACE IF EXISTS pks;", ()).await.unwrap();
    create_schema(&session).await.unwrap();
    info!("Finished creating the scylla schema");
}