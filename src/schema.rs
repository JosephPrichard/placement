use scylla::SessionBuilder;
use tracing::log::info;
use crate::backend::query::{create_schema, QueryStore};

mod backend;

#[tokio::main]
async fn main() {
    let port = 9042;
    let session = SessionBuilder::new()
        .known_node(format!("127.0.0.1:{}", port))
        .build()
        .await
        .unwrap();
    info!("Started the scylla session");

    session.query_unpaged("DROP KEYSPACE IF EXISTS pks;", ()).await.unwrap();
    create_schema(&session).await.unwrap();
    info!("Created the scylla schema");
}