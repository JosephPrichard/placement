use std::net::{IpAddr, Ipv4Addr};
use std::time::SystemTime;
use chrono::{TimeZone, Utc};
use scylla::frame::value::CqlTimestamp;
use scylla::SessionBuilder;
use tracing::log::info;
use crate::services::models::{Placement, ServiceError, Tile, GROUP_LEN};
use crate::services::query::{create_schema, QueryStore};

mod services;
mod web;

async fn init_queries() -> QueryStore {
    let session = SessionBuilder::new()
        .known_node("127.0.0.1:9042")
        .build()
        .await
        .unwrap();
    create_schema(&session).await.unwrap();
    QueryStore::init_queries(session).await.unwrap()
}

async fn test_get_then_update_tile(query: &QueryStore) {
    query.session.query_unpaged("TRUNCATE pks.tiles;", ()).await.unwrap();
    
    let result = query.get_one_tile(1, 1).await;

    query.update_tile(5, 4, 2, IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))).await.unwrap();
    let tile = query.get_one_tile(5, 4).await.unwrap();

    assert_eq!(result, Err(ServiceError::NotFoundError(String::from("tile not found at given location"))));
    assert_eq!(tile, Tile { x: 5, y: 4, color: 2, last_updated_time: CqlTimestamp(0) });
}

async fn test_get_tile_group(query: &QueryStore) {
    query.session.query_unpaged("TRUNCATE pks.tiles;", ()).await.unwrap();
    
    query.update_tile(0, 0, 1, IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))).await.unwrap();
    query.update_tile(2, 0, 5, IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))).await.unwrap();
    query.update_tile((GROUP_LEN + 1) as i32, (GROUP_LEN + 1) as i32, 5, IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))).await.unwrap();
    let tiles = query.get_tile_group(0, 0).await.unwrap();

    let mut expected = [[0i8; GROUP_LEN]; GROUP_LEN];
    expected[0][0] = 1;
    expected[2][0] = 5;

    assert_eq!(tiles, expected)
}

async fn test_insert_then_get_placements(query: &QueryStore) {
    query.session.query_unpaged("TRUNCATE pks.placements;", ()).await.unwrap();
    
    let time1 = SystemTime::from(Utc.with_ymd_and_hms(2015, 1, 1, 0, 0, 0).unwrap());
    let time2 = SystemTime::from(Utc.with_ymd_and_hms(2020, 1, 1, 0, 15, 0).unwrap());
    let time3 = SystemTime::from(Utc.with_ymd_and_hms(2020, 1, 1, 0, 30, 0).unwrap());

    query.insert_placement(1, 0, 3, IpAddr::V4(Ipv4Addr::new(127, 2, 2, 2)), time1).await.unwrap();
    query.insert_placement(0, 0, 1, IpAddr::V4(Ipv4Addr::new(127, 0, 0, 0)), time2).await.unwrap();
    query.insert_placement(2, 0, 2, IpAddr::V4(Ipv4Addr::new(127, 1, 1, 1)), time3).await.unwrap();

    let time4 = Utc.with_ymd_and_hms(2020, 1, 1, 0, 45, 0).unwrap();
    let placements = query.get_placements(time4).await.unwrap();

    let expected = vec![Placement {
        x: 0,
        y: 0,
        color: 1,
        ipaddress: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 0)),
        placement_time: CqlTimestamp(0),
    }, Placement {
        x: 2,
        y: 0,
        color: 2,
        ipaddress: IpAddr::V4(Ipv4Addr::new(127, 1, 1, 1)),
        placement_time: CqlTimestamp(0),
    }];
    assert_eq!(placements, expected)
}

async fn test_times_placed_counter(query: &QueryStore) {
    query.session.query_unpaged("TRUNCATE pks.stats;", ()).await.unwrap();

    let ip = IpAddr::V4(Ipv4Addr::new(127, 2, 2, 2));

    let times1 = query.get_times_placed(ip).await.unwrap();

    query.increment_times_placed(ip).await.unwrap();

    let times2 = query.get_times_placed(ip).await.unwrap();

    query.increment_times_placed(ip).await.unwrap();
    query.increment_times_placed(ip).await.unwrap();

    let times3 = query.get_times_placed(ip).await.unwrap();

    assert_eq!(times1, 0);
    assert_eq!(times2, 1);
    assert_eq!(times3, 3)
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();
    
    info!("Started query integration test");
    
    let query = init_queries().await;
    
    test_get_then_update_tile(&query).await;
    test_get_tile_group(&query).await;
    test_insert_then_get_placements(&query).await;
    test_times_placed_counter(&query).await;
    
    info!("Successfully passed query integration test")
}