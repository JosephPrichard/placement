use crate::backend::models::{GroupKey, Placement, ServiceError, Tile, TileGroup, GROUP_DIM};
use crate::backend::query::{create_schema, QueryStore};
use chrono::{TimeZone, Utc};
use scylla::SessionBuilder;
use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;
use std::time::SystemTime;
use tracing::log::info;

async fn init_queries(port: u16) -> QueryStore {
    let session = SessionBuilder::new()
        .known_node(format!("127.0.0.1:{}", port))
        .build()
        .await
        .unwrap();
    info!("Started the scylla session");

    session.query_unpaged("DROP KEYSPACE IF EXISTS pks;", ()).await.unwrap();
    create_schema(&session).await.unwrap();
    info!("Created the scylla schema");

    QueryStore::init_queries(session).await.unwrap()
}

async fn test_get_then_update_tile(query: &QueryStore) {
    query.session.query_unpaged("TRUNCATE pks.tiles;", ()).await.unwrap();

    let tile1 = query.get_one_tile(1, 1).await;

    let time = SystemTime::from(Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap());

    query.update_tile(5, 4, (2, 2, 2), IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), time).await.unwrap();
    let tile2 = query.get_one_tile(5, 4).await;

    assert_eq!(tile1, Err(ServiceError::NotFoundError(String::from("tile not found at given location: (1, 1)"))));
    assert_eq!(tile2, Ok(Tile { x: 5, y: 4, rgb: (2, 2, 2), date: "2025/01/01 00:00:00".to_string() }));
}

async fn test_get_tile_group(query: &QueryStore) {
    query.session.query_unpaged("TRUNCATE pks.tiles;", ()).await.unwrap();

    query.update_tile_now(0, 0, (1, 1, 1), IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))).await.unwrap();
    query.update_tile_now(2, 0, (5, 5, 5), IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))).await.unwrap();
    query.update_tile_now((GROUP_DIM + 1) as i32, (GROUP_DIM + 1) as i32, (6, 6, 6), IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))).await.unwrap();
    let tiles = query.get_tile_group(GroupKey(0, 0)).await.unwrap();

    let mut expected = TileGroup::empty(0, 0);
    expected.set(0, 0, (1, 1, 1));
    expected.set(2, 0, (5, 5, 5));

    assert_eq!(tiles, expected)
}

async fn test_insert_then_get_placements(query: &QueryStore) {
    query.session.query_unpaged("TRUNCATE pks.placements;", ()).await.unwrap();

    let time1 = SystemTime::from(Utc.with_ymd_and_hms(2015, 1, 1, 0, 0, 0).unwrap());
    let time2 = SystemTime::from(Utc.with_ymd_and_hms(2020, 1, 1, 0, 15, 0).unwrap());
    let time3 = SystemTime::from(Utc.with_ymd_and_hms(2020, 1, 1, 0, 30, 0).unwrap());

    query.insert_placement(1, 0, (3, 3, 3), IpAddr::V4(Ipv4Addr::new(127, 2, 2, 2)), time1).await.unwrap();
    query.insert_placement(0, 0, (1, 1, 1), IpAddr::V4(Ipv4Addr::new(127, 0, 0, 0)), time2).await.unwrap();
    query.insert_placement(2, 0, (2, 2, 2), IpAddr::V4(Ipv4Addr::new(127, 1, 1, 1)), time3).await.unwrap();

    let time4 = Utc.with_ymd_and_hms(2020, 1, 1, 0, 45, 0).unwrap();
    let placements = query.get_placements(time4).await.unwrap();

    let expected = vec![Placement {
        x: 0,
        y: 0,
        rgb: (1, 1, 1),
        ipaddress: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 0)),
        placement_date: "2020/01/01 00:15:00".to_string(),
    }, Placement {
        x: 2,
        y: 0,
        rgb: (2, 2, 2),
        ipaddress: IpAddr::V4(Ipv4Addr::new(127, 1, 1, 1)),
        placement_date: "2020/01/01 00:30:00".to_string(),
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

pub async fn test_query() {
    let port = 9043;
    let query = init_queries(port).await;
    info!("Successfully initialized scylla queries");

    test_get_then_update_tile(&query).await;
    test_get_tile_group(&query).await;
    test_insert_then_get_placements(&query).await;
    test_times_placed_counter(&query).await;
}
