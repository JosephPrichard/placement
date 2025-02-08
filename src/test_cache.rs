use std::sync::Arc;
use tracing::log::info;
use server::cache::{get_tile_group, set_tile_group};
use server::models::{GroupKey, TileGroup, GROUP_DIM};

mod server;

async fn test_cache() {
    let client = Arc::new(redis::Client::open("redis://127.0.0.1:6380/").unwrap());

    let conn = &mut client.get_connection().unwrap();

    let mut group1 = TileGroup::empty();
    group1.set(1, 1, (0, 0, 0));
    group1.set(2, 2, (1, 1, 1));
    let group1_expected = group1.clone();
    set_tile_group(conn, GroupKey(0, 0), group1).unwrap();

    let mut group2 = TileGroup::empty();
    group2.set(1, 1, (0, 0, 0));
    group2.set(2, 2, (1, 1, 1));
    let group2_expected = group2.clone();
    set_tile_group(conn, GroupKey(GROUP_DIM as i32, GROUP_DIM as i32), group2).unwrap();
    
    let group1 = get_tile_group(conn, GroupKey(0, 0)).unwrap();
    let group2 = get_tile_group(conn, GroupKey(GROUP_DIM as i32, GROUP_DIM as i32)).unwrap();
    let group3 = get_tile_group(conn, GroupKey((GROUP_DIM * 2) as i32, (GROUP_DIM * 2) as i32)).unwrap();
    
    assert_eq!(Some(group1_expected), group1);
    assert_eq!(Some(group2_expected), group2);
    assert_eq!(None, group3);
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();

    info!("Starting cache integration test");
    
    test_cache().await;
    
    info!("Successfully passed cache integration test");
}