use std::sync::Arc;
use crate::backend::cache::{get_tile_group, set_tile_group};
use crate::backend::models::{GroupKey, TileGroup, GROUP_DIM, GROUP_DIM_I32};

pub async fn test_cache() {
    let port = 6380;
    let client = Arc::new(redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap());

    let conn = &mut client.get_connection().unwrap();

    let mut group1 = TileGroup::empty(0, 0);
    group1.set(1, 1, (0, 0, 0));
    group1.set(2, 2, (1, 1, 1));
    let group1_expected = group1.clone();
    set_tile_group(conn, GroupKey(0, 0), group1).unwrap();

    let mut group2 = TileGroup::empty(GROUP_DIM_I32, GROUP_DIM_I32);
    group2.set(1, 1, (0, 0, 0));
    group2.set(2, 2, (1, 1, 1));
    let group2_expected = group2.clone();
    set_tile_group(conn, GroupKey(GROUP_DIM_I32, GROUP_DIM_I32), group2).unwrap();

    let group1 = get_tile_group(conn, GroupKey(0, 0)).unwrap();
    let group2 = get_tile_group(conn, GroupKey(GROUP_DIM_I32, GROUP_DIM_I32)).unwrap();
    let group3 = get_tile_group(conn, GroupKey(GROUP_DIM_I32 * 2, GROUP_DIM_I32 * 2)).unwrap();

    assert_eq!(Some(group1_expected), group1);
    assert_eq!(Some(group2_expected), group2);
    assert_eq!(None, group3);
}
