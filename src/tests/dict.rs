use deadpool_redis::{Config, Pool, Runtime};
use crate::backend::dict::{get_cached_group, set_cached_group, upsert_cached_group};
use crate::backend::models::{DrawEvent, GroupKey, TileGroup, GROUP_DIM_I32};

async fn test_get_set_cache(redis: &Pool) {
    let conn = &mut redis.get().await.unwrap();

    let mut group1 = TileGroup::empty();
    group1.set(1, 1, (0, 0, 0));
    group1.set(2, 2, (1, 1, 1));
    let group1_expected = group1.clone();

    let mut group2 = TileGroup::empty();
    group2.set(1, 1, (0, 0, 0));
    group2.set(2, 2, (1, 1, 1));
    let group2_expected = group2.clone();

    set_cached_group(conn, GroupKey(0, 0), &group1).await.unwrap();
    set_cached_group(conn, GroupKey(GROUP_DIM_I32, GROUP_DIM_I32), &group2).await.unwrap();

    let group1 = get_cached_group(conn, GroupKey(0, 0)).await.unwrap();
    let group2 = get_cached_group(conn, GroupKey(GROUP_DIM_I32, GROUP_DIM_I32)).await.unwrap();
    let group3 = get_cached_group(conn, GroupKey(GROUP_DIM_I32 * 2, GROUP_DIM_I32 * 2)).await.unwrap();

    assert_eq!(Some(group1_expected), group1);
    assert_eq!(Some(group2_expected), group2);
    assert_eq!(None, group3);
}

async fn test_update_cache(redis: &Pool) {
    let conn = &mut redis.get().await.unwrap();
    
    let mut group1 = TileGroup::empty();
    group1.set(1, 1, (0, 0, 0));
    group1.set(2, 2, (1, 1, 1));

    set_cached_group(conn, GroupKey(0, 0), &group1).await.unwrap();
    upsert_cached_group(conn, DrawEvent { x: 3, y: 3, rgb: (2, 2, 2) }).await.unwrap(); // this will update group 1
    upsert_cached_group(conn, DrawEvent { x: GROUP_DIM_I32 + 3, y: GROUP_DIM_I32 * 2, rgb: (3, 3, 3) }).await.unwrap(); // this will create then update group 2

    let mut group1_expected = group1.clone();
    group1_expected.set(3, 3, (2, 2, 2));

    let mut group2_expected = TileGroup::empty();
    group2_expected.set(3, 0, (3, 3, 3));

    let group1 = get_cached_group(conn, GroupKey(0, 0)).await.unwrap();
    let group2 = get_cached_group(conn, GroupKey(GROUP_DIM_I32, GROUP_DIM_I32 * 2)).await.unwrap();

    assert_eq!(Some(group1_expected), group1);
    assert_eq!(Some(group2_expected), group2);
}

async fn test_redis_lock() {

}

pub async fn test_cache() {
    let port = 6380;
    let redis_url = format!("redis://127.0.0.1:{}/", port);
    let redis = Config::from_url(redis_url).create_pool(Some(Runtime::Tokio1)).unwrap();

    test_get_set_cache(&redis).await;
    test_update_cache(&redis).await;
}
