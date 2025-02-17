use crate::backend::cache::{get_cached_tile_group, set_cached_tile_group};
use crate::backend::models::{GroupKey, TileGroup, GROUP_DIM_I32};
use bb8_redis::bb8::Pool;
use bb8_redis::RedisConnectionManager;

pub async fn test_cache() {
    let port = 6380;
    let redis_url = format!("redis://127.0.0.1:{}/", port);
    let redis = Pool::builder().build(RedisConnectionManager::new(redis_url).unwrap()).await.unwrap();

    let mut group1 = TileGroup::empty(0, 0);
    group1.set(1, 1, (0, 0, 0));
    group1.set(2, 2, (1, 1, 1));
    let group1_expected = group1.clone();
    set_cached_tile_group(&redis, GroupKey(0, 0), &group1).await.unwrap();

    let mut group2 = TileGroup::empty(GROUP_DIM_I32, GROUP_DIM_I32);
    group2.set(1, 1, (0, 0, 0));
    group2.set(2, 2, (1, 1, 1));
    let group2_expected = group2.clone();
    set_cached_tile_group(&redis, GroupKey(GROUP_DIM_I32, GROUP_DIM_I32), &group2).await.unwrap();

    let group1 = get_cached_tile_group(&redis, GroupKey(0, 0)).await.unwrap();
    let group2 = get_cached_tile_group(&redis, GroupKey(GROUP_DIM_I32, GROUP_DIM_I32)).await.unwrap();
    let group3 = get_cached_tile_group(&redis, GroupKey(GROUP_DIM_I32 * 2, GROUP_DIM_I32 * 2)).await.unwrap();

    assert_eq!(Some(group1_expected), group1);
    assert_eq!(Some(group2_expected), group2);
    assert_eq!(None, group3);
}
