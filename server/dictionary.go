package server

import (
	_ "embed"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/rs/zerolog/log"
	"time"
)

//go:embed EXPIRELOCK.lua
var expireLockScript string

//go:embed ZEROINIT.lua
var zeroInitScript string

var expireLock = redis.NewScript(expireLockScript)
var zeroInit = redis.NewScript(zeroInitScript)

func SetCachedGroup(rdb *redis.Client, key GroupKey, group TileGroup) error {
	keyStr := fmt.Sprintf("(%d,%d)", key.x, key.y)

	err := rdb.Set(keyStr, string(group), 0).Err()
	if err != nil {
		log.Err(err).
			Str("key", keyStr).
			Msg("Failed to set tile group into cache")
		return err
	}

	log.Info().
		Str("key", keyStr).
		Msg("Set tile group into cache")
	return nil
}

func GetCachedGroup(rdb *redis.Client, key GroupKey) (TileGroup, error) {
	keyStr := fmt.Sprintf("(%d,%d)", key.x, key.y)

	value, err := rdb.Get(keyStr).Result()
	if err != nil {
		log.Err(err).
			Str("key", keyStr).
			Msg("Failed to get tile group from cache")
		return nil, err
	}

	log.Info().
		Str("key", keyStr).Int("len", len(value)).
		Msg("Got tile group into cache")

	if len(value) == 0 {
		return nil, nil
	} else if len(value) != GroupLen {
		log.Warn().
			Str("key", keyStr).Int("len", len(value)).
			Msg("Failed to get tile group from cache")
		return nil, fmt.Errorf("internal service error: invalid cached group length")
	}

	return []byte(value), nil
}

func InitCachedGroup(rdb *redis.Client, key string) error {
	status, err := zeroInit.Run(rdb, nil, key, GroupLen).Result()
	if err != nil {
		log.Err(err).
			Str("key", key).
			Msg("Failed to init tile group in the cache")
		return err
	}

	log.Info().
		Str("key", key).Any("status", status).
		Msg("Init tile group in cache")
	return nil
}

func UpsertCachedGroup(rdb *redis.Client, d Draw) error {
	key := KeyFromPoint(d.x, d.y)
	keyStr := fmt.Sprintf("(%d,%d)", d.x, d.y)

	err := InitCachedGroup(rdb, keyStr)
	if err != nil {
		return err
	}

	xOff := d.x - key.x
	yOff := d.y - key.y
	if xOff < 0 {
		xOff = 0
	}
	if yOff < 0 {
		yOff = 0
	}

	byteOff := GetTgOffset(xOff, yOff)
	rgbBytes := []byte{d.rgb.R, d.rgb.G, d.rgb.B}

	_, err = rdb.SetRange(keyStr, int64(byteOff), string(rgbBytes)).Result()
	if err != nil {
		log.Err(err).
			Str("key", keyStr).Type("draw", d).Int32("offset", byteOff).Bytes("rgbBytes", rgbBytes).
			Msg("Failed to upsert tile group into cache")
		return err
	}

	return nil
}

// AcquireExpiringLock returns -1 if the time is updated, the time stored at the key if not
func AcquireExpiringLock(rdb *redis.Client, key string, timeAcquiring time.Time, timeMaybeAcquired time.Time, timeExpires time.Duration) (int64, error) {
	status, err := expireLock.Run(rdb, nil, key, timeAcquiring.Second(), timeMaybeAcquired.Second(), timeExpires.Seconds()).Result()
	if err != nil {
		log.Err(err).
			Str("key", key).
			Msg("Failed to init tile group in the cache")
		return 0, err
	}

	log.Info().
		Str("key", key).Any("status", status).
		Msg("Updated placement in cache")
	return status.(int64), nil
}
