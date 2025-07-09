package server

import (
	"context"
	_ "embed"
	"errors"
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

func SetCachedGroup(ctx context.Context, rdb *redis.Client, key GroupKey, group TileGroup) error {
	trace := ctx.Value("trace")

	keyStr := fmt.Sprintf("(%d,%d)", key.X, key.Y)

	err := rdb.Set(keyStr, string(group), 0).Err()
	if err != nil {
		log.Err(err).Any("trace", trace).Str("key", keyStr).Msg("failed to set tile group into cache")
		return err
	}

	log.Info().Any("trace", trace).Str("key", keyStr).Msg("set tile group into cache")
	return nil
}

func GetCachedGroup(ctx context.Context, rdb *redis.Client, key GroupKey) (TileGroup, error) {
	trace := ctx.Value("trace")

	keyStr := fmt.Sprintf("(%d,%d)", key.X, key.Y)

	value, err := rdb.Get(keyStr).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) || value == "" {
			log.Info().Any("trace", trace).Str("key", keyStr).Msg("got an empty or nil tile group from cache")
			return nil, nil
		}
		log.Err(err).Str("key", keyStr).Msg("failed to get tile group from cache")
		return nil, err
	}
	if len(value) != GroupLen {
		log.Warn().Any("trace", trace).Str("key", keyStr).Int("len", len(value)).Msg("invalid length for cached tile group")
		return nil, fmt.Errorf("internal service error: invalid cached group length")
	}

	return []byte(value), nil
}

func InitCachedGroup(ctx context.Context, rdb *redis.Client, key string) error {
	trace := ctx.Value("trace")

	status, err := zeroInit.Run(rdb, nil, key, GroupLen).Result()
	if err != nil {
		log.Err(err).Any("trace", trace).Str("key", key).Msg("failed to init tile group in the cache")
		return err
	}

	log.Info().Any("trace", trace).Str("key", key).Any("status", status).Msg("init tile group in cache")
	return nil
}

func UpsertCachedGroup(ctx context.Context, rdb *redis.Client, d Draw) error {
	trace := ctx.Value("trace")

	key := KeyFromPoint(d.X, d.Y)
	keyStr := fmt.Sprintf("(%d,%d)", d.X, d.Y)

	err := InitCachedGroup(ctx, rdb, keyStr)
	if err != nil {
		return err
	}

	xOff := d.X - key.X
	yOff := d.Y - key.Y
	if xOff < 0 {
		xOff = 0
	}
	if yOff < 0 {
		yOff = 0
	}

	byteOff := GetTgOffset(xOff, yOff)
	rgbBytes := []byte{d.Rgb.R, d.Rgb.G, d.Rgb.B}

	_, err = rdb.SetRange(keyStr, int64(byteOff), string(rgbBytes)).Result()
	if err != nil {
		log.Err(err).
			Any("trace", trace).Str("key", keyStr).Any("draw", d).Int("offset", byteOff).Bytes("rgbBytes", rgbBytes).
			Msg("failed to upsert tile group into cache")
		return err
	}

	return nil
}

// AcquireExpiringLock returns -1 if the time is updated, the time stored at the key if not
func AcquireExpiringLock(ctx context.Context, rdb *redis.Client, key string, timeAcquiring time.Time, timeMaybeAcquired time.Time, timeExpires time.Duration) (int64, error) {
	trace := ctx.Value("trace")

	arg1, arg2, arg3 := timeAcquiring.Unix(), timeMaybeAcquired.Unix(), timeExpires.Seconds()
	status, err := expireLock.Run(rdb, nil, key, arg1, arg2, arg3).Result()
	if err != nil {
		log.Err(err).Any("trace", trace).Str("key", key).Msg("failed to acquire an expiring lock")
		return 0, err
	}

	log.Info().Any("trace", trace).Str("key", key).Any("status", status).Msg("updated placement in cache")

	ret, ok := status.(int64)
	if !ok {
		return 0, fmt.Errorf("internal service error: invalid expiring lock status value: %d, must be int64", status)
	}
	return ret, nil
}
