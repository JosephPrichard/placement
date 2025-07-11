package dict

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/rs/zerolog/log"
	"math"
	"placement/app/models"
	"time"
)

//go:embed EXPIRELOCK.lua
var expireLockScript string

//go:embed ZEROINIT.lua
var zeroInitScript string

var expireLock = redis.NewScript(expireLockScript)
var zeroInit = redis.NewScript(zeroInitScript)

func SetCachedGroup(ctx context.Context, rdb *redis.Client, x, y int, group models.TileGroup) error {
	trace := ctx.Value("trace")
	keyStr := models.KeyFromPoint(x, y).String()

	err := rdb.Set(keyStr, string(group), 0).Err()
	if err != nil {
		log.Err(err).Any("trace", trace).Str("key", keyStr).Msg("failed to set tile group into cache")
		return err
	}

	log.Info().Any("trace", trace).Str("key", keyStr).Msg("set tile group into cache")
	return nil
}

func GetCachedGroup(ctx context.Context, rdb *redis.Client, x, y int) (models.TileGroup, error) {
	trace := ctx.Value("trace")
	keyStr := models.KeyFromPoint(x, y).String()

	value, err := rdb.Get(keyStr).Result()
	if errors.Is(err, redis.Nil) || (value == "" && err == nil) {
		log.Info().Any("trace", trace).Str("key", keyStr).Msg("got an empty or nil tile group from cache")
		return models.TileGroup{}, nil
	}
	if err != nil {
		log.Err(err).Str("key", keyStr).Msg("failed to get tile group from cache")
		return nil, err
	}
	if len(value) != models.GroupLen {
		log.Warn().Any("trace", trace).Str("key", keyStr).Int("len", len(value)).Msg("invalid length for cached tile group")
		return nil, fmt.Errorf("app service error: invalid cached group length")
	}

	log.Info().Any("trace", trace).Str("key", keyStr).Msg("retrieved a tile group from the cache")
	return models.TileGroup(value), nil
}

func InitCachedGroup(ctx context.Context, rdb *redis.Client, key string) error {
	trace := ctx.Value("trace")

	status, err := zeroInit.Run(rdb, nil, key, models.GroupLen).Result()
	if err != nil {
		log.Err(err).Any("trace", trace).Str("key", key).Msg("failed to init tile group in the cache")
		return err
	}

	log.Info().Any("trace", trace).Str("key", key).Any("status", status).Msg("init tile group in cache")
	return nil
}

func rangeArgs(key models.GroupKey, d models.Draw) (int64, []byte) {
	xOff := int(math.Abs(float64(d.X - key.X)))
	yOff := int(math.Abs(float64(d.Y - key.Y)))
	byteOff := int64(models.GetTgOffset(xOff, yOff))
	rgbBytes := []byte{d.Rgb.R, d.Rgb.G, d.Rgb.B}
	return byteOff, rgbBytes
}

func UpsertCachedGroup(ctx context.Context, rdb *redis.Client, d models.Draw) error {
	trace := ctx.Value("trace")

	key := models.KeyFromPoint(d.X, d.Y)
	keyStr := key.String()

	if err := InitCachedGroup(ctx, rdb, keyStr); err != nil {
		return err
	}

	byteOff, rgbBytes := rangeArgs(key, d)

	if _, err := rdb.SetRange(keyStr, byteOff, string(rgbBytes)).Result(); err != nil {
		log.Err(err).
			Any("trace", trace).Str("key", keyStr).Any("draw", d).Int64("offset", byteOff).Bytes("rgbBytes", rgbBytes).
			Msg("failed to upsert tile group into cache")
		return err
	}

	log.Info().Any("trace", trace).Any("key", key).Int64("offset", byteOff).Msg("upsert tile group into the cache")
	return nil
}

func BatchCachedGroup(ctx context.Context, rdb *redis.Client, keyStr string, group []models.Draw) error {
	trace := ctx.Value("trace")

	if err := InitCachedGroup(ctx, rdb, keyStr); err != nil {
		return err
	}

	p := rdb.TxPipeline()
	defer func() {
		if err := p.Close(); err != nil {
			log.Panic().Err(err).Str("key", keyStr).Msg("failed to close BatchCachedGroup pipeline")
		}
	}()

	for _, d := range group {
		key := models.KeyFromPoint(d.X, d.Y)
		byteOff, rgbBytes := rangeArgs(key, d)

		if _, err := p.SetRange(keyStr, byteOff, string(rgbBytes)).Result(); err != nil {
			log.Err(err).Any("trace", trace).Str("key", keyStr).Msg("failed to add upsert tile group to pipeline")
			return err
		}
		if _, err := p.Exec(); err != nil {
			log.Err(err).Any("trace", trace).Str("key", keyStr).Msg("failed to exec upsert tile group pipeline")
			return err
		}
		log.Info().Any("trace", trace).Any("key", key).Msg("set tile group into the cache")
	}

	return nil
}

func BatchDrawCachedGroup(ctx context.Context, rdb *redis.Client, dArr []models.Draw) error {
	aggGroups := make(map[string][]models.Draw)

	for _, d := range dArr {
		key := models.KeyFromPoint(d.X, d.Y).String()
		group := aggGroups[key]
		group = append(group, d)
		aggGroups[key] = group
	}

	for keyStr, group := range aggGroups {
		if err := BatchCachedGroup(ctx, rdb, keyStr, group); err != nil {
			return err
		}
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

	log.Info().Any("trace", trace).Str("key", key).Any("status", status).Msg("acquired a lock with status")

	ret, ok := status.(int64)
	if !ok {
		return 0, fmt.Errorf("app service error: invalid expiring lock status value: %d, must be int64", status)
	}
	return ret, nil
}
