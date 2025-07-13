package dict

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"placement/app/models"
	"time"

	"github.com/go-redis/redis"
)

//go:embed EXPIRELOCK.lua
var expireLockScript string

//go:embed ZEROINIT.lua
var zeroInitScript string

var expireLock = redis.NewScript(expireLockScript)
var zeroInit = redis.NewScript(zeroInitScript)

const TestRedisURL = "redis://127.0.0.1:6380/"

func CreateRedis(redisURL string) *redis.Client {
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		slog.Error("failed to connect to redis", "redisURL", redisURL, "error", err)
		panic(err)
	}
	return redis.NewClient(opt)
}

func SetCachedGroup(ctx context.Context, rdb *redis.Client, x, y int, group models.TileGroup) error {
	trace := ctx.Value("trace")
	keyStr := models.KeyFromPoint(x, y).String()

	err := rdb.Set(keyStr, string(group), 0).Err()
	if err != nil {
		slog.Error("failed to set tile group into cache", "trace", trace, "key", keyStr, "error", err)
		return err
	}

	slog.Info("set tile group into cache", "trace", trace, "key", keyStr)
	return nil
}

func GetCachedGroup(ctx context.Context, rdb *redis.Client, x, y int) (models.TileGroup, error) {
	trace := ctx.Value("trace")
	keyStr := models.KeyFromPoint(x, y).String()

	value, err := rdb.Get(keyStr).Result()
	if errors.Is(err, redis.Nil) || (value == "" && err == nil) {
		slog.Info("got an empty or nil tile group from cache", "trace", trace, "key", keyStr)
		return models.TileGroup{}, nil
	}
	if err != nil {
		slog.Error("failed to get tile group from cache", "trace", trace, "key", keyStr, "error", err)
		return nil, err
	}
	if len(value) != models.GroupLen {
		slog.Warn("invalid length for cached tile group", "trace", trace, "key", keyStr, "len", len(value))
		return nil, fmt.Errorf("app service error: invalid cached group length")
	}

	slog.Info("retrieved a tile group from the cache", "trace", trace, "key", keyStr)
	return models.TileGroup(value), nil
}

func InitCachedGroup(ctx context.Context, rdb *redis.Client, key string) error {
	trace := ctx.Value("trace")

	status, err := zeroInit.Run(rdb, nil, key, models.GroupLen).Result()
	if err != nil {
		slog.Error("failed to init tile group in the cache", "trace", trace, "key", key, "error", err)
		return err
	}

	slog.Info("init tile group in cache", "trace", trace, "key", key, "status", status)
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
		slog.Error("failed to upsert tile group into cache", "trace", trace, "key", keyStr, "draw", d, "offset", byteOff, "rgbBytes", rgbBytes, "error", err)
		return err
	}

	slog.Info("upsert tile group into the cache", "trace", trace, "key", key, "offset", byteOff)
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
			slog.Error("failed to close BatchCachedGroup pipeline", "key", keyStr, "error", err)
			panic(err)
		}
	}()

	for _, d := range group {
		key := models.KeyFromPoint(d.X, d.Y)
		byteOff, rgbBytes := rangeArgs(key, d)

		if _, err := p.SetRange(keyStr, byteOff, string(rgbBytes)).Result(); err != nil {
			slog.Error("failed to add upsert tile group to pipeline", "trace", trace, "key", keyStr, "error", err)
			return err
		}
		if _, err := p.Exec(); err != nil {
			slog.Error("failed to exec upsert tile group pipeline", "trace", trace, "key", keyStr, "error", err)
			return err
		}
		slog.Info("set tile group into the cache", "trace", trace, "key", key)
	}

	return nil
}

func BatchDrawCachedGroup(ctx context.Context, rdb *redis.Client, dArr []models.Draw) error {
	aggGroups := make(map[string][]models.Draw)

	for _, d := range dArr {
		key := models.KeyFromPoint(d.X, d.Y).String()
		aggGroups[key] = append(aggGroups[key], d)
	}

	for keyStr, group := range aggGroups {
		if err := BatchCachedGroup(ctx, rdb, keyStr, group); err != nil {
			return err
		}
	}
	return nil
}

func AcquireExpiringLock(ctx context.Context, rdb *redis.Client, key string, timeAcquiring, timeMaybeAcquired time.Time, timeExpires time.Duration) (int64, error) {
	trace := ctx.Value("trace")

	arg1, arg2, arg3 := timeAcquiring.Unix(), timeMaybeAcquired.Unix(), timeExpires.Seconds()
	status, err := expireLock.Run(rdb, nil, key, arg1, arg2, arg3).Result()
	if err != nil {
		slog.Error("failed to acquire an expiring lock", "trace", trace, "key", key, "error", err)
		return 0, err
	}

	slog.Info("acquired a lock with status", "trace", trace, "key", key, "status", status)

	ret, ok := status.(int64)
	if !ok {
		return 0, fmt.Errorf("app service error: invalid expiring lock status value: %v, must be int64", status)
	}
	return ret, nil
}
