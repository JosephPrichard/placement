package dict

import (
	"context"
	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
	"image/color"
	"placement/app/models"
	"placement/app/utils"
	"testing"
)

func init() {
	models.GroupDim = 5
	models.GroupLen = models.GroupDim * models.GroupDim * 3
}

func teardownRedis(t *testing.T, rdb *redis.Client) {
	if err := rdb.FlushAll().Err(); err != nil {
		t.Fatalf("failed to flush redis db: %v", err)
	}
}

func TestEchoCachedGroup(t *testing.T) {
	rdb := utils.CreateRedis(utils.TestRedisURL)
	defer func() {
		_ = rdb.Close()
	}()
	defer teardownRedis(t, rdb)

	tgIn := models.TileGroup{}.SetTileOff(0, 0, color.RGBA{R: 255, G: 50, B: 150, A: 255})
	var tgOut models.TileGroup
	var err error

	ctx := context.WithValue(context.Background(), "trace", "test-cg-trace")

	if err = SetCachedGroup(ctx, rdb, 0, 0, tgIn); err != nil {
		t.Fatalf("failure in SetCachedGroup %v", err)
	}
	if tgOut, err = GetCachedGroup(ctx, rdb, 0, 0); err != nil {
		t.Fatalf("failure in GetCachedGroup %v", err)
	}

	assert.Equal(t, tgIn, tgOut)
}

func TestEmptyCachedGroup(t *testing.T) {
	rdb := utils.CreateRedis(utils.TestRedisURL)
	defer func() {
		_ = rdb.Close()
	}()
	defer teardownRedis(t, rdb)

	ctx := context.WithValue(context.Background(), "trace", "test-cg-trace")

	tgOut, err := GetCachedGroup(ctx, rdb, 0, 0)
	if err != nil {
		t.Fatalf("failure in GetCachedGroup %v", err)
	}

	assert.Equal(t, models.TileGroup{}, tgOut)
}

func TestBatchDrawCachedGroup(t *testing.T) {
	rdb := utils.CreateRedis(utils.TestRedisURL)
	defer func() {
		_ = rdb.Close()
	}()
	defer teardownRedis(t, rdb)

	ctx := context.WithValue(context.Background(), "trace", "test-cg-trace")

	dArr := []models.Draw{
		{X: 0, Y: 0, Rgb: color.RGBA{R: 1}},
		{X: 1, Y: 3, Rgb: color.RGBA{R: 2}},
		{X: 3, Y: 1, Rgb: color.RGBA{R: 3}},
		{X: 6, Y: 1, Rgb: color.RGBA{R: 4}},
	}

	if err := BatchDrawCachedGroup(ctx, rdb, dArr); err != nil {
		t.Fatalf("failure in GetCachedGroup %v", err)
	}

	type Assert struct {
		x, y  int
		expTg models.TileGroup
	}

	asserts := []Assert{
		{
			x: 0,
			y: 0,
			expTg: models.TileGroup{}.
				SetTileOff(0, 0, color.RGBA{R: 1}).
				SetTileOff(1, 3, color.RGBA{R: 2}).
				SetTileOff(3, 1, color.RGBA{R: 3}),
		},
		{x: 5, y: 0, expTg: models.TileGroup{}.SetTileOff(1, 1, color.RGBA{R: 4})},
		{x: 5, y: 5, expTg: models.TileGroup{}},
	}

	for _, a := range asserts {
		tg, err := GetCachedGroup(ctx, rdb, a.x, a.y)
		if err != nil {
			t.Fatalf("failure in GetCachedGroup %v", err)
		}
		assert.Equal(t, a.expTg, tg)
	}
}
