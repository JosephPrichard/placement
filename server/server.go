package server

import (
	"context"
	"fmt"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/go-redis/redis"
	"github.com/rs/zerolog/log"
	"net"
	"time"
)

const DrawPeriod = time.Minute

type State struct {
	rdb       *redis.Client
	cassandra *gocql.Session
}

func GetGroup(ctx context.Context, state State, key GroupKey) (TileGroup, error) {
	var tg TileGroup

	tg, err := GetCachedGroup(state.rdb, key)
	if err != nil {
		return nil, err
	}
	if tg == nil {
		tg, err = GetTileGroup(ctx, state.cassandra, key)
		if err != nil {
			return nil, err
		}
	}
	err = SetCachedGroup(state.rdb, key, tg)
	if err != nil {
		return nil, err
	}

	return tg, nil
}

func DrawTile(ctx context.Context, state State, draw Draw, ip net.IP) error {
	now := time.Now()

	ret, err := AcquireExpiringLock(state.rdb, ip.String(), now, now.Add(-DrawPeriod), DrawPeriod)
	if err != nil {
		return err
	}

	if ret >= 0 {
		timePlaced := time.Unix(ret, 0)
		if timePlaced.After(now) {
			return fmt.Errorf("invariant is false: timePlaced=%v is not smaller than now=%v", timePlaced, now)
		}
		difference := now.Sub(timePlaced)
		if difference > DrawPeriod {
			return fmt.Errorf("invariant is false: difference=%v is not smaller than DrawPeriod=%v", difference, DrawPeriod)
		}
		remaining := DrawPeriod - difference
		return fmt.Errorf("%s minutes remaining until player can draw another tile", remaining)
	}

	// this will be what will immediately reflect if other users can see this update, so we must handle this error sync. other tasks just need to finish eventually.
	err = UpsertCachedGroup(state.rdb, draw)
	if err != nil {
		return err
	}
	go func() {
		if err := BatchUpsertTile(ctx, state.cassandra, draw.x, draw.y, draw.rgb, ip, now); err != nil {
			log.Err(err).Msg("BatchUpsertTile failed in background task")
		}
	}()
	go func() {
		if err := BroadcastDraw(state.rdb, draw); err != nil {
			log.Err(err).Msg("BroadcastDraw failed in background task")
		}
	}()

	return nil
}
