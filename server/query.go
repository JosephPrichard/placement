package server

import (
	"context"
	"errors"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/rs/zerolog/log"
	"image/color"
	"net"
	"time"
)

func GetTileGroup(ctx context.Context, session *gocql.Session, key GroupKey) (TileGroup, error) {
	query := session.Query("SELECT x, y, rgb FROM pks.tiles WHERE group_x = ? AND group_y = ?").
		Idempotent(true).
		Bind(key.x, key.y)

	scanner := query.IterContext(ctx).Scanner()

	group := TileGroup{}

	for scanner.Next() {
		var x, y int32
		var rgb color.RGBA

		if err := scanner.Scan(&x, &y, &rgb.R, &rgb.G, &rgb.B); err != nil {
			log.Err(err).Msg("Error while scanning row of GetTileGroup")
		}

		xOff := x - key.x
		yOff := y - key.y
		if xOff < 0 {
			xOff = 0
		}
		if yOff < 0 {
			yOff = 0
		}

		group = group.SetTile(xOff, yOff, rgb)
	}
	if err := scanner.Err(); err != nil {
		log.Err(err).Msg("Error while scanning results of GetTileGroup")
		return nil, err
	}

	log.Info().Type("key", key).Msg("Selected TileGroup")
	return group, nil
}

func scanPlacement(scanner gocql.Scanner) Tile {
	var placement Tile

	var lastUpdatedTime time.Time
	if err := scanner.Scan(&placement.d.x, &placement.d.y, &placement.d.rgb.R, &placement.d.rgb.G, &placement.d.rgb.B, &lastUpdatedTime); err != nil {
		log.Err(err).Msg("Error while scanning row of GetOneTile")
	}
	placement.date = lastUpdatedTime.String()

	return placement
}

func GetOneTile(ctx context.Context, session *gocql.Session, x, y int32) (Tile, error) {
	key := KeyFromPoint(x, y)

	query := session.Query("SELECT x, y, rgb, last_updated_time FROM pks.tiles WHERE group_x = ? AND group_y = ? AND x = ? AND y = ? LIMIT 1;").
		Idempotent(true).
		Bind(key.x, key.y, x, y)

	scanner := query.IterContext(ctx).Scanner()

	var tile Tile

	if scanner.Next() {
		tile = scanPlacement(scanner)
	} else {
		log.Warn().Msg("Error while scanning results of GetOneTile, expected to retrieve one row, got none")
		return Tile{}, errors.New("expected GetOneTile results to have one result, got none")
	}
	if err := scanner.Err(); err != nil {
		log.Err(err).Msg("Error while scanning results of GetOneTile")
		return Tile{}, err
	}

	log.Info().
		Type("key", key).Int32("x", x).Int32("y", y).Type("tile", tile).
		Msg("Selected OneTile")
	return tile, nil
}

func GetTiles(ctx context.Context, session *gocql.Session, day int64, after time.Duration) ([]Tile, error) {
	query := session.Query("SELECT x, y, rgb, placement_time FROM pks.placements WHERE day = ? AND placement_time <= ? ORDER BY placement_time ASC;").
		Idempotent(true).
		Bind(day, after.Milliseconds())

	scanner := query.IterContext(ctx).Scanner()

	var tiles []Tile

	for scanner.Next() {
		tile := scanPlacement(scanner)
		tiles = append(tiles, tile)
	}
	if err := scanner.Err(); err != nil {
		log.Err(err).Msg("Error while scanning results of GetOneTile")
		return nil, err
	}

	log.Info().
		Type("day", day).Dur("after", after).Type("tiles", tiles).
		Msg("Selected Tiles")
	return tiles, nil
}

func BatchUpsertTile(ctx context.Context, session *gocql.Session, x, y int32, rgb color.RGBA, ip net.IP, placementTime time.Time) error {
	key := KeyFromPoint(x, y)

	day := placementTime.Day()

	batch := session.Batch(gocql.LoggedBatch)
	batch.Query(
		"INSERT INTO pks.tiles (group_x, group_y, x, y, rgb, last_updated_ipaddress, last_updated_time) VALUES (?, ?, ?, ?, ?, ?, ?);",
		key.x, key.y, x, y, rgb, ip, placementTime)
	batch.Query(
		"INSERT INTO pks.placements (day, x, y, rgb, ipaddress, placement_time) VALUES (?, ?, ?, ?, ?, ?);",
		day, x, y, rgb, ip, placementTime)

	if err := batch.ExecContext(ctx); err != nil {
		log.Err(err).Msg("Error while executing BatchUpsertTile")
		return err
	}

	log.Info().
		Type("day", day).Int32("x", x).Int32("y", y).Type("key", key).Type("rgb", rgb).Type("ip", ip).Time("placement_time", placementTime).
		Msg("Executed tile upsert batch")
	return nil
}
