package server

import (
	"context"
	"errors"
	"github.com/gocql/gocql"
	"github.com/rs/zerolog/log"
	"image/color"
	"net"
	"time"
)

func GetTileGroup(ctx context.Context, cdb *gocql.Session, key GroupKey) (TileGroup, error) {
	trace := ctx.Value("trace")

	query := cdb.Query(`SELECT x, y, r, g, b 
			FROM pks.tiles 
			WHERE group_x = ? AND group_y = ?`).
		WithContext(ctx).
		Idempotent(true).
		Bind(key.X, key.Y)

	scanner := query.Iter().Scanner()

	group := TileGroup{}

	for scanner.Next() {
		var x, y int
		var rgb color.RGBA

		if err := scanner.Scan(&x, &y, &rgb.R, &rgb.G, &rgb.B); err != nil {
			log.Err(err).Any("trace", trace).Msg("Error while scanning row of GetTileGroup")
		}

		xOff := x - key.X
		yOff := y - key.Y
		if xOff < 0 {
			xOff = 0
		}
		if yOff < 0 {
			yOff = 0
		}

		group = group.SetTile(xOff, yOff, rgb)
	}
	if err := scanner.Err(); err != nil {
		log.Err(err).Any("trace", trace).Msg("Error while scanning results of GetTileGroup")
		return nil, err
	}

	log.Info().Any("trace", trace).Any("key", key).Msg("Selected TileGroup")
	return group, nil
}

func scanTile(ctx context.Context, scanner gocql.Scanner) Tile {
	trace := ctx.Value("trace")

	var tile Tile
	var t time.Time

	if err := scanner.Scan(&tile.D.X, &tile.D.Y, &tile.D.Rgb.R, &tile.D.Rgb.G, &tile.D.Rgb.B, &t); err != nil {
		log.Err(err).Any("trace", trace).Msg("Error while scanning row of GetOneTile")
	}
	tile.Date = t.String()

	return tile
}

var TileNotFoundError = errors.New("tile not found")

func GetOneTile(ctx context.Context, cdb *gocql.Session, x, y int) (Tile, error) {
	trace := ctx.Value("trace")

	key := KeyFromPoint(x, y)

	query := cdb.Query(`SELECT x, y, r, g, b, last_updated_time
			FROM pks.tiles 
			WHERE group_x = ? AND group_y = ? AND X = ? AND y = ? LIMIT 1;`).
		WithContext(ctx).
		Idempotent(true).
		Bind(key.X, key.Y, x, y)

	scanner := query.Iter().Scanner()

	var tile Tile

	if scanner.Next() {
		tile = scanTile(ctx, scanner)
	} else {
		log.Info().Any("trace", trace).Msg("Error while scanning results of GetOneTile, expected to retrieve one row, got none")
		return Tile{}, TileNotFoundError
	}
	if err := scanner.Err(); err != nil {
		log.Err(err).Any("trace", trace).Msg("Error while scanning results of GetOneTile")
		return Tile{}, err
	}

	log.Info().
		Any("trace", trace).
		Any("key", key).Int("X", x).Int("y", y).Any("tile", tile).
		Msg("Selected OneTile")
	return tile, nil
}

func GetTiles(ctx context.Context, cdb *gocql.Session, day int64, after time.Time) ([]Tile, error) {
	trace := ctx.Value("trace")

	query := cdb.Query(`SELECT x, y, r, g, b, placement_time 
			FROM pks.placements 
			WHERE day = ? AND placement_time <= ? ORDER BY placement_time ASC;`).
		WithContext(ctx).
		Idempotent(true).
		Bind(day, after)

	scanner := query.Iter().Scanner()

	var tiles []Tile

	for scanner.Next() {
		tile := scanTile(ctx, scanner)
		tiles = append(tiles, tile)
	}
	if err := scanner.Err(); err != nil {
		log.Err(err).Any("trace", trace).Msg("Error while scanning results of GetOneTile")
		return nil, err
	}

	log.Info().
		Any("trace", trace).
		Int64("day", day).Time("after", after).Type("tiles", tiles).
		Msg("Selected Tiles")
	return tiles, nil
}

type BatchUpsertArgs struct {
	X             int        `json:"x"`
	Y             int        `json:"y"`
	Rgb           color.RGBA `json:"rgb"`
	Ip            net.IP     `json:"ip"`
	PlacementTime time.Time  `json:"placementTime"`
}

func BatchUpsertTile(ctx context.Context, cdb *gocql.Session, args BatchUpsertArgs) error {
	trace := ctx.Value("trace")

	key := KeyFromPoint(args.X, args.Y)
	day := args.PlacementTime.Hour()

	batch := cdb.Batch(gocql.LoggedBatch).
		WithContext(ctx).
		Query("INSERT INTO pks.tiles (group_x, group_y, x, y, r, g, b, last_updated_ipaddress, last_updated_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
			key.X,
			key.Y,
			args.X,
			args.Y,
			args.Rgb.R,
			args.Rgb.G,
			args.Rgb.B,
			args.Ip,
			args.PlacementTime).
		Query("INSERT INTO pks.placements (hour, x, y, r, g, b, ipaddress, placement_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
			day,
			args.X,
			args.Y,
			args.Rgb.R,
			args.Rgb.G,
			args.Rgb.B,
			args.Ip,
			args.PlacementTime)

	if err := cdb.ExecuteBatch(batch); err != nil {
		log.Err(err).Msg("Error while executing BatchUpsertTile")
		return err
	}

	log.Info().
		Any("trace", trace).Any("args", args).
		Msg("Executed BatchUpsertTile")
	return nil
}
