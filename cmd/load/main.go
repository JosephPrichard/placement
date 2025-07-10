package main

import (
	"context"
	"flag"
	"github.com/rs/zerolog/log"
	"image/color"
	"image/png"
	"net"
	"os"
	"placement/internal/app"
	"placement/internal/utils"
	"time"
)

var image = flag.String("image", "", "The image to load into the database")
var xOff = flag.Int("xOff", 0, "The x offset to draw the image")
var yOff = flag.Int("yOff", 0, "The y offset to draw the image")

func main() {
	contactPoints := os.Getenv("CASSANDRA_CONTACT_POINTS")
	redisURL := os.Getenv("REDIS_URL")

	f, err := os.Open(*image)
	if err != nil {
		log.Panic().Err(err).Msg("failed to open the image file")
	}
	img, err := png.Decode(f)
	if err != nil {
		log.Panic().Err(err).Msg("failed to decode image")
	}

	cdb := utils.CreateCassandra(contactPoints)
	defer cdb.Close()

	rdb, closer := utils.CreateRedis(redisURL)
	defer closer()

	var argsArr []app.BatchUpsertArgs

	bounds := img.Bounds()
	for x := bounds.Min.X; x < bounds.Max.X; x++ {
		for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
			rgb := img.At(x, y)
			r, g, b, a := rgb.RGBA()
			argsArr = append(argsArr, app.BatchUpsertArgs{
				X:             x + *xOff,
				Y:             y + *yOff,
				Rgb:           color.RGBA{R: uint8(r), G: uint8(g), B: uint8(b), A: uint8(a)},
				Ip:            net.IPv4(127, 0, 0, 1),
				PlacementTime: time.Now(),
			})
		}
	}

	ctx := context.WithValue(context.Background(), "trace", "load-image-trace")
	for _, arg := range argsArr {
		if arg.Rgb.A == 255 {
			d := app.Draw{X: arg.X, Y: arg.Y, Rgb: arg.Rgb}
			if err := app.UpsertCachedGroup(ctx, rdb, d); err != nil {
				log.Panic().Err(err).Msg("failed exec UpsertCachedGroup")
			}
		}
	}
	if err := app.BatchUpsertTile(ctx, cdb, argsArr); err != nil {
		log.Panic().Err(err).Msg("failed exec BatchUpsertTile")
	}
}
