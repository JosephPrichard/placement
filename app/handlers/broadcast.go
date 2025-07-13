package handlers

import (
	"context"
	"github.com/go-redis/redis"
	"google.golang.org/protobuf/proto"
	"image/color"
	"log/slog"
	"placement/app/models"
	"placement/pb"
)

const DrawChannel = "draw-events"

func ListenBroadcast(rdb *redis.Client, drawChan chan models.Draw) {
	pubSub := rdb.Subscribe(DrawChannel)
	defer func() {
		if err := pubSub.Close(); err != nil {
			slog.Error("error while closing PubSub", "err", err)
		}
	}()

	redisChan := pubSub.Channel()
	slog.Info("begin listening on channel", "channel", DrawChannel)

	for m := range redisChan {
		slog.Info("received a message on channel", "channel", m.Channel, "message", m.String())

		switch m.Channel {
		case DrawChannel:
			var d pb.Draw
			err := proto.Unmarshal([]byte(m.Payload), &d)
			if err != nil {
				slog.Error("error while deserializing a message from channel", "err", err)
				continue
			}

			drawChan <- models.Draw{
				X:   int(d.X),
				Y:   int(d.Y),
				Rgb: color.RGBA{R: uint8(d.R), G: uint8(d.G), B: uint8(d.B), A: 255},
			}
		}
	}
}

type Subscriber struct {
	id      string
	subChan chan models.Draw
}

func MuxEventChannels(drawChan chan models.Draw, subChan chan Subscriber, unsubChan chan string) {
	subscribers := make(map[string]Subscriber)
	for {
		select {
		case draw := <-drawChan:
			for _, subscriber := range subscribers {
				subscriber.subChan <- draw
			}
			slog.Info("broadcasted to all subscribers", "count", len(subscribers))
		case sub := <-subChan:
			subscribers[sub.id] = sub
			slog.Info("appended a subscriber", "id", sub.id)
		case id := <-unsubChan:
			delete(subscribers, id)
			slog.Info("removed a subscriber", "id", id)
		}
	}
}

func BroadcastDraw(ctx context.Context, rdb *redis.Client, draw models.Draw) error {
	trace := ctx.Value("trace")

	payload, err := proto.Marshal(&pb.Draw{
		X: int32(draw.X),
		Y: int32(draw.Y),
		R: int32(draw.Rgb.R),
		G: int32(draw.Rgb.G),
		B: int32(draw.Rgb.B),
	})
	if err != nil {
		slog.Error("failed to serialize Draw message", "err", err, "trace", trace)
		return err
	}

	err = rdb.Publish(DrawChannel, payload).Err()
	if err != nil {
		slog.Error("failed to publish Draw message", "err", err, "trace", trace)
		return err
	}

	slog.Info("published a Draw message", "trace", trace, "draw", draw)
	return nil
}
