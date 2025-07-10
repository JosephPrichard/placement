package app

import (
	"context"
	"github.com/go-redis/redis"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"
	"image/color"
	"placement/pb"
)

const DrawChannel = "draw-events"

func ListenBroadcast(rdb *redis.Client, drawChan chan Draw) {
	pubSub := rdb.Subscribe(DrawChannel)
	defer func() {
		if err := pubSub.Close(); err != nil {
			log.Err(err).Msg("error while closing PubSub")
		}
	}()

	redisChan := pubSub.Channel()
	for m := range redisChan {
		log.Log().
			Str("channel", m.Channel).Str("message", m.String()).
			Msg("received a message on channel")

		var e pb.Event
		err := proto.Unmarshal([]byte(m.Payload), &e)
		if err != nil {
			log.Err(err).Msg("error while deserializing a message from channel")
			continue
		}

		drawChan <- Draw{
			X:   int(e.Draw.X),
			Y:   int(e.Draw.Y),
			Rgb: color.RGBA{R: uint8(e.Draw.R), G: uint8(e.Draw.G), B: uint8(e.Draw.B), A: 255},
		}
	}
}

type Subscriber struct {
	id      string
	subChan chan Draw
}

func MuxEventChannels(drawChan chan Draw, subChan chan Subscriber, unsubChan chan string) {
	subscribers := make(map[string]Subscriber)
	for {
		select {
		case draw := <-drawChan:
			for _, subscriber := range subscribers {
				subscriber.subChan <- draw
			}
			log.Info().Int("count", len(subscribers)).Msg("broadcasted to all subscribers")
		case sub := <-subChan:
			subscribers[sub.id] = sub
			log.Info().Str("id", sub.id).Msg("appended a subscriber")
		case id := <-unsubChan:
			delete(subscribers, id)
			log.Info().Str("id", id).Msg("removed a subscriber")
		}
	}
}

func BroadcastDraw(ctx context.Context, rdb *redis.Client, draw Draw) error {
	trace := ctx.Value("trace")

	payload, err := proto.Marshal(&pb.Event{
		Draw: &pb.Draw{
			X: int32(draw.X),
			Y: int32(draw.Y),
			R: int32(draw.Rgb.R),
			G: int32(draw.Rgb.G),
			B: int32(draw.Rgb.B),
		},
	})
	if err != nil {
		log.Err(err).Any("trace", trace).Msg("failed to serialize Draw message")
		return err
	}

	err = rdb.Publish(DrawChannel, payload).Err()
	if err != nil {
		log.Err(err).Any("trace", trace).Msg("failed to publish Draw message")
		return err
	}

	log.Info().Any("trace", trace).Any("draw", draw).Msg("published a Draw message")
	return nil
}
