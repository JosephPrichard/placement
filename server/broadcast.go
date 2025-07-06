package server

import (
	"github.com/go-redis/redis"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"
	"image/color"
	"placement/pb"
)

const DrawChannel = "draw-events"

func ListenPubSub(rdb redis.Client, drawChan chan Draw) {
	pubSub := rdb.Subscribe(DrawChannel)
	defer func() {
		if err := pubSub.Close(); err != nil {
			log.Err(err).Msg("Error while closing PubSub")
		}
	}()

	redisChan := pubSub.Channel()
	for m := range redisChan {
		log.Log().
			Str("channel", m.Channel).Str("message", m.String()).
			Msg("Received a message on channel")

		var d pb.Draw
		err := proto.Unmarshal([]byte(m.Payload), &d)
		if err != nil {
			log.Err(err).Msg("Error while deserializing a message from channel")
			continue
		}

		drawChan <- Draw{
			x:   d.X,
			y:   d.Y,
			rgb: color.RGBA{R: uint8(d.R), G: uint8(d.G), B: uint8(d.B), A: 255},
		}
	}
}

type Subscriber struct {
	id      int64
	subChan chan Draw
}

func HandleDrawEvent(drawChan chan Draw, subChan chan Subscriber) {
	var subscribers []Subscriber
	for {
		select {
		case draw := <-drawChan:
			for _, subscriber := range subscribers {
				subscriber.subChan <- draw
			}
			log.Info().
				Int("count", len(subscribers)).
				Msg("Broadcasted to all subscribers")
		case sub := <-subChan:
			subscribers = append(subscribers, sub)
			log.Info().
				Int64("id", sub.id).
				Msg("Appended a subscriber")
		}
	}
}

func BroadcastDraw(rdb *redis.Client, draw Draw) error {
	payload, err := proto.Marshal(&pb.Draw{
		X: draw.x,
		Y: draw.y,
		R: int32(draw.rgb.R),
		G: int32(draw.rgb.G),
		B: int32(draw.rgb.B),
	})
	if err != nil {
		log.Err(err).Msg("Failed to serialize Draw message")
		return err
	}

	err = rdb.Publish(DrawChannel, payload).Err()
	if err != nil {
		log.Err(err).Msg("Failed to publish Draw message")
		return err
	}
	return nil
}
