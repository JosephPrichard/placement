package server

import (
	"github.com/go-redis/redis"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"
	"image/color"
	"placement/pb"
)

const DrawChannel = "draw-events"

func ListenBroadcast(rdb *redis.Client, eventChan chan Draw) {
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

		var e pb.Event
		err := proto.Unmarshal([]byte(m.Payload), &e)
		if err != nil {
			log.Err(err).Msg("Error while deserializing a message from channel")
			continue
		}

		eventChan <- Draw{
			x:   int(e.Draw.X),
			y:   int(e.Draw.Y),
			rgb: color.RGBA{R: uint8(e.Draw.R), G: uint8(e.Draw.G), B: uint8(e.Draw.B), A: 255},
		}
	}
}

type Subscriber struct {
	id      uuid.UUID
	subChan chan Draw
}

func MuxDrawChannels(eventChan chan Draw, subChan chan Subscriber) {
	var subscribers []Subscriber
	for {
		select {
		case draw := <-eventChan:
			for _, subscriber := range subscribers {
				subscriber.subChan <- draw
			}
			log.Info().
				Int("count", len(subscribers)).
				Msg("Broadcasted to all subscribers")
		case sub := <-subChan:
			subscribers = append(subscribers, sub)
			log.Info().
				Uint32("id", sub.id.ID()).
				Msg("Appended a subscriber")
		}
	}
}

func BroadcastDraw(rdb *redis.Client, draw Draw) error {
	payload, err := proto.Marshal(&pb.Event{
		Draw: &pb.Draw{
			X: int32(draw.x),
			Y: int32(draw.y),
			R: int32(draw.rgb.R),
			G: int32(draw.rgb.G),
			B: int32(draw.rgb.B),
		},
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
