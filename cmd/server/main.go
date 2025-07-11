package main

import (
	"github.com/joho/godotenv"
	"github.com/rs/zerolog/log"
	"net/http"
	"os"
	"placement/app"
	"placement/app/clients"
	"placement/app/models"
	"placement/app/utils"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Panic().Msg("failed to load .env file")
	}

	contactPoints := os.Getenv("CASSANDRA_CONTACT_POINTS")
	redisURL := os.Getenv("REDIS_URL")
	port := os.Getenv("PORT")

	cdb := utils.CreateCassandra(contactPoints)
	defer cdb.Close()

	rdb := utils.CreateRedis(redisURL)
	defer func() {
		_ = rdb.Close()
	}()

	drawChan := make(chan models.Draw)
	subChan := make(chan app.Subscriber)
	unSubChan := make(chan string)

	go app.ListenBroadcast(rdb, drawChan)
	go app.MuxEventChannels(drawChan, subChan, unSubChan)

	state := app.State{
		Rdb:       rdb,
		Cdb:       cdb,
		SubChan:   subChan,
		UnsubChan: unSubChan,
		Recaptcha: &clients.RecaptchaClient{},
	}

	mux := app.HandleServer(state)

	log.Info().Str("port", port).Msg("starting the server")

	port = ":" + port
	if err := http.ListenAndServe(port, mux); err != nil {
		log.Panic().Err(err).Msg("failed to start the server")
	}
}
