package main

import (
	"github.com/joho/godotenv"
	"log/slog"
	"net/http"
	"os"
	"placement/app/clients"
	"placement/app/cql"
	"placement/app/dict"
	"placement/app/handlers"
	"placement/app/models"
)

func main() {
	if err := godotenv.Load(); err != nil {
		panic("failed to load .env file")
	}

	contactPoints := os.Getenv("CASSANDRA_CONTACT_POINTS")
	redisURL := os.Getenv("REDIS_URL")
	port := os.Getenv("PORT")

	cdb := cql.CreateCassandra(contactPoints)
	defer cdb.Close()

	rdb := dict.CreateRedis(redisURL)
	defer func() {
		_ = rdb.Close()
	}()

	drawChan := make(chan models.Draw)
	subChan := make(chan handlers.Subscriber)
	unSubChan := make(chan string)

	go handlers.ListenBroadcast(rdb, drawChan)
	go handlers.MuxEventChannels(drawChan, subChan, unSubChan)

	state := handlers.State{
		Rdb:       rdb,
		Cdb:       cdb,
		SubChan:   subChan,
		UnsubChan: unSubChan,
		Recaptcha: &clients.RecaptchaClient{},
	}

	mux := handlers.HandleServer(state)

	slog.Info("starting the server", "port", port)

	port = ":" + port
	if err := http.ListenAndServe(port, mux); err != nil {
		panic(err)
	}
}
