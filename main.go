package main

import (
	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/go-redis/redis"
	"github.com/rs/zerolog/log"
	"net/http"
	"os"
	"placement/server"
	"strings"
)

func main() {
	cassandraURI := os.Getenv("CASSANDRA_URI")
	redisURL := os.Getenv("REDIS_URL")
	port := os.Getenv("PORT")

	hosts := strings.Split(cassandraURI, ",")
	cluster := gocql.NewCluster(hosts...)
	cluster.Consistency = gocql.Quorum

	cassandra, err := cluster.CreateSession()
	if err != nil {
		log.Panic().Err(err).Msg("Failed to connect to Cassandra")
	}
	defer cassandra.Close()

	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Panic().Err(err).Msg("Failed to connect to Redis")
	}
	rdb := redis.NewClient(opt)
	defer func() {
		if err := rdb.Close(); err != nil {
			log.Err(err).Msg("Failed to close to Redis client")
		}
	}()

	drawChan := make(chan server.Draw)
	subChan := make(chan server.Subscriber)

	go server.ListenBroadcast(rdb, drawChan)
	go server.MuxDrawChannels(drawChan, subChan)

	state := server.State{
		Rdb:       rdb,
		Cassandra: cassandra,
		SubChan:   subChan,
	}

	mux := server.HandleServer(state)

	port = ":" + port
	log.Info().Str("port", port).Msg("Starting the server")
	if err := http.ListenAndServe(port, mux); err != nil {
		log.Panic().Err(err).Msg("Failed to start the server")
	}
}
