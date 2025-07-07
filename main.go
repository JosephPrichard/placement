package main

import (
	"flag"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/gocql/gocql"
	"github.com/rs/zerolog/log"
	"net/http"
	"os"
	"placement/server"
	"strings"
	"time"
)

const (
	RunMode = "run"
)

var mode = flag.String("mode", "server", fmt.Sprintf("The mode to execute the application: %s", RunMode))

func createCassandra(contactPoints string) *gocql.Session {
	hosts := strings.Split(contactPoints, ",")
	cluster := gocql.NewCluster(hosts...)
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = time.Second * 5

	cassandra, err := cluster.CreateSession()
	if err != nil {
		log.Panic().Err(err).Msg("Failed to connect to Cassandra")
	}

	return cassandra
}

func createRedis(redisURL string) (*redis.Client, func()) {
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Panic().Err(err).Msg("Failed to connect to Redis")
	}
	rdb := redis.NewClient(opt)
	closer := func() {
		if err := rdb.Close(); err != nil {
			log.Err(err).Msg("Failed to close to Redis client")
		}
	}
	return rdb, closer
}

func main() {
	if mode == nil || *mode == "" || *mode == RunMode {
		contactPoints := os.Getenv("CASSANDRA_CONTACT_POINTS")
		redisURL := os.Getenv("REDIS_URL")
		port := os.Getenv("PORT")

		cassandra := createCassandra(contactPoints)
		defer cassandra.Close()

		rdb, closer := createRedis(redisURL)
		defer closer()

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
	} else {
		log.Panic().Str("mode", *mode).Msg(fmt.Sprintf("The mode is not supported, expected: %s", RunMode))
	}
}
