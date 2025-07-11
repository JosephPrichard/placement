package utils

import (
	"github.com/go-redis/redis"
	"github.com/gocql/gocql"
	"github.com/rs/zerolog/log"
	"strings"
	"time"
)

const TestCassandraURI = "127.0.0.1:9042"
const TestRedisURL = "redis://127.0.0.1:6380/"

func CreateCassandra(contactPoints string) *gocql.Session {
	hosts := strings.Split(contactPoints, ",")
	cluster := gocql.NewCluster(hosts...)
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = time.Second * 5

	cassandra, err := cluster.CreateSession()
	if err != nil {
		log.Panic().Str("contactPoints", contactPoints).Err(err).Msg("failed to connect to cassandra")
	}

	return cassandra
}

func CreateRedis(redisURL string) *redis.Client {
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Panic().Str("redisURL", redisURL).Err(err).Msg("failed to connect to redis")
	}
	return redis.NewClient(opt)
}
