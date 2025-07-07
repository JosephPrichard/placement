package server

import (
	"context"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/gocql/gocql"
	"github.com/gookit/goutil/testutil/assert"
	"image/color"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const cassandraURI = "127.0.0.1:9042"
const redisURL = "redis://127.0.0.1:6380/"

type Databases struct {
	cdb *gocql.Session
	rdb *redis.Client
}

func createTestServer(t *testing.T) (Databases, *httptest.Server, func()) {
	cluster := gocql.NewCluster(cassandraURI)
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = time.Second * 1
	cluster.Keyspace = "pks"

	cdb, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("Failed to connect to cdb: %v", err)
	}

	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Fatalf("Failed to connect to redis: %v", err)
	}
	rdb := redis.NewClient(opt)

	state := State{
		Cdb:     cdb,
		Rdb:     rdb,
		SubChan: make(chan Subscriber),
	}
	server := httptest.NewServer(HandleServer(state))

	closer := func() {
		server.Close()
		cdb.Close()
		if err := rdb.Close(); err != nil {
			t.Fatalf("Failed to close redis client: %v", err)
		}
	}
	return Databases{cdb: cdb, rdb: rdb}, server, closer
}

func seedCassandra(t *testing.T, cdb *gocql.Session) {
	ctx := context.WithValue(context.Background(), "trace", "test-trace")

	truncateQueries := []*gocql.Query{
		cdb.Query("TRUNCATE pks.placements;"),
		cdb.Query("TRUNCATE pks.tiles;"),
	}
	for _, query := range truncateQueries {
		if err := query.Exec(); err != nil {
			t.Fatalf("Failed to perform truncate while seeding cassandra db: %v", err)
		}
	}

	upsertArgs := []BatchUpsertArgs{
		{0, 0, color.RGBA{R: 80, G: 120, B: 130, A: 255},
			net.IPv4(1, 2, 3, 4), time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC)},
		{2, 2, color.RGBA{R: 95, G: 45, B: 20, A: 255},
			net.IPv4(4, 3, 2, 1), time.Date(2025, time.January, 1, 15, 5, 0, 0, time.UTC)},
		{3, 4, color.RGBA{R: 90, G: 55, B: 50, A: 255},
			net.IPv4(1, 2, 3, 4), time.Date(2025, time.January, 1, 20, 15, 0, 0, time.UTC)},
		{GroupDim + 5, GroupDim + 2, color.RGBA{R: 95, G: 90, B: 45, A: 255},
			net.IPv4(1, 2, 3, 4), time.Date(2025, time.January, 2, 5, 5, 0, 0, time.UTC)},
	}
	for _, arg := range upsertArgs {
		if err := BatchUpsertTile(ctx, cdb, arg); err != nil {
			t.Fatalf("Failed to perform BatchUpsertTile while seeding cassandra db: %v", err)
		}
	}
}

func TestGetTile(t *testing.T) {
	db, server, closer := createTestServer(t)
	defer closer()

	seedCassandra(t, db.cdb)

	type Test struct {
		x, y      int
		expResp   string
		expStatus int
	}

	tests := []Test{
		{x: 0, y: 0, expResp: "", expStatus: http.StatusOK},
		{x: 2, y: 2, expResp: "", expStatus: http.StatusOK},
		{x: 10000, y: 10000, expResp: "", expStatus: http.StatusNotFound},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			resp, err := http.Get(fmt.Sprintf("%s/tile?x=%d&y=%d", server.URL, test.x, test.y))
			if err != nil {
				t.Fatalf("Failed to send http request: %v", err)
			}
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("Failed to read response: %v", err)
			}

			assert.Equal(t, test.expResp, string(body))
			assert.Equal(t, test.expStatus, resp.StatusCode)
		})
	}
}

func TestPostTile(t *testing.T) {
	db, server, closer := createTestServer(t)
	defer closer()

	seedCassandra(t, db.cdb)

	type Test struct {
		body      string
		expResp   string
		expStatus int
	}

	tests := []Test{
		{body: "", expResp: "", expStatus: http.StatusOK},
		{body: "", expResp: "", expStatus: http.StatusBadRequest},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			resp, err := http.Post(fmt.Sprintf("%s/tile", server.URL), "application/json", strings.NewReader(test.body))
			if err != nil {
				t.Fatalf("Failed to send http request: %v", err)
			}
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("Failed to read response: %v", err)
			}

			assert.Equal(t, test.expResp, string(body))
			assert.Equal(t, test.expStatus, resp.StatusCode)
		})
	}
}

func TestGetGroup(t *testing.T) {
	db, server, closer := createTestServer(t)
	defer closer()

	seedCassandra(t, db.cdb)

	type Test struct {
		x, y      int
		expResp   []byte
		expStatus int
	}

	tests := []Test{
		{x: 0, y: 0, expResp: []byte{}, expStatus: http.StatusOK},
		{x: 22, y: 10, expResp: []byte{}, expStatus: http.StatusOK},
		{x: 100_000, y: 100_000, expResp: []byte{}, expStatus: http.StatusOK},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			resp, err := http.Get(fmt.Sprintf("%s/group?x=%d&y=%d", server.URL, test.x, test.y))
			if err != nil {
				t.Fatalf("Failed to send http request: %v", err)
			}
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("Failed to read response: %v", err)
			}

			assert.Equal(t, test.expResp, body)
			assert.Equal(t, test.expStatus, resp.StatusCode)
		})
	}
}

func TestGetPlacements(t *testing.T) {
	db, server, closer := createTestServer(t)
	defer closer()

	seedCassandra(t, db.cdb)

	type Test struct {
		days      int
		after     string
		expResp   string
		expStatus int
	}

	tests := []Test{
		{days: 0, expResp: "[]\n", expStatus: http.StatusOK},
		{days: 0, after: "", expResp: "[]\n", expStatus: http.StatusOK},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			resp, err := http.Get(fmt.Sprintf("%s/placements?days=%d&after=%s", server.URL, test.days, test.after))
			if err != nil {
				t.Fatalf("Failed to send http request: %v", err)
			}
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("Failed to read response: %v", err)
			}

			assert.Equal(t, test.expResp, string(body))
			assert.Equal(t, test.expStatus, resp.StatusCode)
		})
	}
}
