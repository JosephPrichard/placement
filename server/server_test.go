package server

import (
	"fmt"
	"github.com/go-redis/redis"
	"github.com/gocql/gocql"
	"github.com/gookit/goutil/testutil/assert"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const cassandraURI = "127.0.0.1:9042"
const redisURL = "redis://127.0.0.1:6380/"

func CreateTestServer(t *testing.T) (State, *httptest.Server, func()) {
	cluster := gocql.NewCluster(cassandraURI)
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = time.Second * 1
	cluster.Keyspace = "pks"

	cassandra, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("Failed to connect to cassandra: %v", err)
	}

	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Fatalf("Failed to connect to redis: %v", err)
	}
	rdb := redis.NewClient(opt)

	state := State{
		Cassandra: cassandra,
		Rdb:       rdb,
		SubChan:   make(chan Subscriber),
	}
	server := httptest.NewServer(HandleServer(state))

	closer := func() {
		server.Close()
		cassandra.Close()
		if err := rdb.Close(); err != nil {
			t.Fatalf("Failed to close redis client: %v", err)
		}
	}
	return state, server, closer
}

func TestGetTile(t *testing.T) {
	_, server, closer := CreateTestServer(t)
	defer closer()

	type Test struct {
		x, y      int
		expResp   string
		expStatus int
	}

	tests := []Test{
		{x: 0, y: 0, expResp: "", expStatus: http.StatusOK},
		{x: 10000, y: 10000, expResp: "", expStatus: http.StatusNotFound},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			resp, err := http.Get(fmt.Sprintf("%s/tile?X=%d&y=%d", server.URL, test.x, test.y))
			if err != nil {
				t.Fatalf("Failed to send http request: %v", err)
			}
			defer func() {
				if err := resp.Body.Close(); err != nil {
					t.Fatalf("Failed to close body reader: %v", err)
				}
			}()
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
	_, server, closer := CreateTestServer(t)
	defer closer()

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
			defer func() {
				if err := resp.Body.Close(); err != nil {
					t.Fatalf("Failed to close body reader: %v", err)
				}
			}()
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
	_, server, closer := CreateTestServer(t)
	defer closer()

	type Test struct {
		x, y      int
		expResp   []byte
		expStatus int
	}

	tests := []Test{
		{x: 0, y: 0, expResp: []byte{}, expStatus: http.StatusOK},
		{x: 22, y: 10, expResp: []byte{}, expStatus: http.StatusOK},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			resp, err := http.Get(fmt.Sprintf("%s/group?X=%d&y=%d", server.URL, test.x, test.y))
			if err != nil {
				t.Fatalf("Failed to send http request: %v", err)
			}
			defer func() {
				if err := resp.Body.Close(); err != nil {
					t.Fatalf("Failed to close body reader: %v", err)
				}
			}()
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
	_, server, closer := CreateTestServer(t)
	defer closer()

	type Test struct {
		days      int
		after     string
		expResp   string
		expStatus int
	}

	tests := []Test{
		{days: 0, expResp: "", expStatus: http.StatusOK},
		{days: 0, after: "", expResp: "", expStatus: http.StatusOK},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			resp, err := http.Get(fmt.Sprintf("%s/placements?days=%d&after=%s", server.URL, test.days, test.after))
			if err != nil {
				t.Fatalf("Failed to send http request: %v", err)
			}
			defer func() {
				if err := resp.Body.Close(); err != nil {
					t.Fatalf("Failed to close body reader: %v", err)
				}
			}()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("Failed to read response: %v", err)
			}

			assert.Equal(t, test.expResp, body)
			assert.Equal(t, test.expStatus, resp.StatusCode)
		})
	}
}
