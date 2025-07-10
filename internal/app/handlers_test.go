package app

import (
	"bufio"
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/gocql/gocql"
	"github.com/gookit/goutil/testutil/assert"
	"image/color"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

const cassandraURI = "127.0.0.1:9042"
const redisURL = "redis://127.0.0.1:6380/"

//go:embed cql/teardownSchema.cql
var truncateQuery string

type Databases struct {
	cdb *gocql.Session
	rdb *redis.Client
}

func init() {
	GroupDim = 5
	GroupLen = GroupDim * GroupDim * 3
}

func createTestServer(t *testing.T, recaptcha RecaptchaApi) (Databases, *httptest.Server, State, func()) {
	cluster := gocql.NewCluster(cassandraURI)
	cluster.Consistency = gocql.All
	cluster.Timeout = time.Second * 1
	cluster.Keyspace = "pks"

	cdb, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("failed to connect to cdb: %v", err)
	}

	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Fatalf("failed to connect to redis: %v", err)
	}
	rdb := redis.NewClient(opt)

	state := State{
		Cdb:       cdb,
		Rdb:       rdb,
		SubChan:   make(chan Subscriber),
		UnsubChan: make(chan string),
		Recaptcha: recaptcha,
	}
	server := httptest.NewServer(HandleServer(state))

	closer := func() {
		server.Close()
		cdb.Close()
		if err := rdb.Close(); err != nil {
			t.Fatalf("failed to close redis client: %v", err)
		}
	}
	return Databases{cdb: cdb, rdb: rdb}, server, state, closer
}

func seedDatabases(t *testing.T, db Databases) {
	teardownDatabases(t, db)

	upsertArgs := []BatchUpsertArgs{
		{
			X:             0,
			Y:             0,
			Rgb:           color.RGBA{R: 80, G: 120, B: 130, A: 255},
			Ip:            net.IPv4(1, 2, 3, 4),
			PlacementTime: time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			X:             2,
			Y:             2,
			Rgb:           color.RGBA{R: 95, G: 45, B: 20, A: 255},
			Ip:            net.IPv4(4, 3, 2, 1),
			PlacementTime: time.Date(2025, time.January, 1, 15, 5, 0, 0, time.UTC),
		},
		{
			X:             3,
			Y:             4,
			Rgb:           color.RGBA{R: 90, G: 55, B: 50, A: 255},
			Ip:            net.IPv4(1, 2, 3, 4),
			PlacementTime: time.Date(2025, time.January, 1, 20, 15, 0, 0, time.UTC),
		},
		{
			X:             GroupDim + 5,
			Y:             GroupDim + 2,
			Rgb:           color.RGBA{R: 95, G: 90, B: 45, A: 255},
			Ip:            net.IPv4(1, 2, 3, 4),
			PlacementTime: time.Date(2025, time.January, 2, 5, 5, 0, 0, time.UTC),
		},
	}

	ctx := context.WithValue(context.Background(), "trace", "seed-database-trace")
	for _, arg := range upsertArgs {
		d := Draw{X: arg.X, Y: arg.Y, Rgb: color.RGBA{R: arg.Rgb.R, G: arg.Rgb.G, B: arg.Rgb.B}}
		if err := UpsertCachedGroup(ctx, db.rdb, d); err != nil {
			t.Fatalf("failed to perform UpsertCachedGroup while seeding redis db: %v", err)
		}
	}
	if err := BatchUpsertTile(ctx, db.cdb, upsertArgs); err != nil {
		t.Fatalf("failed to perform BatchUpsertTile while seeding cassandra db: %v", err)
	}
}

func teardownDatabases(t *testing.T, db Databases) {
	if err := db.rdb.FlushAll().Err(); err != nil {
		t.Fatalf("failed to flush redis db: %v", err)
	}
	for _, stmt := range strings.Split(truncateQuery, "\n") {
		if err := db.cdb.Query(stmt).Exec(); err != nil {
			t.Fatalf("failed to perform truncate while seeding cassandra db: %v", err)
		}
	}
}

func createExpGroup() []byte {
	// contains the expected bytes for the group 0,0 that is initialized in seedDatabases
	b := make([]byte, GroupLen)
	b[0] = 80
	b[1] = 120
	b[2] = 130
	o1 := GetTgOffset(2, 2)
	b[o1] = 95
	b[o1+1] = 45
	b[o1+2] = 20
	o2 := GetTgOffset(3, 4)
	b[o2] = 90
	b[o2+1] = 55
	b[o2+2] = 50
	return b
}

func TestGetTile(t *testing.T) {
	db, server, _, closer := createTestServer(t, nil)
	defer closer()

	seedDatabases(t, db)
	defer teardownDatabases(t, db)

	type Test struct {
		x, y      int
		expResp   string
		expStatus int
	}

	tests := []Test{
		{x: 0, y: 0, expResp: `{"d":{"x":0,"y":0,"rgb":{"R":80,"G":120,"B":130,"A":0}},"date":"2025-01-01 00:00:00 +0000 UTC"}`, expStatus: http.StatusOK},
		{x: 2, y: 2, expResp: `{"d":{"x":2,"y":2,"rgb":{"R":95,"G":45,"B":20,"A":0}},"date":"2025-01-01 15:05:00 +0000 UTC"}`, expStatus: http.StatusOK},
		{x: 10000, y: 10000, expResp: `{"msg":"tile not found","code":404}`, expStatus: http.StatusNotFound},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			resp, err := http.Get(fmt.Sprintf("%s/tile?x=%d&y=%d", server.URL, test.x, test.y))
			if err != nil {
				t.Fatalf("failed to send http request: %v", err)
			}
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("failed to read response: %v", err)
			}
			bodyStr := string(body)

			assert.Equal(t, test.expResp+"\n", bodyStr)
			assert.Equal(t, test.expStatus, resp.StatusCode)
		})
	}
}

type RecaptchaMock struct{}

func (m *RecaptchaMock) Verify(_ context.Context, token string, _ string) error {
	switch token {
	case "test-token":
		return nil
	case "invalid-token":
		return InvalidCaptchaErr
	case "external-api-failure":
		return errors.New("stub: failed to hit recaptcha")
	}
	panic("no mock RecaptchaMock.Verify implementation matched for arguments")
}

func TestPostTile(t *testing.T) {
	db, server, _, closer := createTestServer(t, &RecaptchaMock{})
	defer closer()

	seedDatabases(t, db)
	defer teardownDatabases(t, db)

	type Test struct {
		body      PostTileBody
		token     string
		expResp   string
		expStatus int
		expDraw   *TileGroup
	}

	eg := TileGroup(createExpGroup())
	o1 := GetTgOffset(0, 1)
	eg[o1] = 50
	eg[o1+1] = 4
	eg[o1+2] = 90

	tests := []Test{
		{body: PostTileBody{X: 0, Y: 1, Rgb: []byte{50, 4, 90}}, token: "test-token", expStatus: http.StatusOK, expDraw: &eg},
		{body: PostTileBody{X: 0, Y: 0, Rgb: []byte{0, 0, 0}}, token: "test-token", expStatus: http.StatusUnauthorized},
		{body: PostTileBody{X: 0, Y: 0, Rgb: []byte{0, 0, 0}}, token: "invalid-token", expStatus: http.StatusUnauthorized},
		{body: PostTileBody{X: 0, Y: 0, Rgb: []byte{0, 0, 0}}, token: "external-api-failure", expStatus: http.StatusInternalServerError},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			b, _ := json.Marshal(test.body)

			req, err := http.NewRequest("POST", fmt.Sprintf("%s/tile", server.URL), bytes.NewReader(b))
			if err != nil {
				t.Fatalf("failed to construct http request: %v", err)
			}
			req.Header.Set(RecaptchaTokenHeader, test.token)

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("failed to send http request: %v", err)
			}
			defer resp.Body.Close()

			assert.Equal(t, test.expStatus, resp.StatusCode)

			assertCtx := context.WithValue(context.Background(), "trace", "assertion-trace")
			if test.expDraw != nil {
				key := KeyFromPoint(test.body.X, test.body.Y)
				groupCdb, err := GetTileGroup(assertCtx, db.cdb, key)
				if err != nil {
					t.Fatalf("failed to get group from db: %v", err)
				}
				groupRdb, err := GetCachedGroup(assertCtx, db.rdb, key)
				if err != nil {
					t.Fatalf("failed to get group from cache: %v", err)
				}
				assert.Equal(t, *test.expDraw, groupCdb)
				assert.Equal(t, *test.expDraw, groupRdb)
			}
		})
	}
}

func TestGetGroup(t *testing.T) {
	db, server, _, closer := createTestServer(t, nil)
	defer closer()

	seedDatabases(t, db)
	defer teardownDatabases(t, db)

	type Test struct {
		x, y      int
		expResp   []byte
		expStatus int
	}

	expResp := createExpGroup()
	tests := []Test{
		{x: 0, y: 0, expResp: expResp, expStatus: http.StatusOK},
		{x: 4, y: 3, expResp: expResp, expStatus: http.StatusOK},
		{x: 100, y: 100, expResp: []byte{}, expStatus: http.StatusOK},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			resp, err := http.Get(fmt.Sprintf("%s/group?x=%d&y=%d", server.URL, test.x, test.y))
			if err != nil {
				t.Fatalf("failed to send http request: %v", err)
			}
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("failed to read response: %v", err)
			}

			assert.Equal(t, test.expResp, body)
			assert.Equal(t, test.expStatus, resp.StatusCode)
		})
	}
}

func TestGetPlacements(t *testing.T) {
	db, server, _, closer := createTestServer(t, nil)
	defer closer()

	seedDatabases(t, db)
	defer teardownDatabases(t, db)

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
				t.Fatalf("failed to send http request: %v", err)
			}
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("failed to read response: %v", err)
			}

			assert.Equal(t, test.expResp, string(body))
			assert.Equal(t, test.expStatus, resp.StatusCode)
		})
	}
}

func TestDrawEvents(t *testing.T) {
	db, server, state, closer := createTestServer(t, nil)
	defer closer()

	drawChan := make(chan Draw)

	go ListenBroadcast(db.rdb, drawChan)
	go MuxEventChannels(drawChan, state.SubChan, state.UnsubChan)

	var events []string
	count := 5

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()

		resp, err := http.Get(fmt.Sprintf("%s/draw/events", server.URL))
		if err != nil {
			log.Fatalf("failed to send http request: %v", err)
		}
		defer resp.Body.Close()
		log.Printf("sent draw events request")

		reader := bufio.NewReader(resp.Body)
		for i := 0; i < count; i++ {
			line, _ := reader.ReadSlice('\n')
			if err != nil {
				log.Fatalf("failed to read slice from body: %v", err)
			}
			events = append(events, string(line))

		}
		log.Printf("finished retrieving draw events")
	}()

	ctx := context.WithValue(context.Background(), "trace", "test-trace")
	go func() {
		defer wg.Done()
		time.Sleep(time.Second * 1)

		for i := 0; i < count; i++ {
			if err := BroadcastDraw(ctx, db.rdb, Draw{X: 1, Y: 2}); err != nil {
				log.Fatalf("failed to broadcast draw: %v", err)
			}
		}
		log.Printf("done sending draw events")
	}()

	t.Logf("waiting for draw events routine")

	wg.Wait()

	expEvents := slices.Repeat([]string{`{"x":1,"y":2,"rgb":{"R":0,"G":0,"B":0,"A":255}}` + "\n"}, count)
	assert.Equal(t, expEvents, events)
}
