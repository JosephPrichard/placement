package server

import (
	"encoding/json"
	"errors"
	"fmt"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/go-redis/redis"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const DrawPeriod = time.Minute

type State struct {
	Rdb       *redis.Client
	Cassandra *gocql.Session
	SubChan   chan Subscriber
}

type ServiceError struct {
	msg  string
	code int
}

func WriteError(w http.ResponseWriter, msg string, code int) {
	log.Error().
		Str("error", msg).Int("code", code).
		Msg("Writing error response")

	errResp := ServiceError{
		msg:  msg,
		code: code,
	}

	if err := json.NewEncoder(w).Encode(errResp); err != nil {
		log.Panic().Msg(err.Error())
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusInternalServerError)
}

func WriteFatalError(w http.ResponseWriter) {
	WriteError(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
}

type Point struct {
	x int
	y int
}

func parsePoint(url *url.URL) (Point, error) {
	query := url.Query()
	xStr := query.Get("x")
	yStr := query.Get("y")

	x, err := strconv.Atoi(xStr)
	if err != nil {
		return Point{}, fmt.Errorf("x must be an integer, got %s", xStr)
	}
	y, err := strconv.Atoi(yStr)
	if err != nil {
		return Point{}, fmt.Errorf("y must be an integer, got %s", yStr)
	}

	return Point{x: x, y: y}, nil
}

func HandleGetTile(state State, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	point, err := parsePoint(r.URL)
	if err != nil {
		WriteError(w, err.Error(), http.StatusBadRequest)
		return
	}

	tile, err := GetOneTile(ctx, state.Cassandra, point.x, point.y)
	if errors.Is(err, TileNotFoundError) {
		WriteError(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		WriteFatalError(w)
		return
	}

	if err = json.NewEncoder(w).Encode(tile); err != nil {
		WriteFatalError(w)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
}

func HandleGetGroup(state State, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	point, err := parsePoint(r.URL)
	if err != nil {
		WriteError(w, err.Error(), http.StatusBadRequest)
		return
	}

	key := GroupKey{x: point.x, y: point.y}
	var tg TileGroup

	tg, err = GetCachedGroup(ctx, state.Rdb, key)
	if err != nil {
		WriteFatalError(w)
		return
	}
	if tg == nil {
		tg, err = GetTileGroup(ctx, state.Cassandra, key)
		if err != nil {
			WriteFatalError(w)
			return
		}

		go func() {
			if err = SetCachedGroup(ctx, state.Rdb, key, tg); err != nil {
				log.Err(err).Msg("failed SetCachedGroup in background task")
			}
		}()
	}

	if _, err = w.Write(tg); err != nil {
		WriteFatalError(w)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
}

func HandleGetPlacements(state State, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	query := r.URL.Query()
	daysAgoStr := query.Get("daysAgo")
	afterStr := query.Get("after")

	var daysAgo int
	var after time.Time

	var err error
	if daysAgoStr != "" {
		daysAgo, err = strconv.Atoi(daysAgoStr)
		if err != nil {
			WriteError(w, fmt.Sprintf("daysAgo must be an integer, got %s", daysAgoStr), http.StatusBadRequest)
			return
		}
	}
	if afterStr == "" {
		after = time.Now().Add(DrawPeriod)
	} else {
		after, err = time.Parse(time.RFC3339, afterStr)
		if err != nil {
			WriteError(w, fmt.Sprintf("after must be a valid RFC3339 timestamp, got %s", daysAgoStr), http.StatusBadRequest)
			return
		}
	}

	var tiles []Tile
	if daysAgo <= 0 || after.IsZero() {
		tiles = make([]Tile, 0)
	} else {
		tiles, err = GetTiles(ctx, state.Cassandra, int64(daysAgo), after)
		if err != nil {
			WriteFatalError(w)
			return
		}
	}

	if err = json.NewEncoder(w).Encode(tiles); err != nil {
		WriteFatalError(w)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
}

func getIpAddr(r *http.Request) net.IP {
	xff := r.Header.Get("X-Forwarded-For")
	if xff != "" {
		// X-Forwarded-For may contain multiple IPs: client, proxy1, proxy2, ...
		parts := strings.Split(xff, ",")
		return net.ParseIP(strings.TrimSpace(parts[0]))
	}

	// Fallback to X-Real-IP
	if xrip := r.Header.Get("X-Real-IP"); xrip != "" {
		return net.ParseIP(xrip)
	}

	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return net.ParseIP(r.RemoteAddr)
	}
	return net.ParseIP(ip)
}

func HandlePostTile(state State, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var draw Draw
	if err := json.NewDecoder(r.Body).Decode(&draw); err != nil {
		WriteFatalError(w)
		return
	}

	now := time.Now()
	ip := getIpAddr(r)
	if ip == nil {
		WriteError(w, fmt.Sprintf("ip must be a valid IP address"), http.StatusBadRequest)
		return
	}

	ret, err := AcquireExpiringLock(ctx, state.Rdb, ip.String(), now, now.Add(-DrawPeriod), DrawPeriod)
	if err != nil {
		WriteFatalError(w)
		return
	}

	if ret >= 0 {
		timePlaced := time.Unix(ret, 0)
		difference := now.Sub(timePlaced)
		remaining := DrawPeriod - difference

		WriteError(w, fmt.Sprintf("%d minutes remaining until player can draw another tile", int64(remaining.Minutes())), http.StatusUnauthorized)
		return
	}

	err = UpsertCachedGroup(ctx, state.Rdb, draw)
	if err != nil {
		WriteFatalError(w)
		return
	}
	go func() {
		if err := BatchUpsertTile(ctx, state.Cassandra, draw.x, draw.y, draw.rgb, ip, now); err != nil {
			log.Err(err).Msg("BatchUpsertTile failed in background task")
		}
	}()
	go func() {
		if err := BroadcastDraw(state.Rdb, draw); err != nil {
			log.Err(err).Msg("BroadcastDraw failed in background task")
		}
	}()

	w.WriteHeader(http.StatusOK)
}

func HandleDrawEvents(state State, w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	ctx := r.Context()
	trace := ctx.Value("trace")
	sseId := uuid.New()

	subChan := make(chan Draw)
	state.SubChan <- Subscriber{id: sseId, subChan: subChan}

	for {
		select {
		case <-ctx.Done():
			log.Info().Type("trace", trace).Msg("Client disconnected from sse")
			return
		case draw := <-subChan:
			if err := json.NewEncoder(w).Encode(draw); err != nil {
				WriteFatalError(w)
				return
			}
			flusher.Flush()
		}
	}
}

func HandleServer(state State) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Info().
			Str("method", r.Method).Str("url", r.URL.String()).
			Msg("Received an http request")

		switch r.Method {
		case "GET":
			switch r.URL.Path {
			case "/placements":
				HandleGetPlacements(state, w, r)
			case "/tile":
				HandleGetTile(state, w, r)
			case "/group":
				HandleGetGroup(state, w, r)
			case "/draw/events":
				HandleDrawEvents(state, w, r)
			}
		case "POST":
			switch r.URL.Path {
			case "/tile":
				HandlePostTile(state, w, r)
			}
		}
	})
}
