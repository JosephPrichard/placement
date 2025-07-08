package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/rs/zerolog/log"
	"image/color"
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
	Cdb       *gocql.Session
	SubChan   chan Subscriber
	UnsubChan chan string
}

type Point struct {
	X int `json:"x"`
	Y int `json:"y"`
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

	return Point{X: x, Y: y}, nil
}

func HandleGetTile(state State, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	point, err := parsePoint(r.URL)
	if err != nil {
		ErrorCode(w, r, err.Error(), http.StatusBadRequest)
		return
	}

	log.Info().
		Any("trace", ctx.Value("trace")).Any("point", point).
		Msg("Handling GetTile")

	tile, err := GetOneTile(ctx, state.Cdb, point.X, point.Y)
	if errors.Is(err, TileNotFoundError) {
		Error(w, r, err)
		return
	}
	if err != nil {
		Error(w, r, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	_ = json.NewEncoder(w).Encode(tile)
}

func HandleGetGroup(state State, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	point, err := parsePoint(r.URL)
	if err != nil {
		Error(w, r, err)
		return
	}
	key := KeyFromPoint(point.X, point.Y)

	log.Info().
		Any("trace", ctx.Value("trace")).Any("point", point).Any("key", key).
		Msg("Handling GetGroup")

	var tg TileGroup
	if tg, err = GetCachedGroup(ctx, state.Rdb, key); err != nil {
		Error(w, r, err)
		return
	}
	if tg == nil {
		log.Info().
			Any("trace", ctx.Value("trace")).Any("point", point).Any("key", key).
			Msg("Executing GetTileGroup to retrieve group, group was not cached")

		if tg, err = GetTileGroup(ctx, state.Cdb, key); err != nil {
			Error(w, r, err)
			return
		}
		go func() {
			if err = SetCachedGroup(ctx, state.Rdb, key, tg); err != nil {
				log.Err(err).Msg("failed SetCachedGroup in background task")
			}
		}()
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)

	_, _ = w.Write(tg)
}

func HandleGetPlacements(state State, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	query := r.URL.Query()
	daysStr := query.Get("days")
	afterStr := query.Get("after")

	log.Info().
		Any("trace", ctx.Value("trace")).Str("days", daysStr).Str("after", afterStr).
		Msg("Handling GetPlacements")

	var days int
	var after time.Time
	var err error

	if daysStr != "" {
		if days, err = strconv.Atoi(daysStr); err != nil {
			ErrorCode(w, r, fmt.Sprintf("days must be an integer, got %s", daysStr), http.StatusBadRequest)
			return
		}
	}
	if afterStr != "" {
		if after, err = time.Parse(time.RFC3339, afterStr); err != nil {
			ErrorCode(w, r, fmt.Sprintf("after must be a valid RFC3339 timestamp, got %s", daysStr), http.StatusBadRequest)
			return
		}
	} else {
		after = time.Now()
	}

	var tiles []Tile
	if days <= 0 || after.IsZero() {
		tiles = make([]Tile, 0)
	} else {
		if tiles, err = GetTiles(ctx, state.Cdb, int64(days), after); err != nil {
			Error(w, r, err)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	_ = json.NewEncoder(w).Encode(tiles)
}

func getIpAddr(r *http.Request) net.IP {
	xff := r.Header.Get("X-Forwarded-For")
	if xff != "" {
		// X-Forwarded-For may contain multiple IPs: client, proxy1, proxy2, ...
		parts := strings.Split(xff, ",")
		return net.ParseIP(strings.TrimSpace(parts[0]))
	}

	if xrip := r.Header.Get("X-Real-IP"); xrip != "" {
		return net.ParseIP(xrip)
	}

	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return net.ParseIP(r.RemoteAddr)
	}
	return net.ParseIP(ip)
}

type PostTileBody struct {
	X   int    `json:"x"`
	Y   int    `json:"y"`
	Rgb []byte `json:"rgb"`
}

func HandlePostTile(state State, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var body PostTileBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		ErrorCode(w, r, "Failed to deserialize response, expected PostTileBody", http.StatusBadRequest)
		return
	}

	if len(body.Rgb) != 3 {
		ErrorCode(w, r, fmt.Sprintf("Rgb color tuple must be of length 3, was %d", len(body.Rgb)), http.StatusBadRequest)
		return
	}
	draw := Draw{
		X:   body.X,
		Y:   body.Y,
		Rgb: color.RGBA{R: body.Rgb[0], G: body.Rgb[1], B: body.Rgb[2], A: 255},
	}

	log.Info().Any("trace", ctx.Value("trace")).Any("draw", draw).Msg("Handling PostTile")

	ip := getIpAddr(r)
	if ip == nil {
		ErrorCode(w, r, fmt.Sprintf("ip must be a valid IP address"), http.StatusBadRequest)
		return
	}

	now := time.Now()
	ret, err := AcquireExpiringLock(ctx, state.Rdb, ip.String(), now, now.Add(-DrawPeriod), DrawPeriod)
	if err != nil {
		Error(w, r, err)
		return
	}

	if ret >= 0 {
		timePlaced := time.Unix(ret, 0)
		difference := now.Sub(timePlaced)
		remaining := DrawPeriod - difference
		if remaining < 0 || timePlaced.After(now) {
			ErrorCode(w, r, fmt.Sprintf("invariant is false: remaining=%d must be larger than 0 and timePlaced=%v must be before now=%v", remaining, timePlaced, now), http.StatusInternalServerError)
			return
		}
		ErrorCode(w, r, fmt.Sprintf("%d minutes remaining until player can draw another tile", int64(remaining.Minutes())), http.StatusUnauthorized)
		return
	}

	if err = UpsertCachedGroup(ctx, state.Rdb, draw); err != nil {
		Error(w, r, err)
		return
	}
	go func() {
		ctx := context.WithValue(context.Background(), "trace", ctx.Value("trace"))
		if err := BatchUpsertTile(ctx, state.Cdb, BatchUpsertArgs{draw.X, draw.Y, draw.Rgb, ip, now}); err != nil {
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

	trace := r.Header.Get("trace")
	sseId := uuid.NewString()

	subChan := make(chan Draw)
	state.SubChan <- Subscriber{id: sseId, subChan: subChan}
	defer func() {
		state.UnsubChan <- sseId
		close(subChan)
	}()

	for {
		select {
		case <-ctx.Done():
			log.Info().Any("trace", trace).Str("sseId", sseId).Msg("Client disconnected from sse")
			return
		case draw := <-subChan:
			log.Info().Any("trace", trace).Str("sseId", sseId).Any("draw", draw).Msg("Client retrieved draw msg")
			_ = json.NewEncoder(w).Encode(draw)
			flusher.Flush()
		}
	}
}

func SideChannelMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		trace := r.Header.Get("trace")
		if trace == "" {
			trace = uuid.NewString()
		}
		ctx := context.WithValue(r.Context(), "trace", trace)
		r = r.WithContext(ctx)

		log.Info().
			Any("trace", r.Context().Value("trace")).Str("method", r.Method).Str("url", r.URL.String()).
			Msg("Handling an http request")

		next.ServeHTTP(w, r)
	})
}

func HandleServer(state State) http.Handler {
	r := mux.NewRouter()

	r.Use(SideChannelMiddleware)

	get := r.Methods(http.MethodGet).Subrouter()
	get.HandleFunc("/placements", func(w http.ResponseWriter, r *http.Request) {
		HandleGetPlacements(state, w, r)
	})
	get.HandleFunc("/tile", func(w http.ResponseWriter, r *http.Request) {
		HandleGetTile(state, w, r)
	})
	get.HandleFunc("/group", func(w http.ResponseWriter, r *http.Request) {
		HandleGetGroup(state, w, r)
	})
	get.HandleFunc("/draw/events", func(w http.ResponseWriter, r *http.Request) {
		HandleDrawEvents(state, w, r)
	})

	post := r.Methods(http.MethodPost).Subrouter()
	post.HandleFunc("/tile", func(w http.ResponseWriter, r *http.Request) {
		HandlePostTile(state, w, r)
	})

	return r
}
