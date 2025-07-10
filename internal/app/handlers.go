package app

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
	Recaptcha RecaptchaApi
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

func HandleGetTile(state State, w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()

	point, err := parsePoint(r.URL)
	if err != nil {
		return ErrorCode(w, r, err.Error(), http.StatusBadRequest)
	}

	log.Info().
		Any("trace", ctx.Value("trace")).Any("point", point).
		Msg("handling GetTile")

	tile, err := GetOneTile(ctx, state.Cdb, point.X, point.Y)
	if errors.Is(err, TileNotFoundErr) {
		return Error(w, r, err)
	}
	if err != nil {
		return Error(w, r, err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	_ = json.NewEncoder(w).Encode(tile)
	return nil
}

func HandleGetGroup(state State, w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()

	point, err := parsePoint(r.URL)
	if err != nil {
		return Error(w, r, err)
	}
	key := KeyFromPoint(point.X, point.Y)

	log.Info().
		Any("trace", ctx.Value("trace")).Any("point", point).Any("key", key).
		Msg("handling GetGroup")

	var tg TileGroup
	if tg, err = GetCachedGroup(ctx, state.Rdb, key); err != nil {
		return Error(w, r, err)
	}
	if tg == nil {
		log.Info().
			Any("trace", ctx.Value("trace")).Any("point", point).Any("key", key).
			Msg("executing GetTileGroup to retrieve group, group was not cached")

		if tg, err = GetTileGroup(ctx, state.Cdb, key); err != nil {
			return Error(w, r, err)
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
	return nil
}

func HandleGetPlacements(state State, w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()

	query := r.URL.Query()
	daysStr := query.Get("days")
	afterStr := query.Get("after")

	log.Info().
		Any("trace", ctx.Value("trace")).Str("days", daysStr).Str("after", afterStr).
		Msg("handling GetPlacements")

	var days int
	var after time.Time
	var err error

	if daysStr != "" {
		if days, err = strconv.Atoi(daysStr); err != nil {
			return ErrorCode(w, r, fmt.Sprintf("days must be an integer, got %s", daysStr), http.StatusBadRequest)
		}
	}
	if afterStr != "" {
		if after, err = time.Parse(time.RFC3339, afterStr); err != nil {
			return ErrorCode(w, r, fmt.Sprintf("after must be a valid RFC3339 timestamp, got %s", daysStr), http.StatusBadRequest)
		}
	} else {
		after = time.Now()
	}

	var tiles []Tile
	if days <= 0 || after.IsZero() {
		tiles = make([]Tile, 0)
	} else {
		if tiles, err = GetTiles(ctx, state.Cdb, int64(days), after); err != nil {
			return Error(w, r, err)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	_ = json.NewEncoder(w).Encode(tiles)
	return nil
}

func getIpAddr(r *http.Request) net.IP {
	var ip string
	var err error

	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// X-Forwarded-For may contain multiple IPs: client, proxy1, proxy2, ...
		parts := strings.Split(xff, ",")
		ip = strings.TrimSpace(parts[0])
	} else if xrip := r.Header.Get("X-Real-IP"); xrip != "" {
		ip = xrip
	} else if ip, _, err = net.SplitHostPort(r.RemoteAddr); err != nil {
		ip = r.RemoteAddr
	}

	return net.ParseIP(ip)
}

const RecaptchaTokenHeader = "X-Recaptcha-Request-Token"

type PostTileBody struct {
	X   int    `json:"x"`
	Y   int    `json:"y"`
	Rgb []byte `json:"rgb"`
}

func HandlePostTile(state State, w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()

	token := r.Header.Get(RecaptchaTokenHeader)

	var body PostTileBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return ErrorCode(w, r, "failed to deserialize response, expected PostTileBody", http.StatusBadRequest)
	}
	if len(body.Rgb) != 3 {
		return ErrorCode(w, r, fmt.Sprintf("rgb color tuple must be of length 3, was %d", len(body.Rgb)), http.StatusBadRequest)
	}
	draw := Draw{
		X:   body.X,
		Y:   body.Y,
		Rgb: color.RGBA{R: body.Rgb[0], G: body.Rgb[1], B: body.Rgb[2], A: 255},
	}
	ip := getIpAddr(r)
	if ip == nil {
		return ErrorCode(w, r, fmt.Sprintf("ip must be a valid IP address"), http.StatusBadRequest)
	}

	ipStr := ip.String()
	log.Info().Any("trace", ctx.Value("trace")).Any("draw", draw).Str("ip", ipStr).Msg("Handling PostTile")

	err := state.Recaptcha.Verify(ctx, token, ipStr)
	if err != nil {
		return Error(w, r, err)
	}

	now := time.Now()
	ret, err := AcquireExpiringLock(ctx, state.Rdb, token, now, now.Add(-DrawPeriod), DrawPeriod)
	if err != nil {
		return Error(w, r, err)
	}

	if ret >= 0 {
		timePlaced := time.Unix(ret, 0)
		difference := now.Sub(timePlaced)
		remaining := DrawPeriod - difference
		if remaining < 0 || timePlaced.After(now) {
			return ErrorCode(w, r, fmt.Sprintf("invariant is false: remaining=%d must be larger than 0 and timePlaced=%v must be before now=%v", remaining, timePlaced, now), http.StatusInternalServerError)
		}
		return ErrorCode(w, r, fmt.Sprintf("%d minutes remaining until player can draw another tile", int64(remaining.Minutes())), http.StatusUnauthorized)
	}

	if err = UpsertCachedGroup(ctx, state.Rdb, draw); err != nil {
		return Error(w, r, err)
	}

	bgTask := context.WithValue(context.Background(), "trace", ctx.Value("trace"))
	go func() {
		if err := BatchUpsertTile(bgTask, state.Cdb, []BatchUpsertArgs{{draw.X, draw.Y, draw.Rgb, ip, now}}); err != nil {
			log.Err(err).Msg("execution BatchUpsertTile failed in background task")
		}
	}()
	go func() {
		if err := BroadcastDraw(bgTask, state.Rdb, draw); err != nil {
			log.Err(err).Msg("execution BroadcastDraw failed in background task")
		}
	}()

	w.WriteHeader(http.StatusOK)
	return nil
}

func HandleDrawEvents(state State, w http.ResponseWriter, r *http.Request) error {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return nil
	}

	ctx := r.Context()

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
			log.Info().Str("sseId", sseId).Msg("client disconnected from sse")
			return nil
		case draw := <-subChan:
			log.Info().Str("sseId", sseId).Any("draw", draw).Msg("client retrieved draw msg")
			_ = json.NewEncoder(w).Encode(draw)
			flusher.Flush()
		}
	}
}

func sideChannelMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		trace := r.Header.Get("trace")
		if trace == "" {
			trace = uuid.NewString()
		}
		ctx := context.WithValue(r.Context(), "trace", trace)
		r = r.WithContext(ctx)

		next.ServeHTTP(w, r)
	})
}

func routeHandler(state State, handler func(state State, w http.ResponseWriter, r *http.Request) error) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		trace := ctx.Value("trace")
		method := r.Method
		path := r.URL.String()

		log.Info().
			Any("trace", trace).Str("method", method).Str("path", path).
			Msg("handling an http request")

		defer func() {
			if ret := recover(); ret != nil {
				_ = Error(w, r, fmt.Errorf("panic in route handler method=%s path=%s: %v", method, path, ret))
			}
		}()
		if err := handler(state, w, r); err != nil {
			log.Info().
				Any("trace", trace).Str("err", err.Error()).Str("method", method).Str("path", path).
				Msg("error occurred in route handler func")
		}
	}
}

func HandleServer(state State) http.Handler {
	r := mux.NewRouter()

	r.Use(sideChannelMiddleware)

	get := r.Methods(http.MethodGet).Subrouter()
	get.HandleFunc("/placements", routeHandler(state, HandleGetPlacements))
	get.HandleFunc("/tile", routeHandler(state, HandleGetTile))
	get.HandleFunc("/group", routeHandler(state, HandleGetGroup))
	get.HandleFunc("/draw/events", routeHandler(state, HandleDrawEvents))

	post := r.Methods(http.MethodPost).Subrouter()
	post.HandleFunc("/tile", routeHandler(state, HandlePostTile))

	return r
}
