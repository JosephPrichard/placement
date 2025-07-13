package handlers

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"image/color"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"placement/app/clients"
	"placement/app/cql"
	"placement/app/dict"
	"placement/app/models"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
	"github.com/gocql/gocql"
	"github.com/google/uuid"
)

const DrawPeriod = time.Minute

//go:embed *
var staticDir embed.FS

type State struct {
	Rdb       *redis.Client
	Cdb       *gocql.Session
	SubChan   chan Subscriber
	UnsubChan chan string
	Recaptcha clients.RecaptchaApi
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

	slog.Info("handling GetTile", "trace", ctx.Value("trace"), "point", point)

	tile, err := cql.GetOneTile(ctx, state.Cdb, point.X, point.Y)
	if errors.Is(err, cql.TileNotFoundErr) {
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
	key := models.KeyFromPoint(point.X, point.Y)

	slog.Info("handling GetGroup", "trace", ctx.Value("trace"), "point", point, "key", key)

	var tg models.TileGroup
	if tg, err = dict.GetCachedGroup(ctx, state.Rdb, point.X, point.Y); err != nil {
		return Error(w, r, err)
	}
	if tg == nil {
		slog.Info("executing GetTileGroup to retrieve group, group was not cached", "trace", ctx.Value("trace"), "point", point, "key", key)
		if tg, err = cql.GetTileGroup(ctx, state.Cdb, point.X, point.Y); err != nil {
			return Error(w, r, err)
		}
		go func() {
			if err = dict.SetCachedGroup(ctx, state.Rdb, point.X, point.Y, tg); err != nil {
				slog.Error("failed SetCachedGroup in background task", "err", err)
			}
		}()
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(tg)
	return nil
}

func HandleGetTiles(state State, w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()

	query := r.URL.Query()
	afterStr := query.Get("after")

	slog.Info("handling GetTiles", "trace", ctx.Value("trace"), "after", afterStr)

	var after time.Time
	var err error

	if afterStr != "" {
		if after, err = time.Parse(time.RFC3339, afterStr); err != nil {
			return ErrorCode(w, r, fmt.Sprintf("after must be a valid RFC3339 timestamp, got %s", afterStr), http.StatusBadRequest)
		}
	} else {
		after = time.Now()
	}

	tiles, err := cql.GetTiles(ctx, state.Cdb, after)
	if err != nil {
		return Error(w, r, err)
	}
	if tiles == nil {
		tiles = make([]models.Tile, 0)
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
	draw := models.Draw{
		X:   body.X,
		Y:   body.Y,
		Rgb: color.RGBA{R: body.Rgb[0], G: body.Rgb[1], B: body.Rgb[2], A: 255},
	}
	ip := getIpAddr(r)
	if ip == nil {
		return ErrorCode(w, r, "ip must be a valid IP address", http.StatusBadRequest)
	}
	ipStr := ip.String()

	slog.Info("Handling PostTile", "trace", ctx.Value("trace"), "draw", draw, "ip", ipStr)

	if err := state.Recaptcha.Verify(ctx, token, ipStr); err != nil {
		return Error(w, r, err)
	}

	now := time.Now()
	ret, err := dict.AcquireExpiringLock(ctx, state.Rdb, token, now, now.Add(-DrawPeriod), DrawPeriod)
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

	if err = dict.UpsertCachedGroup(ctx, state.Rdb, draw); err != nil {
		return Error(w, r, err)
	}

	bgTask := context.WithValue(context.Background(), "trace", ctx.Value("trace"))
	go func() {
		if err := cql.BatchUpsertTile(bgTask, state.Cdb, []cql.BatchUpsertArgs{{draw.X, draw.Y, draw.Rgb, ip, now}}); err != nil {
			slog.Error("execution BatchUpsertTile failed in background task", "err", err)
		}
	}()
	go func() {
		if err := BroadcastDraw(bgTask, state.Rdb, draw); err != nil {
			slog.Error("execution BroadcastDraw failed in background task", "err", err)
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
	subChan := make(chan models.Draw)
	state.SubChan <- Subscriber{id: sseId, subChan: subChan}
	defer func() {
		state.UnsubChan <- sseId
		close(subChan)
	}()

	for {
		select {
		case <-ctx.Done():
			slog.Info("client disconnected from sse", "sseId", sseId)
			return nil
		case draw := <-subChan:
			slog.Info("client retrieved draw msg", "sseId", sseId, "draw", draw)
			_ = json.NewEncoder(w).Encode(draw)
			flusher.Flush()
		}
	}
}

func routeHandler(state State, handler func(state State, w http.ResponseWriter, r *http.Request) error) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		trace := r.Header.Get("trace")
		if trace == "" {
			trace = uuid.NewString()
		}
		ctx := context.WithValue(r.Context(), "trace", trace)
		r = r.WithContext(ctx)

		method := r.Method
		path := r.URL.String()

		slog.Info("handling an http request", "trace", trace, "method", method, "path", path)

		defer func() {
			if ret := recover(); ret != nil {
				_ = Error(w, r, fmt.Errorf("panic in route handler method=%s path=%s: %v", method, path, ret))
			}
		}()
		if err := handler(state, w, r); err != nil {
			slog.Info("error occurred in route handler func", "trace", trace, "err", err.Error(), "method", method, "path", path)
		}
	}
}

func HandleServer(state State) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /tiles", routeHandler(state, HandleGetTiles))
	mux.HandleFunc("GET /tile", routeHandler(state, HandleGetTile))
	mux.HandleFunc("GET /group", routeHandler(state, HandleGetGroup))
	mux.HandleFunc("GET /draw/events", routeHandler(state, HandleDrawEvents))
	mux.HandleFunc("POST /tile", routeHandler(state, HandlePostTile))
	mux.HandleFunc("/", http.FileServer(http.FS(staticDir)).ServeHTTP)

	return mux
}
