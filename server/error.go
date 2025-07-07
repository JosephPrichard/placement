package server

import (
	"encoding/json"
	"github.com/rs/zerolog/log"
	"net/http"
)

// maps sentinel knownErrors to known error codes
var knownErrors = map[error]int{
	TileNotFoundError: http.StatusBadRequest,
}

func Error(w http.ResponseWriter, r *http.Request, err error) {
	ctx := r.Context()

	msg := "an unexpected error has occurred"
	code := http.StatusInternalServerError

	if v, ok := knownErrors[err]; ok {
		msg = err.Error()
		code = v
	}

	log.Err(err).
		Any("trace", ctx.Value("trace")).Int("code", code).
		Msg("Error occurred in request")

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)

	_ = json.NewEncoder(w).Encode(ServiceError{msg: msg, code: code})
}

func ErrorCode(w http.ResponseWriter, r *http.Request, msg string, code int) {
	ctx := r.Context()

	log.Warn().
		Any("trace", ctx.Value("trace")).Str("msg", msg).Int("code", code).
		Msg("Error occurred in request")

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)

	_ = json.NewEncoder(w).Encode(ServiceError{msg: msg, code: code})
}
