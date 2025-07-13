package handlers

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"placement/app/clients"
	"placement/app/cql"
)

type ServiceError struct {
	Msg  string `json:"msg"`
	Code int    `json:"code"`
}

func (e ServiceError) Error() string {
	return e.Msg
}

// maps sentinel knownErrors to known error codes
var knownErrors = map[error]int{
	cql.TileNotFoundErr:       http.StatusNotFound,
	clients.InvalidCaptchaErr: http.StatusUnauthorized,
}

func Error(w http.ResponseWriter, r *http.Request, err error) error {
	ctx := r.Context()

	svcErr := ServiceError{Msg: "an unexpected error has occurred", Code: http.StatusInternalServerError}
	errors.As(err, &svcErr)
	if c, ok := knownErrors[err]; ok {
		svcErr.Msg = err.Error()
		svcErr.Code = c
	}

	if svcErr.Code == http.StatusInternalServerError {
		slog.Error("unexpected error occurred in request", "trace", ctx.Value("trace"), "err", err)
	} else {
		slog.Info("service error occurred in request", "trace", ctx.Value("trace"), "err", err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(svcErr.Code)

	_ = json.NewEncoder(w).Encode(svcErr)
	return svcErr
}

func ErrorCode(w http.ResponseWriter, r *http.Request, msg string, code int) error {
	return Error(w, r, ServiceError{Msg: msg, Code: code})
}
