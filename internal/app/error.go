package app

import (
	"encoding/json"
	"errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"net/http"
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
	TileNotFoundErr:   http.StatusNotFound,
	InvalidCaptchaErr: http.StatusUnauthorized,
}

func Error(w http.ResponseWriter, r *http.Request, err error) error {
	ctx := r.Context()

	svcErr := ServiceError{Msg: "an unexpected error has occurred", Code: http.StatusInternalServerError}
	errors.As(err, &svcErr)
	if c, ok := knownErrors[err]; ok {
		svcErr.Msg = err.Error()
		svcErr.Code = c
	}

	var e *zerolog.Event
	if svcErr.Code == http.StatusInternalServerError {
		e = log.Err(err)
	} else {
		e = log.Info()
	}
	e.Any("trace", ctx.Value("trace")).Any("err", svcErr).Msg("error occurred in request")

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(svcErr.Code)

	_ = json.NewEncoder(w).Encode(svcErr)
	return svcErr
}

func ErrorCode(w http.ResponseWriter, r *http.Request, msg string, code int) error {
	return Error(w, r, ServiceError{Msg: msg, Code: code})
}
