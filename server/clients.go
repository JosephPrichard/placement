package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/rs/zerolog/log"
	"net/http"
	"net/url"
)

const CaptchaSecret string = ""

type RecaptchaApi interface {
	Verify(ctx context.Context, token string, ipaddr string) error
}

type RecaptchaClient struct {
	client http.Client
}

type VerifyResp struct {
	Success    bool     `json:"success"`
	ErrorCodes []string `json:"error-codes,omitempty"`
}

var InvalidCaptchaErr = errors.New("invalid recaptcha token")

func (c *RecaptchaClient) Verify(ctx context.Context, token string, remoteIP string) error {
	if token == "" {
		return InvalidCaptchaErr
	}

	data := url.Values{}
	data.Set("secret", CaptchaSecret)
	data.Set("response", token)
	if remoteIP != "" {
		data.Set("remoteip", remoteIP)
	}

	req, err := http.NewRequest("POST", "https://www.google.com/recaptcha/api/siteverify", bytes.NewBufferString(data.Encode()))
	if err != nil {
		log.Err(err).Msg("failed to create verify captcha request")
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.client.Do(req)
	if err != nil {
		log.Err(err).Msg("failed to send verify captcha request")
		return err
	}
	defer resp.Body.Close()

	var verifyResp VerifyResp
	if err := json.NewDecoder(resp.Body).Decode(&verifyResp); err != nil || len(verifyResp.ErrorCodes) > 0 {
		log.Err(err).Strs("errorCodes", verifyResp.ErrorCodes).Msg("failed to deserialize verify captcha response")
		return err
	}

	log.Info().
		Any("trace", ctx.Value("trace")).Any("response", verifyResp).Any("values", data).
		Msg("sent verify captcha response with content")

	if verifyResp.Success {
		return nil
	} else {
		return InvalidCaptchaErr
	}
}
