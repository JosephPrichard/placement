package server

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/rs/zerolog/log"
	"net/http"
	"net/url"
)

const CaptchaSecret string = ""

type RecaptchaApi interface {
	Verify(ctx context.Context, token string, ipaddr string) (bool, error)
}

type RecaptchaClient struct {
	client http.Client
}

type VerifyResp struct {
	Success    bool     `json:"success"`
	ErrorCodes []string `json:"error-codes,omitempty"`
}

func (c *RecaptchaClient) Verify(ctx context.Context, token string, remoteIP string) (bool, error) {
	if token == "" {
		return false, nil
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
		return false, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.client.Do(req)
	if err != nil {
		log.Err(err).Msg("failed to send verify captcha request")
		return false, err
	}
	defer resp.Body.Close()

	var verifyResp VerifyResp
	if err := json.NewDecoder(resp.Body).Decode(&verifyResp); err != nil || len(verifyResp.ErrorCodes) > 0 {
		log.Err(err).Strs("errorCodes", verifyResp.ErrorCodes).Msg("failed to deserialize verify captcha response")
		return false, err
	}

	log.Info().
		Any("trace", ctx.Value("trace")).Any("response", verifyResp).Any("values", data).
		Msg("sent verify captcha response with content")
	return true, nil
}
