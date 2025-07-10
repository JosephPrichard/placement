package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"net/http"
	"net/url"
	"time"
)

const ExternalRetries = 5
const RetryDelay = time.Second
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

func (c *RecaptchaClient) sendVerifyRequest(req *http.Request, retry int) (VerifyResp, error) {
	var resp *http.Response
	var verifyResp VerifyResp

	resp, err := c.client.Do(req)
	if err != nil {
		log.Err(err).Int("retries", retry).Msg("failed to send verify captcha request")
		return VerifyResp{}, err
	}
	defer resp.Body.Close()

	err = json.NewDecoder(resp.Body).Decode(&verifyResp)
	if err != nil {
		log.Err(err).Int("retries", retry).Msg("failed to deserialize verify captcha response")
		return VerifyResp{}, err
	}

	return verifyResp, nil
}

func (c *RecaptchaClient) Verify(ctx context.Context, token string, remoteIP string) error {
	if token == "" {
		return InvalidCaptchaErr
	}

	var req *http.Request
	var resp VerifyResp
	var err error

	data := url.Values{}
	data.Set("secret", CaptchaSecret)
	data.Set("response", token)
	if remoteIP != "" {
		data.Set("remoteip", remoteIP)
	}

	req, err = http.NewRequest(http.MethodPost, "https://www.google.com/recaptcha/api/siteverify", bytes.NewBufferString(data.Encode()))
	if err != nil {
		log.Err(err).Msg("failed to create verify captcha request")
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	ticker := time.NewTicker(RetryDelay)
	defer ticker.Stop()

	for retry := 0; retry < ExternalRetries; retry++ {
		<-ticker.C

		resp, err = c.sendVerifyRequest(req, retry)
		if err != nil {
			// retry after a delay whenever the http request fails, the error will be captured outside the loop
			continue
		}

		log.Info().
			Any("trace", ctx.Value("trace")).Any("response", resp).Any("values", data).
			Msg("received verify captcha response with content")

		if resp.Success && len(resp.ErrorCodes) == 0 {
			return nil
		} else {
			return InvalidCaptchaErr
		}
	}

	return fmt.Errorf("verify captcha request timed out: %v", err)
}
