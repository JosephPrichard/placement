package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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
		slog.Error("failed to send verify captcha request", "retries", retry, "error", err)
		return VerifyResp{}, err
	}
	defer resp.Body.Close()

	err = json.NewDecoder(resp.Body).Decode(&verifyResp)
	if err != nil {
		slog.Error("failed to deserialize verify captcha response", "retries", retry, "error", err)
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
		slog.Error("failed to create verify captcha request", "error", err)
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	ticker := time.NewTicker(RetryDelay)
	defer ticker.Stop()

	for retry := 0; retry < ExternalRetries; retry++ {
		<-ticker.C

		resp, err = c.sendVerifyRequest(req, retry)
		if err != nil {
			// retry on failure
			continue
		}

		slog.Info("received verify captcha response with content", "trace", ctx.Value("trace"), "response", resp, "values", data)

		if resp.Success && len(resp.ErrorCodes) == 0 {
			return nil
		} else {
			return InvalidCaptchaErr
		}
	}

	return fmt.Errorf("verify captcha request timed out: %v", err)
}
