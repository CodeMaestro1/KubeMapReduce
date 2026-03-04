package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	defaultAdminHTTPTimeout = 10 * time.Second
	defaultRetryAttempts    = 3
	defaultRetryBaseDelay   = 200 * time.Millisecond
)

type KeycloakAdminClient struct {
	baseURL     string
	targetRealm string
	adminRealm  string
	adminUser   string
	adminPass   string
	httpClient  *http.Client
	tokenClient string

	retryAttempts  int
	retryBaseDelay time.Duration
}

type CreateUserRequest struct {
	Username string
	Email    string
	Password string
	Role     string
}

type keycloakTokenResponse struct {
	AccessToken string `json:"access_token"`
}

type keycloakUser struct {
	ID       string `json:"id,omitempty"`
	Username string `json:"username"`
	Email    string `json:"email,omitempty"`
	Enabled  bool   `json:"enabled"`
}

type roleMapping struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type ServiceUnavailableError struct {
	Operation string
	Err       error
}

func (e *ServiceUnavailableError) Error() string {
	if e == nil {
		return "authentication service unavailable"
	}

	if e.Operation == "" {
		if e.Err == nil {
			return "authentication service unavailable"
		}
		return fmt.Sprintf("authentication service unavailable: %v", e.Err)
	}

	if e.Err == nil {
		return fmt.Sprintf("authentication service unavailable while %s", e.Operation)
	}

	return fmt.Sprintf("authentication service unavailable while %s: %v", e.Operation, e.Err)
}

func (e *ServiceUnavailableError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func IsServiceUnavailable(err error) bool {
	var unavailableErr *ServiceUnavailableError
	return errors.As(err, &unavailableErr)
}

func NewServiceUnavailableError(operation string, err error) error {
	return &ServiceUnavailableError{Operation: operation, Err: err}
}

func NewKeycloakAdminClient(baseURL string, targetRealm string, adminUser string, adminPass string) *KeycloakAdminClient {
	return &KeycloakAdminClient{
		baseURL:     strings.TrimRight(baseURL, "/"),
		targetRealm: targetRealm,
		adminRealm:  "master",
		adminUser:   adminUser,
		adminPass:   adminPass,
		tokenClient: "admin-cli",
		httpClient:  &http.Client{Timeout: defaultAdminHTTPTimeout},

		retryAttempts:  defaultRetryAttempts,
		retryBaseDelay: defaultRetryBaseDelay,
	}
}

func (c *KeycloakAdminClient) CreateUser(ctx context.Context, req CreateUserRequest) error {
	if req.Username == "" || req.Password == "" || req.Role == "" {
		return fmt.Errorf("username, password and role are required")
	}

	token, err := c.getAdminAccessToken(ctx)
	if err != nil {
		return err
	}

	newUser := keycloakUser{
		Username: req.Username,
		Email:    req.Email,
		Enabled:  true,
	}

	payload, err := json.Marshal(newUser)
	if err != nil {
		return err
	}

	createURL := fmt.Sprintf("%s/admin/realms/%s/users", c.baseURL, c.targetRealm)
	httpResp, err := c.doRequest(ctx, http.MethodPost, createURL, token, "application/json", payload)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	if err := ensureStatus(httpResp, http.StatusCreated, "create user"); err != nil {
		return err
	}

	userID, err := extractUserIDFromLocation(httpResp.Header.Get("Location"))
	if err != nil {
		return err
	}

	if err := c.setUserPassword(ctx, token, userID, req.Password); err != nil {
		return err
	}

	if err := c.assignRealmRole(ctx, token, userID, req.Role); err != nil {
		return err
	}

	return nil
}

func (c *KeycloakAdminClient) DeleteUserByUsername(ctx context.Context, username string) error {
	if username == "" {
		return fmt.Errorf("username is required")
	}

	token, err := c.getAdminAccessToken(ctx)
	if err != nil {
		return err
	}

	userID, err := c.findUserID(ctx, token, username)
	if err != nil {
		return err
	}

	deleteURL := fmt.Sprintf("%s/admin/realms/%s/users/%s", c.baseURL, c.targetRealm, userID)
	httpResp, err := c.doRequest(ctx, http.MethodDelete, deleteURL, token, "", nil)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	if err := ensureStatus(httpResp, http.StatusNoContent, "delete user"); err != nil {
		return err
	}

	return nil
}

func (c *KeycloakAdminClient) getAdminAccessToken(ctx context.Context) (string, error) {
	if c.adminUser == "" || c.adminPass == "" {
		return "", fmt.Errorf("keycloak admin credentials are missing")
	}

	form := url.Values{}
	form.Set("grant_type", "password")
	form.Set("client_id", c.tokenClient)
	form.Set("username", c.adminUser)
	form.Set("password", c.adminPass)

	tokenURL := fmt.Sprintf("%s/realms/%s/protocol/openid-connect/token", c.baseURL, c.adminRealm)
	httpResp, err := c.doRequest(ctx, http.MethodPost, tokenURL, "", "application/x-www-form-urlencoded", []byte(form.Encode()))
	if err != nil {
		return "", err
	}
	defer httpResp.Body.Close()

	if err := ensureStatus(httpResp, http.StatusOK, "get admin token"); err != nil {
		return "", err
	}

	var tokenResponse keycloakTokenResponse
	if err := json.NewDecoder(httpResp.Body).Decode(&tokenResponse); err != nil {
		return "", err
	}

	if tokenResponse.AccessToken == "" {
		return "", fmt.Errorf("no admin access token in response")
	}

	return tokenResponse.AccessToken, nil
}

func (c *KeycloakAdminClient) setUserPassword(ctx context.Context, token string, userID string, password string) error {
	resetURL := fmt.Sprintf("%s/admin/realms/%s/users/%s/reset-password", c.baseURL, c.targetRealm, userID)
	payload := map[string]interface{}{
		"type":      "password",
		"value":     password,
		"temporary": false,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	httpResp, err := c.doRequest(ctx, http.MethodPut, resetURL, token, "application/json", data)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	if err := ensureStatus(httpResp, http.StatusNoContent, "set password"); err != nil {
		return err
	}

	return nil
}

func (c *KeycloakAdminClient) assignRealmRole(ctx context.Context, token string, userID string, roleName string) error {
	roleName = strings.ToUpper(strings.TrimSpace(roleName))
	if roleName == "" {
		return fmt.Errorf("role name is required")
	}

	roleURL := fmt.Sprintf("%s/admin/realms/%s/roles/%s", c.baseURL, c.targetRealm, url.PathEscape(roleName))
	httpResp, err := c.doRequest(ctx, http.MethodGet, roleURL, token, "", nil)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	if err := ensureStatus(httpResp, http.StatusOK, fmt.Sprintf("fetch role %s", roleName)); err != nil {
		return err
	}

	var role roleMapping
	if err := json.NewDecoder(httpResp.Body).Decode(&role); err != nil {
		return err
	}

	assignURL := fmt.Sprintf("%s/admin/realms/%s/users/%s/role-mappings/realm", c.baseURL, c.targetRealm, userID)
	payload, err := json.Marshal([]roleMapping{role})
	if err != nil {
		return err
	}

	assignResp, err := c.doRequest(ctx, http.MethodPost, assignURL, token, "application/json", payload)
	if err != nil {
		return err
	}
	defer assignResp.Body.Close()

	if err := ensureStatus(assignResp, http.StatusNoContent, "assign role"); err != nil {
		return err
	}

	return nil
}

func (c *KeycloakAdminClient) findUserID(ctx context.Context, token string, username string) (string, error) {
	searchURL := fmt.Sprintf("%s/admin/realms/%s/users?username=%s&exact=true", c.baseURL, c.targetRealm, url.QueryEscape(username))
	httpResp, err := c.doRequest(ctx, http.MethodGet, searchURL, token, "", nil)
	if err != nil {
		return "", err
	}
	defer httpResp.Body.Close()

	if err := ensureStatus(httpResp, http.StatusOK, "search user"); err != nil {
		return "", err
	}

	var users []keycloakUser
	if err := json.NewDecoder(httpResp.Body).Decode(&users); err != nil {
		return "", err
	}

	if len(users) == 0 {
		return "", fmt.Errorf("user %s not found", username)
	}

	return users[0].ID, nil
}

func (c *KeycloakAdminClient) doRequest(ctx context.Context, method string, endpoint string, token string, contentType string, body []byte) (*http.Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	attempts := c.retryAttempts
	if attempts < 1 {
		attempts = 1
	}

	var lastErr error
	var lastStatus int
	operation := method + " " + endpoint

	for attempt := 0; attempt < attempts; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		var reader io.Reader
		if len(body) > 0 {
			reader = bytes.NewReader(body)
		}

		httpReq, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
		if err != nil {
			return nil, err
		}

		if token != "" {
			httpReq.Header.Set("Authorization", "Bearer "+token)
		}
		if contentType != "" {
			httpReq.Header.Set("Content-Type", contentType)
		}

		httpResp, err := c.httpClient.Do(httpReq)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, err
			}

			if !isRetryableError(err) || attempt == attempts-1 {
				if isRetryableError(err) {
					return nil, NewServiceUnavailableError(operation, err)
				}
				return nil, err
			}

			lastErr = err
			if err := sleepWithContext(ctx, c.backoffDelay(attempt)); err != nil {
				return nil, err
			}
			continue
		}

		if !isRetryableStatus(httpResp.StatusCode) {
			return httpResp, nil
		}

		lastStatus = httpResp.StatusCode
		_ = httpResp.Body.Close()

		if attempt == attempts-1 {
			return nil, NewServiceUnavailableError(operation, fmt.Errorf("received status %d", lastStatus))
		}

		if err := sleepWithContext(ctx, c.backoffDelay(attempt)); err != nil {
			return nil, err
		}
	}

	if lastErr != nil {
		return nil, NewServiceUnavailableError(operation, lastErr)
	}

	if lastStatus != 0 {
		return nil, NewServiceUnavailableError(operation, fmt.Errorf("received status %d", lastStatus))
	}

	return nil, NewServiceUnavailableError(operation, fmt.Errorf("request failed"))
}

func (c *KeycloakAdminClient) backoffDelay(attempt int) time.Duration {
	base := c.retryBaseDelay
	if base <= 0 {
		base = defaultRetryBaseDelay
	}

	delay := base << attempt
	maxDelay := 2 * time.Second
	if delay > maxDelay {
		return maxDelay
	}

	return delay
}

func isRetryableStatus(status int) bool {
	switch status {
	case http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func isRetryableError(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}

	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		var nestedNetErr net.Error
		return errors.As(urlErr.Err, &nestedNetErr)
	}

	return false
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func ensureStatus(resp *http.Response, expectedStatus int, operation string) error {
	if resp.StatusCode == expectedStatus {
		return nil
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to %s: status %d (failed to read response body: %v)", operation, resp.StatusCode, err)
	}

	return fmt.Errorf("failed to %s: status %d: %s", operation, resp.StatusCode, string(body))
}

func extractUserIDFromLocation(location string) (string, error) {
	if location == "" {
		return "", fmt.Errorf("missing Location header for created user")
	}

	parts := strings.Split(strings.TrimRight(location, "/"), "/")
	if len(parts) == 0 {
		return "", fmt.Errorf("invalid Location header")
	}

	userID := parts[len(parts)-1]
	if userID == "" {
		return "", fmt.Errorf("invalid user id in Location header")
	}

	return userID, nil
}
