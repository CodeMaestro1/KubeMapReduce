package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	defaultAdminHTTPTimeout = 10 * time.Second
	adminTokenRefreshSkew   = 30 * time.Second
	defaultTokenTTL         = 60 * time.Second
	maxAdminTokenTTL        = 12 * time.Hour
)

// KeycloakAdminClient provides high-level administrative operations on a
// Keycloak realm.
//
// It encapsulates the management of an administrative access token,
// automatically refreshing it when it expires. The client is safe for
// concurrent use and implements a thundering-herd prevention mechanism
// for token refreshes.
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

	tokenMu              sync.Mutex
	tokenCond            *sync.Cond
	tokenRefreshInFlight bool
	cachedAdminToken     string
	cachedAdminTokenTill time.Time
}

// CreateUserRequest defines the parameters for creating a new user in Keycloak.
type CreateUserRequest struct {
	Username string
	Email    string
	Password string
	Role     string
}

// keycloakTokenResponse is the JSON structure returned by the token endpoint.
type keycloakTokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
}

// keycloakUser represents the User object in Keycloak's Admin REST API.
type keycloakUser struct {
	ID       string `json:"id,omitempty"`
	Username string `json:"username"`
	Email    string `json:"email,omitempty"`
	Enabled  bool   `json:"enabled"`
}

// roleMapping represents a Realm Role in Keycloak.
type roleMapping struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// NewKeycloakAdminClient creates a new administrative client for Keycloak.
//
// It requires administrative credentials, which it uses to obtain tokens
// for all subsequent operations.
func NewKeycloakAdminClient(baseURL string, targetRealm string, adminUser string, adminPass string) *KeycloakAdminClient {
	client := &KeycloakAdminClient{
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
	client.tokenCond = sync.NewCond(&client.tokenMu)
	return client
}

// CreateUser creates a user, sets their password, and assigns a role in a
// single logical operation.
//
// It performs three sequential API calls to Keycloak. If any step fails, the
// user may be left in a partially configured state.
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

// DeleteUserByUsername finds a user by their username and removes them.
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

// getAdminAccessToken retrieves or refreshes the administrative token.
//
// It uses a [sync.Cond] to ensure only one goroutine performs a network
// refresh at a time, while others wait for the result.
func (c *KeycloakAdminClient) getAdminAccessToken(ctx context.Context) (string, error) {
	if c.adminUser == "" || c.adminPass == "" {
		return "", fmt.Errorf("keycloak admin credentials are missing")
	}

	c.tokenMu.Lock()
	for {
		now := time.Now()
		if c.cachedAdminToken != "" && now.Add(adminTokenRefreshSkew).Before(c.cachedAdminTokenTill) {
			token := c.cachedAdminToken
			c.tokenMu.Unlock()
			return token, nil
		}

		if !c.tokenRefreshInFlight {
			c.tokenRefreshInFlight = true
			break
		}

		c.tokenCond.Wait()
	}
	c.tokenMu.Unlock()

	form := url.Values{}
	form.Set("grant_type", "password")
	form.Set("client_id", c.tokenClient)
	form.Set("username", c.adminUser)
	form.Set("password", c.adminPass)

	tokenURL := fmt.Sprintf("%s/realms/%s/protocol/openid-connect/token", c.baseURL, c.adminRealm)
	httpResp, err := c.doRequest(ctx, http.MethodPost, tokenURL, "", "application/x-www-form-urlencoded", []byte(form.Encode()))
	if err != nil {
		c.tokenMu.Lock()
		c.tokenRefreshInFlight = false
		c.tokenCond.Broadcast()
		c.tokenMu.Unlock()
		return "", err
	}
	defer httpResp.Body.Close()

	if err := ensureStatus(httpResp, http.StatusOK, "get admin token"); err != nil {
		c.tokenMu.Lock()
		c.tokenRefreshInFlight = false
		c.tokenCond.Broadcast()
		c.tokenMu.Unlock()
		return "", err
	}

	var tokenResponse keycloakTokenResponse
	if err := json.NewDecoder(httpResp.Body).Decode(&tokenResponse); err != nil {
		c.tokenMu.Lock()
		c.tokenRefreshInFlight = false
		c.tokenCond.Broadcast()
		c.tokenMu.Unlock()
		return "", err
	}

	if tokenResponse.AccessToken == "" {
		c.tokenMu.Lock()
		c.tokenRefreshInFlight = false
		c.tokenCond.Broadcast()
		c.tokenMu.Unlock()
		return "", fmt.Errorf("no admin access token in response")
	}

	ttl, ttlErr := validateAdminTokenTTL(tokenResponse.ExpiresIn)
	if ttlErr != nil {
		c.tokenMu.Lock()
		c.tokenRefreshInFlight = false
		c.tokenCond.Broadcast()
		c.tokenMu.Unlock()
		return "", ttlErr
	}

	c.tokenMu.Lock()
	c.cachedAdminToken = tokenResponse.AccessToken
	c.cachedAdminTokenTill = time.Now().Add(ttl)
	c.tokenRefreshInFlight = false
	c.tokenCond.Broadcast()
	c.tokenMu.Unlock()

	return tokenResponse.AccessToken, nil
}

func validateAdminTokenTTL(expiresIn int) (time.Duration, error) {
	if expiresIn < 0 {
		return 0, fmt.Errorf("invalid token expires_in value: %d", expiresIn)
	}
	if expiresIn == 0 {
		return defaultTokenTTL, nil
	}

	ttl := time.Duration(expiresIn) * time.Second
	if ttl > maxAdminTokenTTL {
		return maxAdminTokenTTL, nil
	}

	return ttl, nil
}

// setUserPassword updates a user's password in Keycloak.
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

// assignRealmRole assigns a specific realm role to a user.
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

// findUserID searches for a user ID by exact username match.
func (c *KeycloakAdminClient) findUserID(ctx context.Context, token string, username string) (string, error) {
	searchURL, err := url.Parse(fmt.Sprintf("%s/admin/realms/%s/users", c.baseURL, c.targetRealm))
	if err != nil {
		return "", err
	}
	q := searchURL.Query()
	q.Set("username", username)
	q.Set("exact", "true")
	searchURL.RawQuery = q.Encode()
	httpResp, err := c.doRequest(ctx, http.MethodGet, searchURL.String(), token, "", nil)
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

// doRequest performs an HTTP request with automatic retries for transient
// errors.
//
// It respects the context for cancellation and uses an exponential backoff
// strategy.
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

// backoffDelay calculates the wait time before a retry attempt.
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
