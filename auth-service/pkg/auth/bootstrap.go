package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	defaultHTTPTimeout           = 30 * time.Second
	defaultDialTimeout           = 10 * time.Second
	defaultKeepAlive             = 30 * time.Second
	defaultTLSHandshakeTimeout   = 10 * time.Second
	defaultResponseHeaderTimeout = 15 * time.Second
	defaultExpectContinueTimeout = 1 * time.Second
	defaultIdleConnTimeout       = 90 * time.Second
)

type BootstrapConfig struct {
	BaseURL            string
	Realm              string
	ClientID           string
	UIOrigin           string
	AdminUsername      string
	AdminPassword      string
	EnableRegistration bool
	HTTPClient         *http.Client
}

type keycloakBootstrapper struct {
	baseURL            string
	realm              string
	clientID           string
	uiOrigin           string
	adminUsername      string
	adminPassword      string
	enableRegistration bool
	httpClient         *http.Client
	retryAttempts      int
	retryBaseDelay     time.Duration
	token              string
	output             io.Writer
}

type tokenResponse struct {
	AccessToken string `json:"access_token"`
}

type keycloakClient struct {
	ID string `json:"id"`
}

func BootstrapKeycloak(cfg BootstrapConfig, output io.Writer) error {
	return BootstrapKeycloakWithContext(context.Background(), cfg, output)
}

func BootstrapKeycloakWithContext(ctx context.Context, cfg BootstrapConfig, output io.Writer) error {
	bootstrapper, err := newKeycloakBootstrapper(cfg, output)
	if err != nil {
		return err
	}

	return bootstrapper.bootstrap(ctx)
}

func newKeycloakBootstrapper(cfg BootstrapConfig, output io.Writer) (*keycloakBootstrapper, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	realm := strings.TrimSpace(cfg.Realm)
	clientID := strings.TrimSpace(cfg.ClientID)
	uiOrigin := strings.TrimRight(strings.TrimSpace(cfg.UIOrigin), "/")
	adminUsername := strings.TrimSpace(cfg.AdminUsername)
	adminPassword := strings.TrimSpace(cfg.AdminPassword)

	if baseURL == "" || realm == "" || clientID == "" || uiOrigin == "" {
		return nil, fmt.Errorf("keycloak-base-url, realm, client-id, and ui-origin are required")
	}

	if adminUsername == "" || adminPassword == "" {
		return nil, fmt.Errorf("admin credentials are required")
	}

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = newBootstrapHTTPClient()
	} else {
		clientCopy := *httpClient
		if clientCopy.Timeout == 0 {
			clientCopy.Timeout = defaultHTTPTimeout
		}
		if clientCopy.Transport == nil {
			clientCopy.Transport = newBootstrapTransport()
		}
		httpClient = &clientCopy
	}

	return &keycloakBootstrapper{
		baseURL:            baseURL,
		realm:              realm,
		clientID:           clientID,
		uiOrigin:           uiOrigin,
		adminUsername:      adminUsername,
		adminPassword:      adminPassword,
		enableRegistration: cfg.EnableRegistration,
		httpClient:         httpClient,
		retryAttempts:      defaultRetryAttempts,
		retryBaseDelay:     defaultRetryBaseDelay,
		output:             output,
	}, nil
}

func newBootstrapHTTPClient() *http.Client {
	return &http.Client{
		Timeout:   defaultHTTPTimeout,
		Transport: newBootstrapTransport(),
	}
}

func newBootstrapTransport() http.RoundTripper {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   defaultDialTimeout,
			KeepAlive: defaultKeepAlive,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		TLSHandshakeTimeout:   defaultTLSHandshakeTimeout,
		ResponseHeaderTimeout: defaultResponseHeaderTimeout,
		ExpectContinueTimeout: defaultExpectContinueTimeout,
		IdleConnTimeout:       defaultIdleConnTimeout,
	}
}

func (b *keycloakBootstrapper) bootstrap(ctx context.Context) error {
	token, err := b.getAdminToken(ctx)
	if err != nil {
		return err
	}
	b.token = token

	if err := b.ensureRealm(ctx); err != nil {
		return err
	}
	if err := b.ensureRealmSettings(ctx); err != nil {
		return err
	}
	if err := b.ensureClient(ctx); err != nil {
		return err
	}
	if err := b.ensureAudienceMapper(ctx); err != nil {
		return err
	}
	if err := b.ensureRole(ctx, "USER"); err != nil {
		return err
	}
	if err := b.ensureRole(ctx, "ADMIN"); err != nil {
		return err
	}

	return nil
}

func (b *keycloakBootstrapper) getAdminToken(ctx context.Context) (string, error) {
	form := url.Values{}
	form.Set("grant_type", "password")
	form.Set("client_id", "admin-cli")
	form.Set("username", b.adminUsername)
	form.Set("password", b.adminPassword)

	resp, err := b.doRequest(ctx,
		http.MethodPost,
		b.baseURL+"/realms/master/protocol/openid-connect/token",
		"",
		"application/x-www-form-urlencoded",
		[]byte(form.Encode()),
	)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if err := ensureStatus(resp, http.StatusOK, "get admin token"); err != nil {
		return "", err
	}

	body, err := readBoundedResponseBody(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read admin token response: %w", err)
	}

	var tr tokenResponse
	if err := json.Unmarshal(body, &tr); err != nil {
		return "", err
	}
	if tr.AccessToken == "" {
		return "", fmt.Errorf("admin token missing in response")
	}

	return tr.AccessToken, nil
}

func (b *keycloakBootstrapper) ensureRealm(ctx context.Context) error {
	status, body, err := b.callJSON(ctx, http.MethodGet, "/admin/realms/"+b.realm, nil)
	if err != nil {
		return err
	}
	if status == http.StatusOK {
		b.logf("realm %q already exists", b.realm)
		return nil
	}
	if err := ensureCallStatus(status, body, http.StatusNotFound, fmt.Sprintf("check realm %q", b.realm)); err != nil {
		return err
	}

	payload := map[string]any{"realm": b.realm, "enabled": true}
	createStatus, createBody, err := b.callJSON(ctx, http.MethodPost, "/admin/realms", payload)
	if err != nil {
		return err
	}
	if err := ensureCallStatus(createStatus, createBody, http.StatusCreated, fmt.Sprintf("create realm %q", b.realm)); err != nil {
		return err
	}

	b.logf("created realm %q", b.realm)
	return nil
}

func (b *keycloakBootstrapper) ensureRealmSettings(ctx context.Context) error {
	status, body, err := b.callJSON(ctx, http.MethodGet, "/admin/realms/"+b.realm, nil)
	if err != nil {
		return err
	}
	if err := ensureCallStatus(status, body, http.StatusOK, "read realm settings"); err != nil {
		return err
	}

	var cfg map[string]any
	if err := json.Unmarshal(body, &cfg); err != nil {
		return err
	}

	cfg["registrationAllowed"] = b.enableRegistration
	cfg["loginWithEmailAllowed"] = true

	putStatus, putBody, err := b.callJSON(ctx, http.MethodPut, "/admin/realms/"+b.realm, cfg)
	if err != nil {
		return err
	}
	if err := ensureCallStatus(putStatus, putBody, http.StatusNoContent, "update realm settings"); err != nil {
		return err
	}

	b.logf("ensured realm settings (registration=%t)", b.enableRegistration)
	return nil
}

func (b *keycloakBootstrapper) ensureClient(ctx context.Context) error {
	clientUUID, err := b.findClientUUID(ctx)
	if err != nil {
		return err
	}

	if clientUUID == "" {
		payload := map[string]any{
			"clientId":                  b.clientID,
			"name":                      "MapReduce API",
			"protocol":                  "openid-connect",
			"enabled":                   true,
			"publicClient":              true,
			"directAccessGrantsEnabled": true,
			"standardFlowEnabled":       true,
			"serviceAccountsEnabled":    false,
			"fullScopeAllowed":          true,
			"rootUrl":                   b.uiOrigin,
			"baseUrl":                   "/",
			"redirectUris":              []string{b.uiOrigin + "/*"},
			"webOrigins":                []string{b.uiOrigin},
		}

		status, createBody, err := b.callJSON(ctx, http.MethodPost, "/admin/realms/"+b.realm+"/clients", payload)
		if err != nil {
			return err
		}
		if err := ensureCallStatus(status, createBody, http.StatusCreated, fmt.Sprintf("create client %q", b.clientID)); err != nil {
			return err
		}
		b.logf("created client %q", b.clientID)

		clientUUID, err = b.findClientUUID(ctx)
		if err != nil {
			return err
		}
		if clientUUID == "" {
			return fmt.Errorf("client %q created but id lookup failed", b.clientID)
		}
	} else {
		b.logf("client %q already exists", b.clientID)
	}

	status, body, err := b.callJSON(ctx, http.MethodGet, "/admin/realms/"+b.realm+"/clients/"+clientUUID, nil)
	if err != nil {
		return err
	}
	if err := ensureCallStatus(status, body, http.StatusOK, fmt.Sprintf("read client %q", b.clientID)); err != nil {
		return err
	}

	var cfg map[string]any
	if err := json.Unmarshal(body, &cfg); err != nil {
		return err
	}

	cfg["enabled"] = true
	cfg["publicClient"] = true
	cfg["directAccessGrantsEnabled"] = true
	cfg["standardFlowEnabled"] = true
	cfg["rootUrl"] = b.uiOrigin
	cfg["baseUrl"] = "/"
	cfg["redirectUris"] = []string{b.uiOrigin + "/*"}
	cfg["webOrigins"] = []string{b.uiOrigin}

	putStatus, putBody, err := b.callJSON(ctx, http.MethodPut, "/admin/realms/"+b.realm+"/clients/"+clientUUID, cfg)
	if err != nil {
		return err
	}
	if err := ensureCallStatus(putStatus, putBody, http.StatusNoContent, fmt.Sprintf("update client %q", b.clientID)); err != nil {
		return err
	}

	b.logf("ensured client redirects/web-origins for %q", b.clientID)
	return nil
}

func (b *keycloakBootstrapper) ensureAudienceMapper(ctx context.Context) error {
	clientUUID, err := b.findClientUUID(ctx)
	if err != nil {
		return err
	}
	if clientUUID == "" {
		return fmt.Errorf("cannot add audience mapper: client %q not found", b.clientID)
	}

	// Check if the audience mapper already exists.
	mappersPath := "/admin/realms/" + b.realm + "/clients/" + clientUUID + "/protocol-mappers/models"
	status, body, err := b.callJSON(ctx, http.MethodGet, mappersPath, nil)
	if err != nil {
		return err
	}
	if err := ensureCallStatus(status, body, http.StatusOK, "list protocol mappers"); err != nil {
		return err
	}

	var mappers []map[string]any
	if err := json.Unmarshal(body, &mappers); err != nil {
		return err
	}

	mapperName := b.clientID + "-audience"

	// Desired mapper config.
	mapperConfig := map[string]string{
		"included.custom.audience": b.clientID,
		"id.token.claim":           "false",
		"access.token.claim":       "true",
	}

	// Check if it already exists; if so, update it in place.
	for _, m := range mappers {
		if name, _ := m["name"].(string); name == mapperName {
			mapperID, _ := m["id"].(string)
			if mapperID == "" {
				b.logf("audience mapper %q exists but has no id — recreating", mapperName)
				break
			}

			// Update existing mapper to ensure config is correct.
			updated := map[string]any{
				"id":             mapperID,
				"name":           mapperName,
				"protocol":       "openid-connect",
				"protocolMapper": "oidc-audience-mapper",
				"config":         mapperConfig,
			}
			putStatus, putBody, putErr := b.callJSON(ctx, http.MethodPut, mappersPath+"/"+mapperID, updated)
			if putErr != nil {
				return putErr
			}
			if err := ensureCallStatus(putStatus, putBody, http.StatusNoContent, "update audience mapper"); err != nil {
				return err
			}
			b.logf("updated audience mapper %q for client %q", mapperName, b.clientID)
			return nil
		}
	}

	// Create an audience mapper so the access token includes our client ID in the "aud" claim.
	mapper := map[string]any{
		"name":           mapperName,
		"protocol":       "openid-connect",
		"protocolMapper": "oidc-audience-mapper",
		"config":         mapperConfig,
	}

	createStatus, createBody, err := b.callJSON(ctx, http.MethodPost, mappersPath, mapper)
	if err != nil {
		return err
	}
	if err := ensureCallStatus(createStatus, createBody, http.StatusCreated, "create audience mapper"); err != nil {
		return err
	}

	b.logf("created audience mapper %q for client %q", mapperName, b.clientID)
	return nil
}

func (b *keycloakBootstrapper) findClientUUID(ctx context.Context) (string, error) {
	q := url.Values{}
	q.Set("clientId", b.clientID)
	queryPath := "/admin/realms/" + b.realm + "/clients?" + q.Encode()

	status, body, err := b.callJSON(ctx, http.MethodGet, queryPath, nil)
	if err != nil {
		return "", err
	}
	if err := ensureCallStatus(status, body, http.StatusOK, fmt.Sprintf("query client %q", b.clientID)); err != nil {
		return "", err
	}

	var clients []keycloakClient
	if err := json.Unmarshal(body, &clients); err != nil {
		return "", err
	}
	if len(clients) == 0 {
		return "", nil
	}

	return clients[0].ID, nil
}

func (b *keycloakBootstrapper) ensureRole(ctx context.Context, roleName string) error {
	status, body, err := b.callJSON(ctx, http.MethodGet, "/admin/realms/"+b.realm+"/roles/"+url.PathEscape(roleName), nil)
	if err != nil {
		return err
	}
	if status == http.StatusOK {
		b.logf("role %q already exists", roleName)
		return nil
	}
	if err := ensureCallStatus(status, body, http.StatusNotFound, fmt.Sprintf("check role %q", roleName)); err != nil {
		return err
	}

	createStatus, createBody, err := b.callJSON(ctx, http.MethodPost, "/admin/realms/"+b.realm+"/roles", map[string]any{"name": roleName})
	if err != nil {
		return err
	}
	if err := ensureCallStatus(createStatus, createBody, http.StatusCreated, fmt.Sprintf("create role %q", roleName)); err != nil {
		return err
	}

	b.logf("created role %q", roleName)
	return nil
}

func (b *keycloakBootstrapper) callJSON(ctx context.Context, method string, path string, payload any) (int, []byte, error) {
	var requestBody []byte
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return 0, nil, err
		}
		requestBody = data
	}

	contentType := ""
	if payload != nil {
		contentType = "application/json"
	}

	resp, err := b.doRequest(ctx, method, b.baseURL+path, b.token, contentType, requestBody)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	respBody, readErr := readBoundedResponseBody(resp.Body)
	if readErr != nil {
		return resp.StatusCode, nil, fmt.Errorf("failed to read response body: %w", readErr)
	}

	return resp.StatusCode, respBody, nil
}

func (b *keycloakBootstrapper) doRequest(ctx context.Context, method string, endpoint string, token string, contentType string, body []byte) (*http.Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	attempts := b.retryAttempts
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

		req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
		if err != nil {
			return nil, err
		}

		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}

		resp, err := b.httpClient.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}

			if !isRetryableError(err) || attempt == attempts-1 {
				if isRetryableError(err) {
					return nil, NewServiceUnavailableError(operation, err)
				}
				return nil, err
			}

			lastErr = err
			if err := sleepWithContext(ctx, b.backoffDelay(attempt)); err != nil {
				return nil, err
			}
			continue
		}

		if !isRetryableStatus(resp.StatusCode) {
			return resp, nil
		}

		lastStatus = resp.StatusCode
		_ = resp.Body.Close()

		if attempt == attempts-1 {
			return nil, NewServiceUnavailableError(operation, fmt.Errorf("received status %d", lastStatus))
		}

		if err := sleepWithContext(ctx, b.backoffDelay(attempt)); err != nil {
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

func (b *keycloakBootstrapper) backoffDelay(attempt int) time.Duration {
	base := b.retryBaseDelay
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

func (b *keycloakBootstrapper) logf(format string, args ...any) {
	if b.output == nil {
		return
	}

	_, _ = fmt.Fprintf(b.output, format+"\n", args...)
}
