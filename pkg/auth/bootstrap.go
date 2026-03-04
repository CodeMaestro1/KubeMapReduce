package auth

import (
	"bytes"
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
	bootstrapper, err := newKeycloakBootstrapper(cfg, output)
	if err != nil {
		return err
	}

	return bootstrapper.bootstrap()
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

func (b *keycloakBootstrapper) bootstrap() error {
	token, err := b.getAdminToken()
	if err != nil {
		return err
	}
	b.token = token

	if err := b.ensureRealm(); err != nil {
		return err
	}
	if err := b.ensureRealmSettings(); err != nil {
		return err
	}
	if err := b.ensureClient(); err != nil {
		return err
	}
	if err := b.ensureRole("USER"); err != nil {
		return err
	}
	if err := b.ensureRole("ADMIN"); err != nil {
		return err
	}

	return nil
}

func (b *keycloakBootstrapper) getAdminToken() (string, error) {
	form := url.Values{}
	form.Set("grant_type", "password")
	form.Set("client_id", "admin-cli")
	form.Set("username", b.adminUsername)
	form.Set("password", b.adminPassword)

	resp, err := b.httpClient.Post(
		b.baseURL+"/realms/master/protocol/openid-connect/token",
		"application/x-www-form-urlencoded",
		strings.NewReader(form.Encode()),
	)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("failed to get admin token: status %d: %s", resp.StatusCode, string(body))
	}

	var tr tokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		return "", err
	}
	if tr.AccessToken == "" {
		return "", fmt.Errorf("admin token missing in response")
	}

	return tr.AccessToken, nil
}

func (b *keycloakBootstrapper) ensureRealm() error {
	status, body, err := b.callJSON(http.MethodGet, "/admin/realms/"+b.realm, nil)
	if err != nil {
		return err
	}
	if status == http.StatusOK {
		b.logf("realm %q already exists", b.realm)
		return nil
	}
	if status != http.StatusNotFound {
		return fmt.Errorf("failed checking realm %q: status %d: %s", b.realm, status, string(body))
	}

	payload := map[string]any{"realm": b.realm, "enabled": true}
	createStatus, createBody, err := b.callJSON(http.MethodPost, "/admin/realms", payload)
	if err != nil {
		return err
	}
	if createStatus != http.StatusCreated {
		return fmt.Errorf("failed creating realm %q: status %d: %s", b.realm, createStatus, string(createBody))
	}

	b.logf("created realm %q", b.realm)
	return nil
}

func (b *keycloakBootstrapper) ensureRealmSettings() error {
	status, body, err := b.callJSON(http.MethodGet, "/admin/realms/"+b.realm, nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("failed reading realm settings: status %d: %s", status, string(body))
	}

	var cfg map[string]any
	if err := json.Unmarshal(body, &cfg); err != nil {
		return err
	}

	cfg["registrationAllowed"] = b.enableRegistration
	cfg["loginWithEmailAllowed"] = true

	putStatus, putBody, err := b.callJSON(http.MethodPut, "/admin/realms/"+b.realm, cfg)
	if err != nil {
		return err
	}
	if putStatus != http.StatusNoContent {
		return fmt.Errorf("failed updating realm settings: status %d: %s", putStatus, string(putBody))
	}

	b.logf("ensured realm settings (registration=%t)", b.enableRegistration)
	return nil
}

func (b *keycloakBootstrapper) ensureClient() error {
	clientUUID, err := b.findClientUUID()
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

		status, createBody, err := b.callJSON(http.MethodPost, "/admin/realms/"+b.realm+"/clients", payload)
		if err != nil {
			return err
		}
		if status != http.StatusCreated {
			return fmt.Errorf("failed creating client %q: status %d: %s", b.clientID, status, string(createBody))
		}
		b.logf("created client %q", b.clientID)

		clientUUID, err = b.findClientUUID()
		if err != nil {
			return err
		}
		if clientUUID == "" {
			return fmt.Errorf("client %q created but id lookup failed", b.clientID)
		}
	} else {
		b.logf("client %q already exists", b.clientID)
	}

	status, body, err := b.callJSON(http.MethodGet, "/admin/realms/"+b.realm+"/clients/"+clientUUID, nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("failed reading client %q: status %d: %s", b.clientID, status, string(body))
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

	putStatus, putBody, err := b.callJSON(http.MethodPut, "/admin/realms/"+b.realm+"/clients/"+clientUUID, cfg)
	if err != nil {
		return err
	}
	if putStatus != http.StatusNoContent {
		return fmt.Errorf("failed updating client %q: status %d: %s", b.clientID, putStatus, string(putBody))
	}

	b.logf("ensured client redirects/web-origins for %q", b.clientID)
	return nil
}

func (b *keycloakBootstrapper) findClientUUID() (string, error) {
	status, body, err := b.callJSON(http.MethodGet, "/admin/realms/"+b.realm+"/clients?clientId="+url.QueryEscape(b.clientID), nil)
	if err != nil {
		return "", err
	}
	if status != http.StatusOK {
		return "", fmt.Errorf("failed querying client %q: status %d: %s", b.clientID, status, string(body))
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

func (b *keycloakBootstrapper) ensureRole(roleName string) error {
	status, body, err := b.callJSON(http.MethodGet, "/admin/realms/"+b.realm+"/roles/"+url.PathEscape(roleName), nil)
	if err != nil {
		return err
	}
	if status == http.StatusOK {
		b.logf("role %q already exists", roleName)
		return nil
	}
	if status != http.StatusNotFound {
		return fmt.Errorf("failed checking role %q: status %d: %s", roleName, status, string(body))
	}

	createStatus, createBody, err := b.callJSON(http.MethodPost, "/admin/realms/"+b.realm+"/roles", map[string]any{"name": roleName})
	if err != nil {
		return err
	}
	if createStatus != http.StatusCreated {
		return fmt.Errorf("failed creating role %q: status %d: %s", roleName, createStatus, string(createBody))
	}

	b.logf("created role %q", roleName)
	return nil
}

func (b *keycloakBootstrapper) callJSON(method string, path string, payload any) (int, []byte, error) {
	var bodyReader io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return 0, nil, err
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, b.baseURL+path, bodyReader)
	if err != nil {
		return 0, nil, err
	}
	if b.token != "" {
		req.Header.Set("Authorization", "Bearer "+b.token)
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	respBody, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return resp.StatusCode, nil, readErr
	}

	return resp.StatusCode, respBody, nil
}

func (b *keycloakBootstrapper) logf(format string, args ...any) {
	if b.output == nil {
		return
	}

	_, _ = fmt.Fprintf(b.output, format+"\n", args...)
}
