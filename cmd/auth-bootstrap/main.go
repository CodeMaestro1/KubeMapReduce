package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
)

type tokenResponse struct {
	AccessToken string `json:"access_token"`
}

type keycloakClient struct {
	ID string `json:"id"`
}

type bootstrapper struct {
	baseURL            string
	realm              string
	clientID           string
	uiOrigin           string
	adminUsername      string
	adminPassword      string
	enableRegistration bool
	httpClient         *http.Client
	token              string
}

func main() {
	b := &bootstrapper{httpClient: &http.Client{}}

	flag.StringVar(&b.baseURL, "keycloak-base-url", getEnv("KEYCLOAK_BASE_URL", "http://localhost:8080"), "Keycloak base URL")
	flag.StringVar(&b.realm, "realm", getEnv("KEYCLOAK_REALM", "mapreduce"), "Target Keycloak realm")
	flag.StringVar(&b.clientID, "client-id", getEnv("KEYCLOAK_AUDIENCE", "mapreduce-api"), "OIDC client id")
	flag.StringVar(&b.uiOrigin, "ui-origin", "http://localhost:8081", "UI origin used for redirect URIs and web origins")
	flag.StringVar(&b.adminUsername, "admin-username", getEnv("KEYCLOAK_ADMIN_USERNAME", "admin"), "Keycloak admin username (master realm)")
	flag.StringVar(&b.adminPassword, "admin-password", getEnv("KEYCLOAK_ADMIN_PASSWORD", "admin"), "Keycloak admin password (master realm)")
	flag.BoolVar(&b.enableRegistration, "enable-registration", true, "Enable self-registration in realm")

	flag.Parse()

	b.baseURL = strings.TrimRight(strings.TrimSpace(b.baseURL), "/")
	b.realm = strings.TrimSpace(b.realm)
	b.clientID = strings.TrimSpace(b.clientID)
	b.uiOrigin = strings.TrimRight(strings.TrimSpace(b.uiOrigin), "/")
	b.adminUsername = strings.TrimSpace(b.adminUsername)
	b.adminPassword = strings.TrimSpace(b.adminPassword)

	if b.baseURL == "" || b.realm == "" || b.clientID == "" || b.uiOrigin == "" {
		log.Fatal("keycloak-base-url, realm, client-id, and ui-origin are required")
	}
	if b.adminUsername == "" || b.adminPassword == "" {
		log.Fatal("admin credentials are required (--admin-username and --admin-password)")
	}

	if err := b.bootstrap(); err != nil {
		log.Fatalf("auth bootstrap failed: %v", err)
	}

	fmt.Println("auth bootstrap completed")
	fmt.Println("note: no users were created by this command")
}

func (b *bootstrapper) bootstrap() error {
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

func (b *bootstrapper) getAdminToken() (string, error) {
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

func (b *bootstrapper) ensureRealm() error {
	status, body, err := b.callJSON(http.MethodGet, "/admin/realms/"+b.realm, nil)
	if err != nil {
		return err
	}
	if status == http.StatusOK {
		fmt.Printf("realm %q already exists\n", b.realm)
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

	fmt.Printf("created realm %q\n", b.realm)
	return nil
}

func (b *bootstrapper) ensureRealmSettings() error {
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

	fmt.Printf("ensured realm settings (registration=%t)\n", b.enableRegistration)
	return nil
}

func (b *bootstrapper) ensureClient() error {
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
		fmt.Printf("created client %q\n", b.clientID)

		clientUUID, err = b.findClientUUID()
		if err != nil {
			return err
		}
		if clientUUID == "" {
			return fmt.Errorf("client %q created but id lookup failed", b.clientID)
		}
	} else {
		fmt.Printf("client %q already exists\n", b.clientID)
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

	fmt.Printf("ensured client redirects/web-origins for %q\n", b.clientID)
	return nil
}

func (b *bootstrapper) findClientUUID() (string, error) {
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

func (b *bootstrapper) ensureRole(roleName string) error {
	status, body, err := b.callJSON(http.MethodGet, "/admin/realms/"+b.realm+"/roles/"+url.PathEscape(roleName), nil)
	if err != nil {
		return err
	}
	if status == http.StatusOK {
		fmt.Printf("role %q already exists\n", roleName)
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

	fmt.Printf("created role %q\n", roleName)
	return nil
}

func (b *bootstrapper) callJSON(method string, path string, payload any) (int, []byte, error) {
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

func getEnv(key string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}
