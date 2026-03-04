package auth

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type KeycloakAdminClient struct {
	baseURL     string
	targetRealm string
	adminRealm  string
	adminUser   string
	adminPass   string
	httpClient  *http.Client
	tokenClient string
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

func NewKeycloakAdminClient(baseURL string, targetRealm string, adminUser string, adminPass string) *KeycloakAdminClient {
	return &KeycloakAdminClient{
		baseURL:     strings.TrimRight(baseURL, "/"),
		targetRealm: targetRealm,
		adminRealm:  "master",
		adminUser:   adminUser,
		adminPass:   adminPass,
		tokenClient: "admin-cli",
		httpClient:  &http.Client{},
	}
}

func (c *KeycloakAdminClient) CreateUser(req CreateUserRequest) error {
	if req.Username == "" || req.Password == "" || req.Role == "" {
		return fmt.Errorf("username, password and role are required")
	}

	token, err := c.getAdminAccessToken()
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
	httpReq, err := http.NewRequest(http.MethodPost, createURL, bytes.NewReader(payload))
	if err != nil {
		return err
	}

	httpReq.Header.Set("Authorization", "Bearer "+token)
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(httpResp.Body)
		return fmt.Errorf("failed to create user: status %d: %s", httpResp.StatusCode, string(body))
	}

	userID, err := extractUserIDFromLocation(httpResp.Header.Get("Location"))
	if err != nil {
		return err
	}

	if err := c.setUserPassword(token, userID, req.Password); err != nil {
		return err
	}

	if err := c.assignRealmRole(token, userID, req.Role); err != nil {
		return err
	}

	return nil
}

func (c *KeycloakAdminClient) DeleteUserByUsername(username string) error {
	if username == "" {
		return fmt.Errorf("username is required")
	}

	token, err := c.getAdminAccessToken()
	if err != nil {
		return err
	}

	userID, err := c.findUserID(token, username)
	if err != nil {
		return err
	}

	deleteURL := fmt.Sprintf("%s/admin/realms/%s/users/%s", c.baseURL, c.targetRealm, userID)
	httpReq, err := http.NewRequest(http.MethodDelete, deleteURL, nil)
	if err != nil {
		return err
	}

	httpReq.Header.Set("Authorization", "Bearer "+token)
	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(httpResp.Body)
		return fmt.Errorf("failed to delete user: status %d: %s", httpResp.StatusCode, string(body))
	}

	return nil
}

func (c *KeycloakAdminClient) getAdminAccessToken() (string, error) {
	if c.adminUser == "" || c.adminPass == "" {
		return "", fmt.Errorf("keycloak admin credentials are missing")
	}

	form := url.Values{}
	form.Set("grant_type", "password")
	form.Set("client_id", c.tokenClient)
	form.Set("username", c.adminUser)
	form.Set("password", c.adminPass)

	tokenURL := fmt.Sprintf("%s/realms/%s/protocol/openid-connect/token", c.baseURL, c.adminRealm)
	httpResp, err := c.httpClient.Post(tokenURL, "application/x-www-form-urlencoded", strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(httpResp.Body)
		return "", fmt.Errorf("failed to get admin token: status %d: %s", httpResp.StatusCode, string(body))
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

func (c *KeycloakAdminClient) setUserPassword(token string, userID string, password string) error {
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

	httpReq, err := http.NewRequest(http.MethodPut, resetURL, bytes.NewReader(data))
	if err != nil {
		return err
	}

	httpReq.Header.Set("Authorization", "Bearer "+token)
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(httpResp.Body)
		return fmt.Errorf("failed to set password: status %d: %s", httpResp.StatusCode, string(body))
	}

	return nil
}

func (c *KeycloakAdminClient) assignRealmRole(token string, userID string, roleName string) error {
	roleName = strings.ToUpper(strings.TrimSpace(roleName))
	if roleName == "" {
		return fmt.Errorf("role name is required")
	}

	roleURL := fmt.Sprintf("%s/admin/realms/%s/roles/%s", c.baseURL, c.targetRealm, url.PathEscape(roleName))
	httpReq, err := http.NewRequest(http.MethodGet, roleURL, nil)
	if err != nil {
		return err
	}

	httpReq.Header.Set("Authorization", "Bearer "+token)
	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(httpResp.Body)
		return fmt.Errorf("failed to fetch role %s: status %d: %s", roleName, httpResp.StatusCode, string(body))
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

	assignReq, err := http.NewRequest(http.MethodPost, assignURL, bytes.NewReader(payload))
	if err != nil {
		return err
	}

	assignReq.Header.Set("Authorization", "Bearer "+token)
	assignReq.Header.Set("Content-Type", "application/json")

	assignResp, err := c.httpClient.Do(assignReq)
	if err != nil {
		return err
	}
	defer assignResp.Body.Close()

	if assignResp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(assignResp.Body)
		return fmt.Errorf("failed to assign role: status %d: %s", assignResp.StatusCode, string(body))
	}

	return nil
}

func (c *KeycloakAdminClient) findUserID(token string, username string) (string, error) {
	searchURL := fmt.Sprintf("%s/admin/realms/%s/users?username=%s&exact=true", c.baseURL, c.targetRealm, url.QueryEscape(username))
	httpReq, err := http.NewRequest(http.MethodGet, searchURL, nil)
	if err != nil {
		return "", err
	}

	httpReq.Header.Set("Authorization", "Bearer "+token)
	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return "", err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(httpResp.Body)
		return "", fmt.Errorf("failed to search user: status %d: %s", httpResp.StatusCode, string(body))
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
