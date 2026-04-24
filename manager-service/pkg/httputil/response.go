package httputil

import (
	"encoding/json"
	"net/http"
)

const contentTypeJSON = "application/json"

// WriteJSON serializes a payload to JSON and writes it to the response with a status code.
//
// JSON is chosen as the standard exchange format for the UI Service because
// of its ubiquity in modern web clients and its ease of debugging.
// This function also appends a newline to the output, making it compatible
// with CLI tools like 'curl' or 'jq'.
func WriteJSON(w http.ResponseWriter, status int, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		WriteError(w, http.StatusInternalServerError, "failed to encode response")
		return err
	}

	w.Header().Set("Content-Type", contentTypeJSON)
	w.WriteHeader(status)

	_, err = w.Write(append(body, '\n'))
	return err
}

// WriteErrorJSON sends a structured JSON error response.
func WriteErrorJSON(w http.ResponseWriter, status int, message string) {
	_ = WriteJSON(w, status, map[string]any{
		"error": message,
		"code":  status,
	})
}

// WriteError sends a plain-text error message with the specified HTTP status code.
func WriteError(w http.ResponseWriter, status int, message string) {
	http.Error(w, message, status)
}
