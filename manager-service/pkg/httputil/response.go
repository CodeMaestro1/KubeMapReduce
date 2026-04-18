package httputil

import (
	"encoding/json"
	"errors"
	"net/http"
)

const contentTypeJSON = "application/json"

// DefaultMaxBodyBytes is the default maximum request body size (1 MB).
const DefaultMaxBodyBytes int64 = 1 << 20

// DecodeJSONBody reads a JSON request body with an enforced size limit.
// If limit <= 0, DefaultMaxBodyBytes is used.
// Returns a 413 error when the body exceeds the limit and a 400 error for
// malformed JSON.
func DecodeJSONBody(w http.ResponseWriter, r *http.Request, dst any, limit int64) error {
	if limit <= 0 {
		limit = DefaultMaxBodyBytes
	}
	r.Body = http.MaxBytesReader(w, r.Body, limit)

	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return err
		}
		http.Error(w, "invalid request payload", http.StatusBadRequest)
		return err
	}
	return nil
}

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

func WriteError(w http.ResponseWriter, status int, message string) {
	http.Error(w, message, status)
}
