package httputil

import (
	"encoding/json"
	"net/http"
)

const contentTypeJSON = "application/json"

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
