package httputil

import (
	"encoding/json"
	"net/http"
)

func WriteJSON(w http.ResponseWriter, status int, payload interface{}) error {
	body, err := json.Marshal(payload)
	if err != nil {
		WriteError(w, http.StatusInternalServerError, "failed to encode response")
		return err
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if _, err := w.Write(append(body, '\n')); err != nil {
		return err
	}

	return nil
}

