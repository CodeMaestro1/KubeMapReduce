package main

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type PresignRequest struct {
	Key   string `json:"key,omitempty"`
	JobID string `json:"job_id,omitempty"`
}

func (r *PresignRequest) UnmarshalJSON(data []byte) error {
	type Alias PresignRequest
	var tmp struct {
		Alias
		Bucket *string `json:"bucket,omitempty"`
	}
	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}
	*r = PresignRequest(tmp.Alias)
	return nil
}

func main() {
	data := []byte(`{"bucket": "foo", "key": "bar"}`)
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var req PresignRequest
	err := dec.Decode(&req)
	fmt.Println("Error:", err)
	fmt.Println("Req:", req)
}
