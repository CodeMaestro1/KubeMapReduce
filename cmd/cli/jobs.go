package main

import (
	"flag"
	"io"
	"log"
	"net/http"
	"os"
)

// ── jobs submit ────────────────────────────────────────────

func cmdJobsSubmit(args []string) {
	fs := flag.NewFlagSet("jobs submit", flag.ExitOnError)
	_ = fs.Parse(args)

	if fs.NArg() < 1 {
		log.Fatal("usage: kubemapreduce jobs submit <file.json>  (use \"-\" for stdin)")
	}

	filename := fs.Arg(0)
	var data []byte
	var err error

	if filename == "-" {
		data, err = io.ReadAll(os.Stdin)
	} else {
		data, err = os.ReadFile(filename)
	}
	if err != nil {
		log.Fatalf("failed to read input: %v", err)
	}

	token, serverURL := getValidToken()
	resp, err := doAuthRequest(http.MethodPost, serverURL+"/jobs", token, data)
	if err != nil {
		log.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		log.Fatalf("job submission failed (HTTP %d): %s", resp.StatusCode, string(body))
	}

	printResponse(resp)
}
