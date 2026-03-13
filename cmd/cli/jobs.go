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
	resp := doAuthRequestExpect(
		http.MethodPost,
		serverURL+"/jobs",
		token,
		data,
		http.StatusAccepted,
		"job submission failed",
	)
	defer resp.Body.Close()

	printResponse(resp)
}
