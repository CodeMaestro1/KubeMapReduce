package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
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

// ── jobs list ──────────────────────────────────────────────

func cmdJobsList() {
	token, serverURL := getValidToken()
	resp := doAuthRequestExpect(
		http.MethodGet,
		serverURL+"/jobs",
		token,
		nil,
		http.StatusOK,
		"jobs list failed",
	)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("failed to read response: %v", err)
	}

	var jobs []struct {
		JobID     string    `json:"jobId"`
		Status    string    `json:"status"`
		Filename  string    `json:"filename"`
		CreatedAt time.Time `json:"createdAt"`
	}
	if err := json.Unmarshal(body, &jobs); err != nil {
		fmt.Print(string(body))
		return
	}

	if len(jobs) == 0 {
		fmt.Println("No jobs found.")
		return
	}

	fmt.Printf("%-26s  %-12s  %-30s  %s\n", "JOB ID", "STATUS", "FILENAME", "CREATED AT")
	fmt.Printf("%-26s  %-12s  %-30s  %s\n", "------", "------", "--------", "----------")
	for _, j := range jobs {
		fmt.Printf("%-26s  %-12s  %-30s  %s\n",
			j.JobID, j.Status, j.Filename, j.CreatedAt.Local().Format("2006-01-02 15:04:05"))
	}
}

// ── jobs status ────────────────────────────────────────────

func cmdJobsStatus(jobID string) {
	token, serverURL := getValidToken()
	resp := doAuthRequestExpect(
		http.MethodGet,
		serverURL+"/jobs/"+jobID,
		token,
		nil,
		http.StatusOK,
		"job status failed",
	)
	defer resp.Body.Close()

	printResponse(resp)
}
