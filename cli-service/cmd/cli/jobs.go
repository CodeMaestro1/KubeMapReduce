package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ── jobs submit ────────────────────────────────────────────

type cliJobFuncSpec struct {
	Language   string `json:"language"`
	Artifact   string `json:"artifact"`
	Entrypoint string `json:"entrypoint"`
	Interface  string `json:"interface"`
}

type cliJobPayload struct {
	Filename string          `json:"filename"`
	Mapper   cliJobFuncSpec  `json:"mapper"`
	Reducer  cliJobFuncSpec  `json:"reducer"`
	Combiner *cliJobFuncSpec `json:"combiner,omitempty"`
	Reducers int             `json:"reducers,omitempty"`
}

var jobsSubmitGetValidToken = getValidToken
var jobsSubmitDoAuthRequestExpect = doAuthRequestExpect
var jobsSubmitExit = os.Exit
var jobsListGetValidToken = getValidToken
var jobsListDoAuthRequestExpect = doAuthRequestExpect
var jobsListExit = os.Exit

func validateReducersCount(reducers int) error {
	if reducers < 1 {
		return fmt.Errorf("--reducers must be > 0")
	}

	return nil
}

func inferLanguage(path string) string {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".py":
		return "python"
	case ".java", ".jar":
		return "java"
	case ".c":
		return "c"
	case ".cpp", ".cc", ".cxx":
		return "cpp"
	default:
		log.Fatalf("cannot infer language from extension %q; supported: .py, .java, .jar, .c, .cpp", filepath.Ext(path))
		return ""
	}
}

// cmdJobsSubmit handles the 'jobs submit' command.
//
// It parses job specification flags (mapper, reducer, input, etc.) and constructs
// a JSON payload to send to the API server. This command is designed to be
// metadata-only: it tells the system where to find the code and data, but does
// not upload the files themselves, assuming a shared storage environment or
// pre-staged artifacts.
func cmdJobsSubmit(args []string) {
	fs := flag.NewFlagSet("jobs submit", flag.ExitOnError)
	mapperPath := fs.String("mapper", "", "path to mapper file (required)")
	reducerPath := fs.String("reducer", "", "path to reducer file (required)")
	combinerPath := fs.String("combiner", "", "path to combiner file (optional)")
	inputFile := fs.String("input", "", "path to input data file (required)")
	numReducers := fs.Int("reducers", 1, "number of reducers (default: 1)")
	specFile := fs.String("spec", "", "path to raw JSON spec file (advanced; overrides other flags)")
	_ = fs.Parse(args)

	var data []byte
	if *specFile != "" {
		var err error
		data, err = os.ReadFile(*specFile)
		if err != nil {
			log.Fatalf("failed to read spec file: %v", err)
		}
	} else {
		if *mapperPath == "" || *reducerPath == "" || *inputFile == "" {
			fmt.Fprintln(os.Stderr, "usage: kubemapreduce jobs submit --mapper <file> --reducer <file> --input <file> [--combiner <file>] [--reducers N]")
			jobsSubmitExit(1)
			return
		}
		if err := validateReducersCount(*numReducers); err != nil {
			fmt.Fprintln(os.Stderr, "usage: kubemapreduce jobs submit --mapper <file> --reducer <file> --input <file> [--combiner <file>] [--reducers N]")
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			jobsSubmitExit(1)
			return
		}
		payload := cliJobPayload{
			Filename: filepath.Base(*inputFile),
			Mapper: cliJobFuncSpec{
				Language:   inferLanguage(*mapperPath),
				Artifact:   filepath.Base(*mapperPath),
				Entrypoint: "map",
				Interface:  "map(key,value)->[]KeyValue",
			},
			Reducer: cliJobFuncSpec{
				Language:   inferLanguage(*reducerPath),
				Artifact:   filepath.Base(*reducerPath),
				Entrypoint: "reduce",
				Interface:  "reduce(key,values)->Value",
			},
			Reducers: *numReducers,
		}
		if *combinerPath != "" {
			payload.Combiner = &cliJobFuncSpec{
				Language:   inferLanguage(*combinerPath),
				Artifact:   filepath.Base(*combinerPath),
				Entrypoint: "combine",
				Interface:  "reduce(key,values)->Value",
			}
		}
		var err error
		data, err = json.Marshal(payload)
		if err != nil {
			log.Fatalf("failed to build job request: %v", err)
		}
	}

	token, serverURL := jobsSubmitGetValidToken()
	resp := jobsSubmitDoAuthRequestExpect(
		http.MethodPost,
		serverURL+"/jobs",
		token,
		data,
		http.StatusAccepted,
		"job submission failed",
	)
	defer resp.Body.Close()

	printResponse(resp)
	fmt.Println("\nNote: this submits job metadata only. No file data is transferred to the server.")
}

// ── jobs list ──────────────────────────────────────────────

// cmdJobsList fetches and displays a summary of all jobs in the system.
//
// This command provides a tabular view of job IDs, statuses, and creation times.
// It allows users to quickly monitor the overall state of the MapReduce cluster
// and identify jobs that require further investigation.
func cmdJobsList() {
	token, serverURL := jobsListGetValidToken()
	resp := jobsListDoAuthRequestExpect(
		http.MethodGet,
		serverURL+"/jobs",
		token,
		nil,
		http.StatusOK,
		"jobs list failed",
	)
	defer resp.Body.Close()

	body, err := readResponseBody(resp.Body)
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
		fmt.Fprintln(os.Stderr, "kubemapreduce: jobs list returned an unexpected response shape")
		fmt.Fprintf(os.Stderr, "kubemapreduce: failed to decode jobs list response: %v\n", err)
		if len(body) > 0 {
			fmt.Fprintln(os.Stderr, "kubemapreduce: raw response:")
			fmt.Fprintln(os.Stderr, string(body))
		}
		jobsListExit(1)
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

// ── jobs download ─────────────────────────────────────────

// cmdJobsDownload retrieves the output results for a completed job.
//
// Results are saved as a JSON file in the specified output directory. This
// command interacts with the API's results endpoint, which aggregates or streams
// data from the underlying storage (e.g., MinIO) back to the user's local machine.
func cmdJobsDownload(args []string) {
	fs := flag.NewFlagSet("jobs download", flag.ExitOnError)
	jobID := fs.String("id", "", "job ID whose results to download (required)")
	outputDir := fs.String("output", "./results/", "directory to save result file (default: ./results/)")
	_ = fs.Parse(args)

	if *jobID == "" {
		fmt.Fprintln(os.Stderr, "usage: kubemapreduce jobs download --id <job-id> [--output ./results/]")
		os.Exit(1)
	}

	normalizedJobID := strings.TrimSpace(*jobID)
	if normalizedJobID == "" {
		fmt.Fprintln(os.Stderr, "usage: kubemapreduce jobs download --id <job-id> [--output ./results/]")
		os.Exit(1)
	}

	token, serverURL := getValidToken()
	resp, err := doAuthRequest(http.MethodGet, serverURL+jobRequestPath(normalizedJobID, "/results"), token, nil)
	if err != nil {
		log.Fatalf("job download failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotImplemented {
		fmt.Fprintf(os.Stderr, "kubemapreduce: result download not yet available (501)\n")
		printResponse(resp)
		return
	}
	if resp.StatusCode != http.StatusOK {
		log.Fatalf("job download failed: server returned %s", resp.Status)
	}

	if err := os.MkdirAll(*outputDir, 0o750); err != nil {
		log.Fatalf("failed to create output directory: %v", err)
	}

	outPath := filepath.Join(*outputDir, safeJobResultFilename(normalizedJobID))
	f, err := os.OpenFile(outPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640)
	if err != nil {
		log.Fatalf("failed to create output file: %v", err)
	}
	defer f.Close()

	bytesWritten, err := io.Copy(f, resp.Body)
	if err != nil {
		log.Fatalf("failed to write results: %v", err)
	}
	fmt.Printf("results saved to %s (%d bytes)\n", outPath, bytesWritten)
}

// ── jobs status ────────────────────────────────────────────

// cmdJobsStatus displays detailed status information for a specific job.
//
// It returns a JSON object containing the current phase (e.g., Mapping, Reducing),
// task completion counts, and any error messages if the job failed. This is the
// primary tool for debugging job execution issues.
func cmdJobsStatus(args []string) {
	fs := flag.NewFlagSet("jobs status", flag.ExitOnError)
	jobID := fs.String("id", "", "job ID to query (required)")
	_ = fs.Parse(args)

	if *jobID == "" {
		fmt.Fprintln(os.Stderr, "usage: kubemapreduce jobs status --id <job-id>")
		os.Exit(1)
	}

	normalizedJobID := strings.TrimSpace(*jobID)
	if normalizedJobID == "" {
		fmt.Fprintln(os.Stderr, "usage: kubemapreduce jobs status --id <job-id>")
		os.Exit(1)
	}

	token, serverURL := getValidToken()
	resp := doAuthRequestExpect(
		http.MethodGet,
		serverURL+jobRequestPath(normalizedJobID, ""),
		token,
		nil,
		http.StatusOK,
		"job status failed",
	)
	defer resp.Body.Close()

	printResponse(resp)
}
	fs := flag.NewFlagSet("jobs status", flag.ExitOnError)
	jobID := fs.String("id", "", "job ID to query (required)")
	_ = fs.Parse(args)

	if *jobID == "" {
		fmt.Fprintln(os.Stderr, "usage: kubemapreduce jobs status --id <job-id>")
		os.Exit(1)
	}

	normalizedJobID := strings.TrimSpace(*jobID)
	if normalizedJobID == "" {
		fmt.Fprintln(os.Stderr, "usage: kubemapreduce jobs status --id <job-id>")
		os.Exit(1)
	}

	token, serverURL := getValidToken()
	resp := doAuthRequestExpect(
		http.MethodGet,
		serverURL+jobRequestPath(normalizedJobID, ""),
		token,
		nil,
		http.StatusOK,
		"job status failed",
	)
	defer resp.Body.Close()

	printResponse(resp)
}
