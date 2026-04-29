package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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

const (
	inputBucketName = "inputs"
	codeBucketName  = "code"
)

type cliJobFuncSpec struct {
	Language   string `json:"language"`
	Artifact   string `json:"artifact"`
	Entrypoint string `json:"entrypoint"`
	Interface  string `json:"interface"`
}

type cliJobPayload struct {
	Filename      string          `json:"filename"`
	Mapper        cliJobFuncSpec  `json:"mapper"`
	Reducer       cliJobFuncSpec  `json:"reducer"`
	Combiner      *cliJobFuncSpec `json:"combiner,omitempty"`
	Reducers      int             `json:"reducers,omitempty"`
	InputChecksum string          `json:"inputChecksum,omitempty"`
}

var jobsSubmitGetValidToken = getValidToken
var jobsSubmitDoAuthRequest = doAuthRequest
var jobsSubmitDoAuthRequestExpect = doAuthRequestExpect
var jobsSubmitHTTPClient = cliHTTPClient
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
// It parses job specification flags (mapper, reducer, input, etc.), uploads
// the referenced files to object storage, and constructs the JSON payload sent
// to the API server with MinIO URIs and the input checksum.
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
	var token, serverURL string
	if *specFile != "" {
		var err error
		data, err = os.ReadFile(*specFile)
		if err != nil {
			log.Fatalf("failed to read spec file: %v", err)
		}
		token, serverURL = jobsSubmitGetValidToken()
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
		token, serverURL = jobsSubmitGetValidToken()
		uploadPrefix := fmt.Sprintf("submission-%d", time.Now().UTC().UnixNano())

		inputArtifact, err := presignAndUploadFile(context.Background(), token, serverURL, inputBucketName, buildUploadObjectKey(uploadPrefix, "input", *inputFile), *inputFile)
		if err != nil {
			log.Fatalf("failed to upload input file: %v", err)
		}

		mapperArtifact, err := presignAndUploadFile(context.Background(), token, serverURL, codeBucketName, buildUploadObjectKey(uploadPrefix, "mapper", *mapperPath), *mapperPath)
		if err != nil {
			log.Fatalf("failed to upload mapper file: %v", err)
		}

		reducerArtifact, err := presignAndUploadFile(context.Background(), token, serverURL, codeBucketName, buildUploadObjectKey(uploadPrefix, "reducer", *reducerPath), *reducerPath)
		if err != nil {
			log.Fatalf("failed to upload reducer file: %v", err)
		}

		payload := cliJobPayload{
			Filename: inputArtifact.URI,
			Mapper: cliJobFuncSpec{
				Language:   inferLanguage(*mapperPath),
				Artifact:   mapperArtifact.URI,
				Entrypoint: "map",
				Interface:  "map(key,value)->[]KeyValue",
			},
			Reducer: cliJobFuncSpec{
				Language:   inferLanguage(*reducerPath),
				Artifact:   reducerArtifact.URI,
				Entrypoint: "reduce",
				Interface:  "reduce(key,values)->Value",
			},
			Reducers:      *numReducers,
			InputChecksum: inputArtifact.Checksum,
		}
		if *combinerPath != "" {
			combinerArtifact, err := presignAndUploadFile(context.Background(), token, serverURL, codeBucketName, buildUploadObjectKey(uploadPrefix, "combiner", *combinerPath), *combinerPath)
			if err != nil {
				log.Fatalf("failed to upload combiner file: %v", err)
			}
			payload.Combiner = &cliJobFuncSpec{
				Language:   inferLanguage(*combinerPath),
				Artifact:   combinerArtifact.URI,
				Entrypoint: "combine",
				Interface:  "reduce(key,values)->Value",
			}
		}
		data, err = json.Marshal(payload)
		if err != nil {
			log.Fatalf("failed to build job request: %v", err)
		}
	}

	resp := jobsSubmitDoAuthRequestExpect(
		http.MethodPost,
		serverURL+"/api/v1/jobs",
		token,
		data,
		http.StatusAccepted,
		"job submission failed",
	)
	defer resp.Body.Close()

	printResponse(resp)
	fmt.Println("\nUploaded files to object storage and submitted the job specification.")
}

type uploadedArtifact struct {
	URI      string
	Checksum string
}

func presignAndUploadFile(ctx context.Context, token, serverURL, bucket, objectKey, localPath string) (uploadedArtifact, error) {
	data, err := os.ReadFile(localPath)
	if err != nil {
		return uploadedArtifact{}, err
	}

	checksum := sha256.Sum256(data)
	presignedURL, err := requestPresignedUploadURL(token, serverURL, bucket, objectKey)
	if err != nil {
		return uploadedArtifact{}, err
	}

	if err := uploadBytesToPresignedURL(ctx, presignedURL, data); err != nil {
		return uploadedArtifact{}, err
	}

	return uploadedArtifact{URI: fmt.Sprintf("s3://%s/%s", bucket, objectKey), Checksum: hex.EncodeToString(checksum[:])}, nil
}

func requestPresignedUploadURL(token, serverURL, bucket, objectKey string) (string, error) {
	body, err := json.Marshal(map[string]string{"bucket": bucket, "key": objectKey})
	if err != nil {
		return "", err
	}

	requestFunc := jobsSubmitDoAuthRequest
	if requestFunc == nil {
		requestFunc = doAuthRequest
	}
	resp, err := requestFunc(http.MethodPost, serverURL+"/api/v1/uploads/presigned", token, body)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, readErr := readResponseBody(resp.Body)
		if readErr != nil {
			return "", fmt.Errorf("presign upload request failed (HTTP %d): %v", resp.StatusCode, readErr)
		}
		return "", fmt.Errorf("presign upload request failed (HTTP %d): %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	var presignResp struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&presignResp); err != nil {
		return "", fmt.Errorf("decode presign response: %w", err)
	}
	if presignResp.URL == "" {
		return "", fmt.Errorf("presign upload response did not include a URL")
	}
	return presignResp.URL, nil
}

func uploadBytesToPresignedURL(ctx context.Context, rawURL string, data []byte) error {
	client := jobsSubmitHTTPClient
	if client == nil {
		client = cliHTTPClient
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, rawURL, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		respBody, readErr := readResponseBody(resp.Body)
		if readErr != nil {
			return fmt.Errorf("upload failed (HTTP %d): %v", resp.StatusCode, readErr)
		}
		return fmt.Errorf("upload failed (HTTP %d): %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	return nil
}

func buildUploadObjectKey(prefix, role, localPath string) string {
	return fmt.Sprintf("%s/%s-%s", prefix, role, filepath.Base(localPath))
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
		serverURL+"/api/v1/jobs",
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

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxCLIResponseBodyBytes))
		log.Fatalf("job download failed (%s): %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var dlResp struct {
		JobID string   `json:"jobId"`
		URLs  []string `json:"urls"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&dlResp); err != nil {
		log.Fatalf("failed to parse server response: %v", err)
	}
	resp.Body.Close()

	if len(dlResp.URLs) == 0 {
		fmt.Println("no output shards available for this job")
		return
	}

	if err := os.MkdirAll(*outputDir, 0o750); err != nil {
		log.Fatalf("failed to create output directory: %v", err)
	}

	type shardResult struct {
		index int
		bytes int64
		err   error
	}
	results := make(chan shardResult, len(dlResp.URLs))
	baseName := safeJobResultFilename(normalizedJobID)
	// strip ".json" suffix so we can add "-part-N.json"
	baseName = strings.TrimSuffix(baseName, ".json")

	for i, u := range dlResp.URLs {
		i, u := i, u
		go func() {
			shardPath := filepath.Join(*outputDir, fmt.Sprintf("%s-part-%d.json", baseName, i))
			n, err := downloadShard(u, shardPath)
			results <- shardResult{index: i, bytes: n, err: err}
		}()
	}

	var totalBytes int64
	for range dlResp.URLs {
		r := <-results
		if r.err != nil {
			log.Fatalf("shard %d download failed: %v", r.index, r.err)
		}
		totalBytes += r.bytes
	}
	fmt.Printf("downloaded %d shard(s), %d bytes total to %s\n", len(dlResp.URLs), totalBytes, *outputDir)
}

// downloadShard fetches a pre-signed URL and writes the body to path.
func downloadShard(rawURL, path string) (int64, error) {
	resp, err := http.Get(rawURL) //nolint:noctx
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("HTTP %d from storage", resp.StatusCode)
	}
	f, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return io.Copy(f, resp.Body)
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

// ── jobs helpers ───────────────────────────────────────────

// jobRequestPath constructs a safe API path for a job request.
// It URL-escapes the jobID to prevent path traversal attacks and appends the given suffix.
// Example: jobRequestPath("../jobs/abc 123", "/results") → "/jobs/..%2Fjobs%2Fabc%20123/results"
func jobRequestPath(jobID, suffix string) string {
	escaped := url.PathEscape(jobID)
	return fmt.Sprintf("/api/v1/jobs/%s%s", escaped, suffix)
}

// safeJobResultFilename sanitizes a job ID to create a safe filesystem filename.
// It removes path separators and special characters that could cause traversal attacks,
// then appends ".json". If the input is empty or whitespace, it returns "job.json".
// Example: safeJobResultFilename("../windows\\system32:evil") → "windows_system32_evil.json"
func safeJobResultFilename(jobID string) string {
	normalized := strings.TrimSpace(jobID)
	if normalized == "" {
		return "job.json"
	}

	// Replace dangerous characters: /, \, :, etc. with underscore
	dangerous := []string{"/", "\\", ":", "..", ".", "~"}
	for _, char := range dangerous {
		normalized = strings.ReplaceAll(normalized, char, "_")
	}

	// Remove any remaining non-alphanumeric characters except underscore and hyphen
	var safe strings.Builder
	for _, ch := range normalized {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '-' {
			safe.WriteRune(ch)
		} else {
			safe.WriteRune('_')
		}
	}

	result := safe.String()
	// Trim leading/trailing underscores that may have resulted from sanitization
	result = strings.Trim(result, "_")
	if result == "" {
		result = "job"
	}
	return result + ".json"
}
