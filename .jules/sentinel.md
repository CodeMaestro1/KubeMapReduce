## 2025-02-27 - Unbounded HTTP Client Timeouts
**Vulnerability:** The CLI used `http.Get(rawURL)` directly without a timeout when downloading result shards in `cmdJobsDownload`.
**Learning:** Default HTTP clients in Go (`http.DefaultClient`) do not have configured timeouts. This can lead to indefinite hangs and resource exhaustion if the remote server fails to respond or drops packets, which is a significant risk in CLI tools that hold system resources.
**Prevention:** Always instantiate a specific `http.Client{Timeout: ...}` or use `context.WithTimeout` with `http.NewRequestWithContext` to enforce bounded operation times for network requests.
