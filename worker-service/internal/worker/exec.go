package worker

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// credentialEnvPrefixes lists env var prefixes that must never be visible to
// user-supplied code. The check is case-insensitive so it covers both Linux
// (upper-case) and Windows (mixed-case) conventions.
var credentialEnvPrefixes = []string{
	"S3_ACCESS_KEY",
	"S3_SECRET_KEY",
	"WORKER_RPC_TOKEN",
	"MANAGER_ADDR",
	"MANAGER_INTERNAL_API_KEY",
	"MANAGER_WORKER_RPC_TOKEN",
	"MINIO_ACCESS",
	"MINIO_SECRET",
	"POSTGRES_",
}

// sandboxedEnv returns os.Environ() with credential-bearing variables removed.
// We strip only known-sensitive keys rather than using an allowlist so that
// system paths, locale settings, and temp dirs remain intact across platforms.
func sandboxedEnv() []string {
	all := os.Environ()
	out := make([]string, 0, len(all))
	for _, kv := range all {
		key := kv
		if i := strings.IndexByte(kv, '='); i >= 0 {
			key = kv[:i]
		}
		upper := strings.ToUpper(key)
		sensitive := false
		for _, prefix := range credentialEnvPrefixes {
			if strings.HasPrefix(upper, prefix) {
				sensitive = true
				break
			}
		}
		if !sensitive {
			out = append(out, kv)
		}
	}
	return out
}

// buildCmd constructs the OS command for a user-provided code artifact.
// Runtime is inferred from runtimeEnv first, then from the file extension.
func buildCmd(ctx context.Context, codePath, runtimeEnv string) (*exec.Cmd, error) {
	rt := strings.ToLower(strings.TrimSpace(runtimeEnv))
	ext := strings.ToLower(filepath.Ext(codePath))
	switch {
	case rt == "python" || rt == "python3" || ext == ".py":
		return exec.CommandContext(ctx, "python3", codePath), nil
	case rt == "java" || ext == ".jar":
		return exec.CommandContext(ctx, "java", "-jar", codePath), nil
	case rt == "c" || rt == "cpp":
		// Compiled binary: downloadCode already stripped the extension and set
		// execute permissions; run the binary directly.
		return exec.CommandContext(ctx, codePath), nil
	default:
		// Pre-compiled binary or arbitrary executable.
		return exec.CommandContext(ctx, codePath), nil
	}
}

// compileC compiles a C source file: gcc -O3 src -o out.
// A 60-second timeout prevents a malicious or pathological source file from
// holding the worker indefinitely.
func compileC(ctx context.Context, srcPath, outPath string) error {
	compileCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(compileCtx, "gcc", "-O3", srcPath, "-o", outPath)
	cmd.Env = sandboxedEnv()
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("gcc: %w\n%s", err, out)
	}
	return nil
}

// compileCpp compiles a C++ source file: g++ -O3 src -o out.
// A 60-second timeout prevents a malicious or pathological source file from
// holding the worker indefinitely.
func compileCpp(ctx context.Context, srcPath, outPath string) error {
	compileCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(compileCtx, "g++", "-O3", srcPath, "-o", outPath)
	cmd.Env = sandboxedEnv()
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("g++: %w\n%s", err, out)
	}
	return nil
}

// runUserCode executes the user code with JSONL piped on stdin and captures stdout.
// cmd.Env uses sandboxedEnv so user-supplied code cannot read credentials
// (S3 keys, RPC tokens) from the worker's environment.
func runUserCode(ctx context.Context, codePath, runtimeEnv string, stdin io.Reader) ([]byte, error) {
	cmd, err := buildCmd(ctx, codePath, runtimeEnv)
	if err != nil {
		return nil, err
	}
	cmd.Env = sandboxedEnv()
	cmd.Stdin = stdin
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("user code %q: %w", codePath, err)
	}
	return out, nil
}
