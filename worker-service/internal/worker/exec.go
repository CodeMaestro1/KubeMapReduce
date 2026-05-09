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

// allowedEnvVars is a whitelist of environment variables that are safe to expose
// to user-supplied code. All other variables are filtered out, including any
// credentials, internal service addresses, or tokens.
var allowedEnvVars = map[string]bool{
	"PATH":    true, // Standard system path
	"HOME":    true, // User home directory
	"USER":    true, // Username
	"LANG":    true, // Locale setting
	"TMPDIR":  true, // Temporary directory
	"TZ":      true, // Timezone
	"TERM":    true, // Terminal type
	"TASK_ID": true, // Task identifier (safe to expose)
	"JOB_ID":  true, // Job identifier (safe to expose)
}

// sandboxedEnv returns a minimal, safe set of environment variables for
// user-supplied code execution. Uses an allowlist approach to prevent
// credential leakage through both explicit and implicit variable access.
func sandboxedEnv() []string {
	all := os.Environ()
	out := make([]string, 0, len(all))
	for _, kv := range all {
		key := kv
		if i := strings.IndexByte(kv, '='); i >= 0 {
			key = kv[:i]
		}
		// Only include variables explicitly in the allowlist.
		if allowedEnvVars[key] {
			out = append(out, kv)
		}
	}
	return out
}

// buildCmd constructs the OS command for a user-provided code artifact.
// Runtime is inferred from runtimeEnv first, then from the file extension.
// Returns an error if the runtime is unsupported (fail-safe).
func buildCmd(ctx context.Context, codePath, runtimeEnv string) (*exec.Cmd, error) {
	rt := strings.ToLower(strings.TrimSpace(runtimeEnv))
	ext := strings.ToLower(filepath.Ext(codePath))
	safePath := ensureSafePath(codePath)
	switch {
	case rt == "python" || rt == "python3" || ext == ".py":
		return exec.CommandContext(ctx, "python3", safePath), nil
	case rt == "java" || ext == ".jar":
		return exec.CommandContext(ctx, "java", "-jar", safePath), nil
	case rt == "c" || rt == "cpp" || ext == ".c" || ext == ".cpp" || ext == ".cc" || ext == ".cxx":
		// Compiled binary: downloadCode already stripped the extension and set
		// execute permissions; run the binary directly.
		return exec.CommandContext(ctx, safePath), nil
	case rt == "sh" || ext == ".sh":
		// Shell script: run with sh interpreter
		return exec.CommandContext(ctx, "sh", safePath), nil
	case rt == "":
		// No runtime specified and no recognized extension; treat as pre-compiled binary
		// only if it has no extension (i.e., likely a compiled binary).
		if ext == "" {
			return exec.CommandContext(ctx, safePath), nil
		}
		fallthrough
	default:
		// Unknown runtime or unsupported file extension.
		return nil, fmt.Errorf("unsupported runtime %q or unknown file extension %q; supported: python3, java, c, cpp, sh", rt, ext)
	}
}

// compileC compiles a C source file with hardening flags.
// A 60-second timeout prevents a malicious or pathological source file from
// holding the worker indefinitely.
func compileC(ctx context.Context, srcPath, outPath string) error {
	compileCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	// Hardening flags: PIE (position-independent executable), stack protector,
	// fortify source, error on warnings.
	cmd := exec.CommandContext(compileCtx, "gcc",
		"-O3",
		"-fPIE",
		"-fstack-protector-strong",
		"-Werror",
		"-D_FORTIFY_SOURCE=2",
		ensureSafePath(filepath.Base(srcPath)),
		"-o", ensureSafePath(filepath.Base(outPath)))
	cmd.Env = sandboxedEnv()
	cmd.Dir = filepath.Dir(outPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("gcc: %w\n%s", err, out)
	}

	// Verify output binary is not a symlink (TOCTOU defense).
	fi, err := os.Lstat(outPath)
	if err != nil {
		return fmt.Errorf("stat compiled binary: %w", err)
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		os.Remove(outPath)
		return fmt.Errorf("compiled binary is a symlink; rejecting")
	}
	return nil
}

// compileCpp compiles a C++ source file with hardening flags.
// A 60-second timeout prevents a malicious or pathological source file from
// holding the worker indefinitely.
func compileCpp(ctx context.Context, srcPath, outPath string) error {
	compileCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	// Hardening flags: PIE (position-independent executable), stack protector,
	// fortify source, error on warnings.
	cmd := exec.CommandContext(compileCtx, "g++",
		"-O3",
		"-fPIE",
		"-fstack-protector-strong",
		"-Werror",
		"-D_FORTIFY_SOURCE=2",
		ensureSafePath(filepath.Base(srcPath)),
		"-o", ensureSafePath(filepath.Base(outPath)))
	cmd.Env = sandboxedEnv()
	cmd.Dir = filepath.Dir(outPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("g++: %w\n%s", err, out)
	}

	// Verify output binary is not a symlink (TOCTOU defense).
	fi, err := os.Lstat(outPath)
	if err != nil {
		return fmt.Errorf("stat compiled binary: %w", err)
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		os.Remove(outPath)
		return fmt.Errorf("compiled binary is a symlink; rejecting")
	}
	return nil
}

// ensureSafePath ensures that a path passed as an argument to a command is
// either absolute or explicitly relative (starting with ./ or ../) to prevent
// it from being interpreted as a command-line flag or a response file (@file).
func ensureSafePath(p string) string {
	if filepath.IsAbs(p) {
		return "./" + filepath.Base(p)
	}
	if strings.HasPrefix(filepath.Clean(p), "..") {
		return "./" + filepath.Base(p)
	}
	if strings.HasPrefix(p, "./") || strings.HasPrefix(p, "../") {
		return p
	}
	return "./" + p
}

// runUserCodeStreaming starts the user binary with JSONL on stdin and returns
// a streaming stdout reader plus a wait func that returns the process exit
// error. Callers MUST close the returned ReadCloser and invoke wait() once
// the stdout pipe is fully drained. cmd.Env uses sandboxedEnv so user code
// cannot read credentials from the worker environment.
func runUserCodeStreaming(ctx context.Context, codePath, runtimeEnv string, stdin io.Reader) (io.ReadCloser, func() error, error) {
	cmd, err := buildCmd(ctx, filepath.Base(codePath), runtimeEnv)
	if err != nil {
		return nil, nil, err
	}
	cmd.Env = sandboxedEnv()
	cmd.Stdin = stdin
	cmd.Stderr = os.Stderr
	cmd.Dir = filepath.Dir(codePath)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, fmt.Errorf("stdout pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, nil, fmt.Errorf("start user code %q: %w", codePath, err)
	}
	wait := func() error {
		if err := cmd.Wait(); err != nil {
			return fmt.Errorf("user code %q: %w", codePath, err)
		}
		return nil
	}
	return stdout, wait, nil
}

// runUserCode executes the user code with JSONL piped on stdin and captures stdout.
// cmd.Env uses sandboxedEnv so user-supplied code cannot read credentials
// (S3 keys, RPC tokens) from the worker's environment.
func runUserCode(ctx context.Context, codePath, runtimeEnv string, stdin io.Reader) ([]byte, error) {
	cmd, err := buildCmd(ctx, filepath.Base(codePath), runtimeEnv)
	if err != nil {
		return nil, err
	}
	cmd.Env = sandboxedEnv()
	cmd.Stdin = stdin
	cmd.Stderr = os.Stderr
	cmd.Dir = filepath.Dir(codePath)
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("user code %q: %w", codePath, err)
	}
	return out, nil
}
