package worker

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

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
func compileC(srcPath, outPath string) error {
	out, err := exec.Command("gcc", "-O3", srcPath, "-o", outPath).CombinedOutput()
	if err != nil {
		return fmt.Errorf("gcc: %w\n%s", err, out)
	}
	return nil
}

// compileCpp compiles a C++ source file: g++ -O3 src -o out.
func compileCpp(srcPath, outPath string) error {
	out, err := exec.Command("g++", "-O3", srcPath, "-o", outPath).CombinedOutput()
	if err != nil {
		return fmt.Errorf("g++: %w\n%s", err, out)
	}
	return nil
}

// runUserCode executes the user code with JSONL piped on stdin and captures stdout.
func runUserCode(ctx context.Context, codePath, runtimeEnv string, stdin io.Reader) ([]byte, error) {
	cmd, err := buildCmd(ctx, codePath, runtimeEnv)
	if err != nil {
		return nil, err
	}
	cmd.Stdin = stdin
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("user code %q: %w", codePath, err)
	}
	return out, nil
}
