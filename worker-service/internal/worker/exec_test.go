package worker

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBuildCmd(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name       string
		codePath   string
		runtimeEnv string
		wantCmd    []string
		wantErr    bool
	}{
		{
			name:       "python runtime",
			codePath:   "script",
			runtimeEnv: "python",
			wantCmd:    []string{"python3", "./script"},
			wantErr:    false,
		},
		{
			name:       "python3 runtime",
			codePath:   "script",
			runtimeEnv: "python3",
			wantCmd:    []string{"python3", "./script"},
			wantErr:    false,
		},
		{
			name:       "python extension fallback",
			codePath:   "script.py",
			runtimeEnv: "",
			wantCmd:    []string{"python3", "./script.py"},
			wantErr:    false,
		},
		{
			name:       "java runtime",
			codePath:   "app.jar",
			runtimeEnv: "java",
			wantCmd:    []string{"java", "-jar", "./app.jar"},
			wantErr:    false,
		},
		{
			name:       "jar extension fallback",
			codePath:   "app.jar",
			runtimeEnv: "",
			wantCmd:    []string{"java", "-jar", "./app.jar"},
			wantErr:    false,
		},
		{
			name:       "default binary (no extension, no runtime)",
			codePath:   "binary",
			runtimeEnv: "",
			wantCmd:    []string{"./binary"},
			wantErr:    false,
		},
		{
			name:       "c runtime env runs binary directly",
			codePath:   "binary",
			runtimeEnv: "c",
			wantCmd:    []string{"./binary"},
			wantErr:    false,
		},
		{
			name:       "cpp runtime env runs binary directly",
			codePath:   "binary",
			runtimeEnv: "cpp",
			wantCmd:    []string{"./binary"},
			wantErr:    false,
		},
		{
			// Security fix: unknown runtime now returns error instead of silently running as binary
			name:       "unknown runtime rejects with error",
			codePath:   "binary",
			runtimeEnv: "unknown",
			wantCmd:    nil,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, err := buildCmd(ctx, tt.codePath, tt.runtimeEnv)
			if (err != nil) != tt.wantErr {
				t.Fatalf("buildCmd() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				return // Expected error case
			}
			if len(cmd.Args) != len(tt.wantCmd) {
				t.Fatalf("buildCmd() got %v, want %v", cmd.Args, tt.wantCmd)
			}
			for i, arg := range cmd.Args {
				// The first argument might be an absolute path to the executable (e.g. /usr/bin/python3)
				// so we check if it ends with the expected command or equals it.
				if i == 0 {
					if !strings.HasSuffix(arg, tt.wantCmd[i]) && arg != tt.wantCmd[i] {
						t.Errorf("buildCmd() arg[%d] got %v, want it to end with %v or equal %v", i, arg, tt.wantCmd[i], tt.wantCmd[i])
					}
				} else {
					if arg != tt.wantCmd[i] {
						t.Errorf("buildCmd() arg[%d] got %v, want %v", i, arg, tt.wantCmd[i])
					}
				}
			}
		})
	}
}

func TestRunUserCode(t *testing.T) {
	// Use python3 as the test runtime — it is available on all CI platforms
	// and avoids the shell-injection vector that sh-based scripts would create.
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not found in PATH")
	}

	tempDir := t.TempDir()

	t.Run("success", func(t *testing.T) {
		binPath := filepath.Join(tempDir, "echo.py")
		script := "import sys\nsys.stdout.write(sys.stdin.read())\n"
		if err := os.WriteFile(binPath, []byte(script), 0644); err != nil {
			t.Fatalf("failed to write temp script: %v", err)
		}

		ctx := context.Background()
		input := []byte("hello world\n")
		out, err := runUserCode(ctx, binPath, "", bytes.NewReader(input))
		if err != nil {
			t.Fatalf("runUserCode() error = %v", err)
		}
		if string(out) != "hello world\n" {
			t.Errorf("runUserCode() got %q, want %q", string(out), "hello world\n")
		}
	})

	t.Run("failure (non-zero exit)", func(t *testing.T) {
		failPath := filepath.Join(tempDir, "fail.py")
		if err := os.WriteFile(failPath, []byte("import sys\nsys.exit(1)\n"), 0644); err != nil {
			t.Fatalf("failed to write temp script: %v", err)
		}

		ctx := context.Background()
		_, err := runUserCode(ctx, failPath, "", bytes.NewReader([]byte{}))
		if err == nil {
			t.Fatal("runUserCode() expected error for non-zero exit code")
		}
		if !strings.Contains(err.Error(), "user code") {
			t.Errorf("expected error to wrap with 'user code', got: %v", err)
		}
	})

	t.Run("context cancellation", func(t *testing.T) {
		sleepPath := filepath.Join(tempDir, "sleep.py")
		if err := os.WriteFile(sleepPath, []byte("import time\ntime.sleep(10)\n"), 0644); err != nil {
			t.Fatalf("failed to write temp script: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		_, err := runUserCode(ctx, sleepPath, "", bytes.NewReader([]byte{}))
		if err == nil {
			t.Fatal("runUserCode() expected error due to context cancellation")
		}
	})
}

func TestCompileC(t *testing.T) {
	// Skip if gcc is not installed
	if _, err := exec.LookPath("gcc"); err != nil {
		t.Skip("gcc not found in PATH")
	}

	tempDir := t.TempDir()
	srcPath := filepath.Join(tempDir, "test.c")
	outPath := filepath.Join(tempDir, "test_out")

	srcContent := `#include <stdio.h>
int main() {
	printf("success\n");
	return 0;
}
`
	if err := os.WriteFile(srcPath, []byte(srcContent), 0644); err != nil {
		t.Fatalf("failed to write C file: %v", err)
	}

	if err := compileC(context.Background(), srcPath, outPath); err != nil {
		t.Fatalf("compileC() error = %v", err)
	}

	if _, err := os.Stat(outPath); os.IsNotExist(err) {
		t.Fatalf("compileC() did not create output file")
	}

	// Verify compiled binary works
	out, err := exec.Command(outPath).Output()
	if err != nil {
		t.Fatalf("compiled binary failed: %v", err)
	}
	if string(out) != "success\n" {
		t.Errorf("compiled binary output = %q, want %q", string(out), "success\n")
	}
}

func TestCompileCpp(t *testing.T) {
	// Skip if g++ is not installed
	if _, err := exec.LookPath("g++"); err != nil {
		t.Skip("g++ not found in PATH")
	}

	tempDir := t.TempDir()
	srcPath := filepath.Join(tempDir, "test.cpp")
	outPath := filepath.Join(tempDir, "test_out")

	srcContent := `#include <iostream>
int main() {
	std::cout << "success" << std::endl;
	return 0;
}
`
	if err := os.WriteFile(srcPath, []byte(srcContent), 0644); err != nil {
		t.Fatalf("failed to write C++ file: %v", err)
	}

	if err := compileCpp(context.Background(), srcPath, outPath); err != nil {
		t.Fatalf("compileCpp() error = %v", err)
	}

	if _, err := os.Stat(outPath); os.IsNotExist(err) {
		t.Fatalf("compileCpp() did not create output file")
	}

	// Verify compiled binary works
	out, err := exec.Command(outPath).Output()
	if err != nil {
		t.Fatalf("compiled binary failed: %v", err)
	}
	if string(out) != "success\n" {
		t.Errorf("compiled binary output = %q, want %q", string(out), "success\n")
	}
}
