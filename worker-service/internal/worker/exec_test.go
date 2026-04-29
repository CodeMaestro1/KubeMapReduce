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
	}{
		{
			name:       "python runtime",
			codePath:   "/path/to/script",
			runtimeEnv: "python",
			wantCmd:    []string{"python3", "/path/to/script"},
		},
		{
			name:       "python3 runtime",
			codePath:   "/path/to/script",
			runtimeEnv: "python3",
			wantCmd:    []string{"python3", "/path/to/script"},
		},
		{
			name:       "python extension fallback",
			codePath:   "/path/to/script.py",
			runtimeEnv: "",
			wantCmd:    []string{"python3", "/path/to/script.py"},
		},
		{
			name:       "java runtime",
			codePath:   "/path/to/app.jar",
			runtimeEnv: "java",
			wantCmd:    []string{"java", "-jar", "/path/to/app.jar"},
		},
		{
			name:       "jar extension fallback",
			codePath:   "/path/to/app.jar",
			runtimeEnv: "",
			wantCmd:    []string{"java", "-jar", "/path/to/app.jar"},
		},
		{
			name:       "default binary",
			codePath:   "/path/to/binary",
			runtimeEnv: "",
			wantCmd:    []string{"/path/to/binary"},
		},
		{
			name:       "c/cpp defaults to binary",
			codePath:   "/path/to/binary",
			runtimeEnv: "c",
			wantCmd:    []string{"/path/to/binary"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, err := buildCmd(ctx, tt.codePath, tt.runtimeEnv)
			if err != nil {
				t.Fatalf("buildCmd() error = %v", err)
			}
			if len(cmd.Args) != len(tt.wantCmd) {
				t.Fatalf("buildCmd() got %v, want %v", cmd.Args, tt.wantCmd)
			}
			for i, arg := range cmd.Args {
				// The first argument might be an absolute path to the executable (e.g. /usr/bin/python3)
				// so we check if it ends with the expected command or equals it.
				if i == 0 {
					if !strings.HasSuffix(arg, tt.wantCmd[i]) && arg != tt.wantCmd[i] {
						t.Errorf("buildCmd() arg[%d] got %v, want it to end with %v", i, arg, tt.wantCmd[i])
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
	// Create a temporary executable that echoes stdin to stdout
	tempDir := t.TempDir()
	binPath := filepath.Join(tempDir, "echo.sh")

	scriptContent := `#!/bin/sh
cat
`
	if err := os.WriteFile(binPath, []byte(scriptContent), 0755); err != nil {
		t.Fatalf("failed to write temp script: %v", err)
	}

	t.Run("success", func(t *testing.T) {
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
		failBinPath := filepath.Join(tempDir, "fail.sh")
		if err := os.WriteFile(failBinPath, []byte("#!/bin/sh\nexit 1\n"), 0755); err != nil {
			t.Fatalf("failed to write temp script: %v", err)
		}

		ctx := context.Background()
		_, err := runUserCode(ctx, failBinPath, "", bytes.NewReader([]byte{}))
		if err == nil {
			t.Fatal("runUserCode() expected error for non-zero exit code")
		}
		if !strings.Contains(err.Error(), "user code") {
			t.Errorf("expected error to wrap with 'user code', got: %v", err)
		}
	})

	t.Run("context cancellation", func(t *testing.T) {
		sleepBinPath := filepath.Join(tempDir, "sleep.sh")
		if err := os.WriteFile(sleepBinPath, []byte("#!/bin/sh\nsleep 0.2\n"), 0755); err != nil {
			t.Fatalf("failed to write temp script: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		_, err := runUserCode(ctx, sleepBinPath, "", bytes.NewReader([]byte{}))
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

	if err := compileC(srcPath, outPath); err != nil {
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

	if err := compileCpp(srcPath, outPath); err != nil {
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
