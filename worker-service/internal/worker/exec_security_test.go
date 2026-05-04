package worker

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestEnsureSafePath(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"file.c", "./file.c"},
		{"./file.c", "./file.c"},
		{"../file.c", "../file.c"},
		{"-o", "./-o"},
		{"@file", "./@file"},
	}

	for _, tt := range tests {
		got := ensureSafePath(tt.input)
		if got != tt.expected {
			t.Errorf("ensureSafePath(%q) = %q, want %q", tt.input, got, tt.expected)
		}
	}

	// Test with platform-specific absolute path
	absPath := filepath.Join(t.TempDir(), "file.c")
	got := ensureSafePath(absPath)
	if got != absPath {
		t.Errorf("ensureSafePath(absolute path) = %q, want %q", got, absPath)
	}
}

func TestSecurityCompileC(t *testing.T) {
	// Skip if gcc is not installed
	if _, err := exec.LookPath("gcc"); err != nil {
		t.Skip("gcc not found in PATH")
	}

	tempDir := t.TempDir()

	// Create a file starting with @ which would be a response file for gcc
	srcName := "@evil.c"
	srcPath := filepath.Join(tempDir, srcName)

	// If it was interpreted as a response file, gcc would try to read it as flags.
	// We put a valid C program in it. If our fix works, gcc treats it as a filename.
	srcContent := "#include <stdio.h>\nint main() { printf(\"hello\"); return 0; }\n"
	if err := os.WriteFile(srcPath, []byte(srcContent), 0644); err != nil {
		t.Fatalf("failed to write C file: %v", err)
	}

	// Change to the temp directory so we can use relative paths
	oldWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get current directory: %v", err)
	}
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("failed to change directory: %v", err)
	}
	defer os.Chdir(oldWd)

	outPath := "test_out"

	// Use relative path to trigger ensureSafePath
	if err := compileC(context.Background(), srcName, outPath); err != nil {
		t.Fatalf("compileC() failed with relative path starting with @: %v", err)
	}

	if _, err := os.Stat(outPath); os.IsNotExist(err) {
		t.Fatalf("compileC() did not create output file")
	}
}

func TestSecurityBuildCmd(t *testing.T) {
	ctx := context.Background()

	t.Run("relative path starting with dash", func(t *testing.T) {
		codePath := "-version.py"
		cmd, err := buildCmd(ctx, codePath, "python3")
		if err != nil {
			t.Fatalf("buildCmd error: %v", err)
		}

		found := false
		for _, arg := range cmd.Args {
			if arg == "./-version.py" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("buildCmd args %v does not contain ./-version.py", cmd.Args)
		}
	})

	t.Run("relative path starting with @", func(t *testing.T) {
		codePath := "@response.py"
		cmd, err := buildCmd(ctx, codePath, "python3")
		if err != nil {
			t.Fatalf("buildCmd error: %v", err)
		}

		found := false
		for _, arg := range cmd.Args {
			if arg == "./@response.py" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("buildCmd args %v does not contain ./@response.py", cmd.Args)
		}
	})
}
