package main

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
)

func TestPromptForInput(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		isTerminal bool
		wantPrompt bool
		wantResult string
	}{
		{
			name:       "Non-terminal input",
			input:      "testuser\n",
			isTerminal: false,
			wantPrompt: false,
			wantResult: "testuser",
		},
		{
			name:       "Non-terminal input with CRLF",
			input:      "testuser\r\n",
			isTerminal: false,
			wantPrompt: false,
			wantResult: "testuser",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Mock stdin
			oldStdin := os.Stdin
			r, w, _ := os.Pipe()
			os.Stdin = r
			defer func() { os.Stdin = oldStdin }()

			// Reset global stdinReader for test
			stdinReader.Reset(r)

			// Mock stderr to check for prompt
			oldStderr := os.Stderr
			stderrR, stderrW, _ := os.Pipe()
			os.Stderr = stderrW
			defer func() { os.Stderr = oldStderr }()

			// Write input
			go func() {
				io.WriteString(w, tt.input)
				w.Close()
			}()

			// Run
			// Note: term.IsTerminal will return false for os.Pipe()
			got, err := promptForInput("Prompt: ")
			if err != nil {
				t.Fatalf("promptForInput failed: %v", err)
			}

			if got != tt.wantResult {
				t.Errorf("promptForInput() = %q, want %q", got, tt.wantResult)
			}

			stderrW.Close()
			var buf bytes.Buffer
			io.Copy(&buf, stderrR)
			hasPrompt := strings.Contains(buf.String(), "Prompt: ")
			if hasPrompt != tt.wantPrompt {
				t.Errorf("prompt visibility = %v, want %v", hasPrompt, tt.wantPrompt)
			}
		})
	}
}

func TestReadSecretInput_NonTTY(t *testing.T) {
	input := "secretpassword\n"
	oldStdin := os.Stdin
	r, w, _ := os.Pipe()
	os.Stdin = r
	defer func() { os.Stdin = oldStdin }()

	stdinReader.Reset(r)

	go func() {
		io.WriteString(w, input)
		w.Close()
	}()

	got, err := readSecretInput("Password: ")
	if err != nil {
		t.Fatalf("readSecretInput failed: %v", err)
	}

	if got != "secretpassword" {
		t.Errorf("readSecretInput() = %q, want %q", got, "secretpassword")
	}
}
