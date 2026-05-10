package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"golang.org/x/term"
)

var stdinReader = bufio.NewReader(os.Stdin)

// readPasswordFn is a package-level variable that wraps term.ReadPassword
// to allow mocking in tests and to provide a consistent interface for
// reading sensitive input across the CLI.
var readPasswordFn = func(fd int) ([]byte, error) {
	if term.IsTerminal(fd) {
		return term.ReadPassword(fd)
	}

	// Fallback for non-TTY: read from stdin directly.
	// We use a shared reader to avoid buffering issues when multiple
	// inputs are read from a pipe.
	line, err := stdinReader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	// Trim both \n and \r\n to ensure cross-platform compatibility.
	return []byte(strings.TrimRight(line, "\r\n")), nil
}

// isTerminal returns true if the provided file descriptor is a terminal.
func isTerminal(fd int) bool {
	return term.IsTerminal(fd)
}

// promptForInput prints a message to stderr and reads a string from stdin.
// It skips the prompt if stdin is not a terminal to avoid polluting
// non-interactive logs.
func promptForInput(prompt string) (string, error) {
	if isTerminal(int(os.Stdin.Fd())) {
		fmt.Fprint(os.Stderr, prompt)
	}

	line, err := stdinReader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(line), nil
}

// readSecretInput reads sensitive input (like a password).
// It disables echo if stdin is a terminal.
func readSecretInput(prompt string) (string, error) {
	if isTerminal(int(os.Stdin.Fd())) {
		fmt.Fprint(os.Stderr, prompt)
		raw, err := readPasswordFn(int(os.Stdin.Fd()))
		fmt.Fprintln(os.Stderr)
		if err != nil {
			return "", err
		}
		return string(raw), nil
	}

	// Non-TTY: use the same readPasswordFn fallback.
	raw, err := readPasswordFn(int(os.Stdin.Fd()))
	if err != nil {
		return "", err
	}
	return string(raw), nil
}
