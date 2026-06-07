package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestHelpIncludesInitCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer

	code := run([]string{"--help"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr = %q", code, stderr.String())
	}
	output := stdout.String() + stderr.String()
	for _, want := range []string{
		"elarkd init (--client|--server)",
		"Commands:",
		"init    write a client or server config template",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("help output = %q, want %q", output, want)
		}
	}
}

func TestInitHelp(t *testing.T) {
	var stdout, stderr bytes.Buffer

	code := run([]string{"init", "--help"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr = %q", code, stderr.String())
	}
	output := stdout.String() + stderr.String()
	for _, want := range []string{
		"elarkd init (--client|--server)",
		"-client",
		"-server",
		"-force",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("init help output = %q, want %q", output, want)
		}
	}
}
