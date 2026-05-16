package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestCLI_MissingStructs verifies that omitting --structs produces the Cobra
// required-flag error and a non-zero exit.
// Covers: R-09
func TestCLI_MissingStructs(t *testing.T) {
	cmd := newRootCmd()
	cmd.SetArgs([]string{
		"--pkg", ".",
		"--out", "dummy.go",
	})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when --structs is omitted, got nil")
	}
	if !strings.Contains(err.Error(), `required flag(s) "structs" not set`) {
		t.Errorf("expected required-flag error, got: %v", err)
	}
}

// TestCLI_NotYetImplemented verifies that a valid invocation propagates the
// generator's "not yet implemented" error back through the CLI layer. With
// G-03 / G-07 / G-04 implemented the parse stage now runs to completion
// (Header resolved, entity.key identified, payload fields classified). The
// first not-yet-implemented stage is Phase 3 tag handling. The fixture at
// ../../internal/deltagen/testdata/parse/valid provides a conforming Snapshot
// (including an eddt:"entity.key" field) so parse succeeds end-to-end.
// Covers: R-09
func TestCLI_NotYetImplemented(t *testing.T) {
	cmd := newRootCmd()
	cmd.SetArgs([]string{
		"--pkg", "../../internal/deltagen/testdata/parse/valid",
		"--structs", "ValidSnapshot",
		"--out", "dummy.go",
	})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error from stub generator, got nil")
	}
	if !strings.Contains(err.Error(), "not yet implemented") {
		t.Errorf("expected 'not yet implemented' error, got: %v", err)
	}
}

// TestCLI_Help verifies that --help exits 0 and the output mentions every
// flag that callers of delta-gen depend on.
// Covers: R-09
func TestCLI_Help(t *testing.T) {
	var buf bytes.Buffer

	cmd := newRootCmd()
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"--help"})

	// Cobra returns nil for --help even though it exits 0.
	if err := cmd.Execute(); err != nil {
		t.Fatalf("--help returned unexpected error: %v", err)
	}

	out := buf.String()
	for _, flag := range []string{"--pkg", "--structs", "--out", "--pkg-alias", "--pkg-name", "--verbose"} {
		if !strings.Contains(out, flag) {
			t.Errorf("--help output missing flag %q", flag)
		}
	}
}

// TestCLI_ImportPathNotInGoMod verifies that invoking delta-gen with a
// nonexistent Go import path produces an error containing "go get" remediation
// guidance (from formatImportPathErrors in load.go) and does not produce a
// "failed to load package directory" error (which would indicate the import
// path was wrongly classified as a filesystem path).
// Covers: R-11
func TestCLI_ImportPathNotInGoMod(t *testing.T) {

	tmpDir := t.TempDir()

	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module dummy\n\ngo 1.25.0\n"), 0644); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "main.go"), []byte("package dummy\n"), 0644); err != nil {
		t.Fatalf("write main.go: %v", err)
	}

	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	defer os.Chdir(origDir) //nolint:errcheck

	cmd := newRootCmd()
	cmd.SetArgs([]string{
		"--pkg", "github.com/nonexistent/pkg123456789",
		"--structs", "Foo",
		"--out", filepath.Join(tmpDir, "dummy.go"),
	})

	err = cmd.Execute()
	if err == nil {
		t.Fatal("expected error for nonexistent import path, got nil")
	}

	errMsg := err.Error()
	if !strings.Contains(errMsg, "go get") {
		t.Errorf("expected error to contain 'go get' remediation guidance, got: %s", errMsg)
	}
	if strings.Contains(errMsg, "failed to load package directory") {
		t.Errorf("expected import-path error, not filesystem-path error, got: %s", errMsg)
	}
}
