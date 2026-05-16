package main

import (
	"bytes"
	"io"
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
	for _, flag := range []string{"--pkg", "--structs", "--out", "--pkg-alias", "--pkg-name", "--verbose", "--key-field"} {
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

// ── Group H: parseKeyFields unit tests ───────────────────────────────────────
//
// These tests call parseKeyFields directly (package main, so accessible) with
// the already-split slice that Cobra's StringSliceVar delivers. The comma-
// separated flag form (--key-field "ThingA=Key,ThingB=Name") is split by Cobra
// before reaching parseKeyFields, so testing with ["ThingA=Key","ThingB=Name"]
// covers both the repeated-flag and comma-separated forms.

// TestParseKeyFields_SingleBare verifies that a single bare FieldName expands
// to every struct in --structs.
// Covers: R-09, E-13
func TestParseKeyFields_SingleBare(t *testing.T) {
	got, err := parseKeyFields([]string{"Peer"}, []string{"NoKeySnapshot"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got["NoKeySnapshot"] != "Peer" {
		t.Errorf("NoKeySnapshot: got %q, want %q", got["NoKeySnapshot"], "Peer")
	}
}

// TestParseKeyFields_SinglePerStruct verifies that a StructName=FieldName entry
// maps exactly the named struct.
// Covers: R-09, E-13
func TestParseKeyFields_SinglePerStruct(t *testing.T) {
	got, err := parseKeyFields([]string{"NoKeySnapshot=Peer"}, []string{"NoKeySnapshot"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got["NoKeySnapshot"] != "Peer" {
		t.Errorf("NoKeySnapshot: got %q, want %q", got["NoKeySnapshot"], "Peer")
	}
}

// TestParseKeyFields_PerStructWinsOverBare verifies that a per-struct entry
// overrides a bare entry for the same struct.
// Covers: R-09, E-13
func TestParseKeyFields_PerStructWinsOverBare(t *testing.T) {
	// Bare says "NoSuchField" for all structs; per-struct overrides to "Location".
	got, err := parseKeyFields(
		[]string{"NoSuchField", "ValidSnapshot=Location"},
		[]string{"ValidSnapshot"},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got["ValidSnapshot"] != "Location" {
		t.Errorf("ValidSnapshot: got %q, want %q (per-struct should win)", got["ValidSnapshot"], "Location")
	}
}

// TestParseKeyFields_TwoPerStructDifferentStructs verifies that two per-struct
// entries for different structs are both mapped correctly. This covers the
// comma-separated form --key-field "ThingA=Key,ThingB=Name" as Cobra delivers
// it: ["ThingA=Key", "ThingB=Name"].
// Covers: R-09, E-13
func TestParseKeyFields_TwoPerStructDifferentStructs(t *testing.T) {
	got, err := parseKeyFields(
		[]string{"ThingA=Key", "ThingB=Name"},
		[]string{"ThingA", "ThingB"},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got["ThingA"] != "Key" {
		t.Errorf("ThingA: got %q, want %q", got["ThingA"], "Key")
	}
	if got["ThingB"] != "Name" {
		t.Errorf("ThingB: got %q, want %q", got["ThingB"], "Name")
	}
}

// TestParseKeyFields_DuplicateBareError verifies that two different bare values
// for the same set of structs produce an error rather than silently picking the
// last one.
// Covers: R-09, E-13
func TestParseKeyFields_DuplicateBareError(t *testing.T) {
	_, err := parseKeyFields([]string{"Peer", "Status"}, []string{"NoKeySnapshot"})
	if err == nil {
		t.Fatal("expected error for duplicate bare values, got nil")
	}
	if !strings.Contains(err.Error(), "ambiguous") {
		t.Errorf("error should mention 'ambiguous', got: %v", err)
	}
}

// TestParseKeyFields_UnrecognisedStruct verifies that a per-struct entry whose
// StructName is not listed in --structs is rejected at parse time.
// Covers: R-09, E-13
func TestParseKeyFields_UnrecognisedStruct(t *testing.T) {
	_, err := parseKeyFields([]string{"NotAStruct=Location"}, []string{"ValidSnapshot"})
	if err == nil {
		t.Fatal("expected error for unrecognised struct name, got nil")
	}
	if !strings.Contains(err.Error(), "NotAStruct") {
		t.Errorf("error should mention the unknown struct name, got: %v", err)
	}
}

// ── Group H: CLI integration tests for --key-field ───────────────────────────

// TestCLI_KeyField_BareAccepted verifies that a bare --key-field value is
// accepted by the CLI and wired through to the generator: the no_key fixture
// has no entity.key tag, so without the override parse would fail; with the
// override parse succeeds and the generator reaches its Phase 3 sentinel.
// Covers: R-09, E-13
func TestCLI_KeyField_BareAccepted(t *testing.T) {
	cmd := newRootCmd()
	cmd.SetArgs([]string{
		"--pkg", "../../internal/deltagen/testdata/parse/no_key",
		"--structs", "NoKeySnapshot",
		"--key-field", "Peer",
		"--out", "dummy.go",
	})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected sentinel error from stub generator, got nil")
	}
	if !strings.Contains(err.Error(), "not yet implemented") {
		t.Errorf("expected 'not yet implemented' (parse should succeed), got: %v", err)
	}
}

// TestCLI_KeyField_PerStructAccepted verifies that the StructName=FieldName form
// is accepted and behaves identically to the bare form when there is one struct.
// Covers: R-09, E-13
func TestCLI_KeyField_PerStructAccepted(t *testing.T) {
	cmd := newRootCmd()
	cmd.SetArgs([]string{
		"--pkg", "../../internal/deltagen/testdata/parse/no_key",
		"--structs", "NoKeySnapshot",
		"--key-field", "NoKeySnapshot=Peer",
		"--out", "dummy.go",
	})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected sentinel error from stub generator, got nil")
	}
	if !strings.Contains(err.Error(), "not yet implemented") {
		t.Errorf("expected 'not yet implemented' (parse should succeed), got: %v", err)
	}
}

// TestCLI_KeyField_PerStructWinsOverBare verifies end-to-end that a per-struct
// --key-field overrides a bare --key-field for the same struct. The bare value
// "NoSuchField" would cause parseKeyField to error; the per-struct override
// "ValidSnapshot=Location" selects the comparable Location field instead, so
// parse succeeds and the generator reaches its sentinel.
// Covers: R-09, E-13
func TestCLI_KeyField_PerStructWinsOverBare(t *testing.T) {
	cmd := newRootCmd()
	cmd.SetArgs([]string{
		"--pkg", "../../internal/deltagen/testdata/parse/valid",
		"--structs", "ValidSnapshot",
		"--key-field", "NoSuchField",
		"--key-field", "ValidSnapshot=Location",
		"--out", "dummy.go",
	})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected sentinel error from stub generator, got nil")
	}
	if !strings.Contains(err.Error(), "not yet implemented") {
		t.Errorf("expected 'not yet implemented' (per-struct override should win), got: %v", err)
	}
}

// TestCLI_KeyField_UnrecognisedStructError verifies that --key-field with a
// StructName not in --structs produces a startup error before any package
// loading occurs.
// Covers: R-09, E-13
func TestCLI_KeyField_UnrecognisedStructError(t *testing.T) {
	cmd := newRootCmd()
	cmd.SetArgs([]string{
		"--pkg", "../../internal/deltagen/testdata/parse/valid",
		"--structs", "ValidSnapshot",
		"--key-field", "NotAStruct=Location",
		"--out", "dummy.go",
	})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected startup error for unrecognised struct, got nil")
	}
	if !strings.Contains(err.Error(), "NotAStruct") {
		t.Errorf("error should mention the unrecognised struct name, got: %v", err)
	}
}

// TestCLI_KeyField_VerboseConflictWarning verifies that when both an
// eddt:"entity.key" tag and a --key-field override are present, the generator
// emits a warning to stdout (under --verbose) identifying the override and the
// tagged field. The valid fixture has Key tagged entity.key; overriding to
// Location triggers the conflict.
// Covers: R-09, E-13
func TestCLI_KeyField_VerboseConflictWarning(t *testing.T) {
	// Redirect os.Stdout so we can capture the verbose warning from fmt.Printf.
	origStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = w

	cmd := newRootCmd()
	cmd.SetArgs([]string{
		"--pkg", "../../internal/deltagen/testdata/parse/valid",
		"--structs", "ValidSnapshot",
		"--key-field", "ValidSnapshot=Location",
		"--verbose",
		"--out", "dummy.go",
	})
	runErr := cmd.Execute()

	w.Close()
	os.Stdout = origStdout

	var buf bytes.Buffer
	if _, copyErr := io.Copy(&buf, r); copyErr != nil {
		t.Fatalf("io.Copy: %v", copyErr)
	}

	// The generator always returns the Phase 3 sentinel after a successful parse.
	if runErr == nil || !strings.Contains(runErr.Error(), "not yet implemented") {
		t.Errorf("expected 'not yet implemented' sentinel, got: %v", runErr)
	}

	out := buf.String()
	if !strings.Contains(out, "warning") {
		t.Errorf("expected conflict warning in verbose output, got:\n%s", out)
	}
	// Warning must name the override field and the tagged field.
	if !strings.Contains(out, "Location") {
		t.Errorf("warning should mention override field 'Location', got:\n%s", out)
	}
	if !strings.Contains(out, "Key") {
		t.Errorf("warning should mention tagged field 'Key', got:\n%s", out)
	}
}
