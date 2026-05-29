package main

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestCLI_MissingStructs verifies that omitting both --type and positional args
// produces the "at least one target struct" error from RunE.
// Covers: R-DG-036, R-DG-037, R-DG-038
func TestCLI_MissingStructs(t *testing.T) {
	cmd := newRootCmd()
	cmd.SetArgs([]string{
		"--pkg", ".",
		"--out", "dummy.go",
	})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when no struct is specified, got nil")
	}
	if !strings.Contains(err.Error(), "at least one target struct must be specified") {
		t.Errorf("expected at-least-one-struct error, got: %v", err)
	}
}

// TestCLI_EmitsDeltaType verifies that a valid invocation runs the full
// pipeline end-to-end and writes a generated file containing the TDelta struct
// declaration. The valid fixture provides a conforming Snapshot with an
// eddt:"entity.key" field so parse succeeds and R-DG-015 emits ValidSnapshotDelta.
// Covers: R-DG-036, R-DG-037, R-DG-038
func TestCLI_EmitsDeltaType(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "valid_delta.go")
	cmd := newRootCmd()
	cmd.SetArgs([]string{
		"--pkg", "../../internal/deltagen/testdata/parse/valid",
		"--type", "ValidSnapshot",
		"--out", outPath,
	})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("expected successful run, got error: %v", err)
	}
	assertDeltaFile(t, outPath, "ValidSnapshotDelta")
}

// TestCLI_Help verifies that --help exits 0 and the output mentions every
// flag that callers of delta-gen depend on.
// Covers: R-DG-036, R-DG-037, R-DG-038
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
	for _, flag := range []string{"--pkg", "--type", "--out", "--pkg-alias", "--pkg-name", "--verbose", "--key-field"} {
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
// Covers: R-DG-037
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
		"--type", "Foo",
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
// to every struct in --type.
// Covers: R-DG-036, R-DG-037, R-DG-038, R-DG-040
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
// Covers: R-DG-036, R-DG-037, R-DG-038, R-DG-040
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
// Covers: R-DG-036, R-DG-037, R-DG-038, R-DG-040
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
// Covers: R-DG-036, R-DG-037, R-DG-038, R-DG-040
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
// Covers: R-DG-036, R-DG-037, R-DG-038, R-DG-040
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
// StructName is not listed in --type is rejected at parse time.
// Covers: R-DG-036, R-DG-037, R-DG-038, R-DG-040
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
// override parse succeeds and R-DG-015 emits NoKeySnapshotDelta.
// Covers: R-DG-036, R-DG-037, R-DG-038, R-DG-040
func TestCLI_KeyField_BareAccepted(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "no_key_delta.go")
	cmd := newRootCmd()
	cmd.SetArgs([]string{
		"--pkg", "../../internal/deltagen/testdata/parse/no_key",
		"--type", "NoKeySnapshot",
		"--key-field", "Peer",
		"--out", outPath,
	})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("expected successful run (key-field should wire through to emit), got: %v", err)
	}
	assertDeltaFile(t, outPath, "NoKeySnapshotDelta")
}

// TestCLI_KeyField_PerStructAccepted verifies that the StructName=FieldName form
// is accepted and behaves identically to the bare form when there is one struct.
// Covers: R-DG-036, R-DG-037, R-DG-038, R-DG-040
func TestCLI_KeyField_PerStructAccepted(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "no_key_delta.go")
	cmd := newRootCmd()
	cmd.SetArgs([]string{
		"--pkg", "../../internal/deltagen/testdata/parse/no_key",
		"--type", "NoKeySnapshot",
		"--key-field", "NoKeySnapshot=Peer",
		"--out", outPath,
	})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("expected successful run (per-struct key-field should wire through), got: %v", err)
	}
	assertDeltaFile(t, outPath, "NoKeySnapshotDelta")
}

// TestCLI_KeyField_PerStructWinsOverBare verifies end-to-end that a per-struct
// --key-field overrides a bare --key-field for the same struct. The bare value
// "NoSuchField" is superseded by "ValidSnapshot=Bearer", so parse succeeds
// and R-DG-015 emits ValidSnapshotDelta.
// Covers: R-DG-036, R-DG-037, R-DG-038, R-DG-040
func TestCLI_KeyField_PerStructWinsOverBare(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "valid_delta.go")
	cmd := newRootCmd()
	cmd.SetArgs([]string{
		"--pkg", "../../internal/deltagen/testdata/parse/valid",
		"--type", "ValidSnapshot",
		"--key-field", "NoSuchField",
		"--key-field", "ValidSnapshot=Bearer",
		"--out", outPath,
	})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("expected successful run (per-struct override should win), got: %v", err)
	}
	assertDeltaFile(t, outPath, "ValidSnapshotDelta")
}

// TestCLI_KeyField_UnrecognisedStructError verifies that --key-field with a
// StructName not in --type produces a startup error before any package
// loading occurs.
// Covers: R-DG-036, R-DG-037, R-DG-038, R-DG-040
func TestCLI_KeyField_UnrecognisedStructError(t *testing.T) {
	cmd := newRootCmd()
	cmd.SetArgs([]string{
		"--pkg", "../../internal/deltagen/testdata/parse/valid",
		"--type", "ValidSnapshot",
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
// emits a slog Warn entry to stderr identifying the override and the tagged
// field. After G-08 the warning fires unconditionally (at Warn level) without
// requiring --verbose. The valid fixture has Key tagged entity.key; overriding
// to Bearer triggers the conflict.
// Covers: R-DG-036, R-DG-037, R-DG-038, R-DG-040
func TestCLI_KeyField_VerboseConflictWarning(t *testing.T) {
	// Redirect os.Stderr to capture the slog Warn output. The slog handler is
	// constructed inside RunE after os.Stderr has been replaced, so it writes
	// to the pipe rather than the real stderr.
	origStderr := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stderr = w

	outPath := filepath.Join(t.TempDir(), "valid_delta.go")
	cmd := newRootCmd()
	cmd.SetArgs([]string{
		"--pkg", "../../internal/deltagen/testdata/parse/valid",
		"--type", "ValidSnapshot",
		"--key-field", "ValidSnapshot=Bearer",
		"--out", outPath,
	})
	runErr := cmd.Execute()

	w.Close()
	os.Stderr = origStderr

	var buf bytes.Buffer
	if _, copyErr := io.Copy(&buf, r); copyErr != nil {
		t.Fatalf("io.Copy: %v", copyErr)
	}

	// R-DG-015 lands: the generator now succeeds; the conflict warning still fires.
	if runErr != nil {
		t.Errorf("expected successful run after conflict warning, got: %v", runErr)
	}

	out := buf.String()
	// slog text handler emits: level=WARN msg="..." struct=... override=... tag_field=...
	if !strings.Contains(out, "level=WARN") {
		t.Errorf("expected level=WARN in slog output, got:\n%s", out)
	}
	if !strings.Contains(out, "struct=ValidSnapshot") {
		t.Errorf("expected struct=ValidSnapshot in slog output, got:\n%s", out)
	}
	if !strings.Contains(out, "override=Bearer") {
		t.Errorf("expected override=Bearer in slog output, got:\n%s", out)
	}
	if !strings.Contains(out, "tag_field=Key") {
		t.Errorf("expected tag_field=Key in slog output, got:\n%s", out)
	}
}

// ── R-DG-038: positional args ────────────────────────────────────────────────────

// TestCLI_PositionalStructArg verifies that struct names passed as positional
// arguments (without --type) are accepted and generate correctly.
// Covers: R-DG-038
func TestCLI_PositionalStructArg(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "valid_delta.go")
	cmd := newRootCmd()
	cmd.SetArgs([]string{
		"--pkg", "../../internal/deltagen/testdata/parse/valid",
		"--out", outPath,
		"ValidSnapshot", // positional
	})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("positional struct arg: expected success, got: %v", err)
	}
	assertDeltaFile(t, outPath, "ValidSnapshotDelta")
}

// TestCLI_PositionalAndTypeMerge verifies that --type and positional args are
// unioned: a struct passed via --type is merged with one passed as a positional
// arg, and both are available as targets.  Uses a single package with two
// snapshot types so both resolve in the same type universe.
// Covers: R-DG-038
func TestCLI_PositionalAndTypeMerge(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "delta.go")
	cmd := newRootCmd()
	cmd.SetArgs([]string{
		"--pkg", "./testdata/multi",
		"--out", outPath,
		"FirstSnapshot",            // positional
		"--type", "SecondSnapshot", // flag
	})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("positional + --type merge: expected success, got: %v", err)
	}
	// Both deltas should appear in the bundled output file.
	assertDeltaFile(t, outPath, "FirstSnapshotDelta")
	assertDeltaFile(t, outPath, "SecondSnapshotDelta")
}

// ── R-DG-038: auto-derived output paths ─────────────────────────────────────────

// TestCLI_AutoDerivedOutPath verifies that omitting --out causes the output
// filename to be auto-derived as <snake_case_struct>_delta.go in the current
// working directory.
// Covers: R-DG-038
func TestCLI_AutoDerivedOutPath(t *testing.T) {
	tmpDir := t.TempDir()
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	defer os.Chdir(origDir) //nolint:errcheck

	// Use absolute --pkg path since we changed directory.
	pkgPath := filepath.Join(origDir, "../../internal/deltagen/testdata/parse/valid")

	cmd := newRootCmd()
	cmd.SetArgs([]string{"--pkg", pkgPath, "ValidSnapshot"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("auto-derive out path: expected success, got: %v", err)
	}
	assertDeltaFile(t, filepath.Join(tmpDir, "valid_snapshot_delta.go"), "ValidSnapshotDelta")
}

// TestCLI_MultiTypeAutoDerivedSplit verifies that when multiple struct names
// are passed without --out, each struct is written to its own auto-derived file.
// Covers: R-DG-038
func TestCLI_MultiTypeAutoDerivedSplit(t *testing.T) {
	tmpDir := t.TempDir()
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	defer os.Chdir(origDir) //nolint:errcheck

	// Use a single package with two snapshot types so both resolve in the same
	// type universe (loadPackages makes one packages.Load call per filesystem path).
	multiPath := filepath.Join(origDir, "testdata/multi")

	cmd := newRootCmd()
	cmd.SetArgs([]string{
		"--pkg", multiPath,
		"FirstSnapshot", "SecondSnapshot", // two structs, no --out
	})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("multi-type auto-derive split: expected success, got: %v", err)
	}
	assertDeltaFile(t, filepath.Join(tmpDir, "first_snapshot_delta.go"), "FirstSnapshotDelta")
	assertDeltaFile(t, filepath.Join(tmpDir, "second_snapshot_delta.go"), "SecondSnapshotDelta")
}

// TestCLI_ExplicitOutOverridesAutoDerive verifies that when --out is given,
// the named file is used (not an auto-derived name), even for a single struct.
// Covers: R-DG-038
func TestCLI_ExplicitOutOverridesAutoDerive(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "custom_name.go")
	cmd := newRootCmd()
	cmd.SetArgs([]string{
		"--pkg", "../../internal/deltagen/testdata/parse/valid",
		"--out", outPath,
		"ValidSnapshot",
	})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("explicit --out: expected success, got: %v", err)
	}
	// The auto-derived name must NOT have been written; only outPath should exist.
	assertDeltaFile(t, outPath, "ValidSnapshotDelta")
}

// ── R-DG-038: deriveOutPath unit tests ──────────────────────────────────────────

// TestDeriveOutPath_Cases exercises the snake_case derivation helper across
// representative struct names including acronyms, digit boundaries, and
// short names.
// Covers: R-DG-038
func TestDeriveOutPath_Cases(t *testing.T) {
	cases := []struct{ in, want string }{
		{"UESnapshot", "ue_snapshot_delta.go"},
		{"SessionSnapshot", "session_snapshot_delta.go"},
		{"HTTPHandler", "http_handler_delta.go"},
		{"IDValue", "id_value_delta.go"},
		{"V1Snapshot", "v1_snapshot_delta.go"},
		{"Snapshot", "snapshot_delta.go"},
		{"S", "s_delta.go"},
		{"AB", "ab_delta.go"},
		{"AbC", "ab_c_delta.go"},
	}
	for _, tc := range cases {
		got := deriveOutPath(tc.in)
		if got != tc.want {
			t.Errorf("deriveOutPath(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// ── R-DG-020: --type flag + -t short alias ──────────────────────────────────────

// TestCLI_TypeFlag verifies that --type is accepted as the replacement for
// --structs and produces a valid generated file.
// Covers: R-DG-020
func TestCLI_TypeFlag(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "valid_delta.go")
	cmd := newRootCmd()
	cmd.SetArgs([]string{
		"--pkg", "../../internal/deltagen/testdata/parse/valid",
		"--type", "ValidSnapshot",
		"--out", outPath,
	})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("--type flag: expected success, got: %v", err)
	}
	assertDeltaFile(t, outPath, "ValidSnapshotDelta")
}

// TestCLI_TypeFlagShort verifies that -t (the short form of --type) is accepted.
// Covers: R-DG-020
func TestCLI_TypeFlagShort(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "valid_delta.go")
	cmd := newRootCmd()
	cmd.SetArgs([]string{
		"--pkg", "../../internal/deltagen/testdata/parse/valid",
		"-t", "ValidSnapshot",
		"--out", outPath,
	})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("-t short flag: expected success, got: %v", err)
	}
	assertDeltaFile(t, outPath, "ValidSnapshotDelta")
}

// assertDeltaFile parses the file at path and verifies it contains a type
// declaration for a struct named deltaName.  Fatals if the file is missing or
// not valid Go; errors if the struct declaration is absent.
func assertDeltaFile(t *testing.T, path, deltaName string) {
	t.Helper()
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("output file not found at %s: %v", path, err)
	}
	fset := token.NewFileSet()
	f, parseErr := parser.ParseFile(fset, path, src, 0)
	if parseErr != nil {
		t.Fatalf("output file is not valid Go: %v\n--- source ---\n%s", parseErr, src)
	}
	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range gd.Specs {
			ts, ok := spec.(*ast.TypeSpec)
			if ok && ts.Name.Name == deltaName {
				if _, isStruct := ts.Type.(*ast.StructType); isStruct {
					return
				}
			}
		}
	}
	t.Errorf("output file does not contain 'type %s struct'", deltaName)
}
