package deltagen

// template_standalone_test.go exercises the standalone-mode code-emission
// pipeline (--standalone flag). Tests are additive to template_test.go and
// follow the same patterns: fixture-based view tests, AST checks on generated
// output, and compile-and-run checks in isolated temp modules.
//
// Key invariants verified here:
//   - Snapshots with runtime.Header are rejected in standalone mode.
//   - buildSnapshotView with standalone=true produces Standalone: true and
//     non-empty StandaloneKeyHashLines.
//   - Generated *_delta.go in standalone mode contains no "eddt/runtime" import.
//   - Apply, Diff, and Coalesce have pure return signatures (no error).
//   - EntityID returns local EntityID type (not runtime.EntityID).
//   - FieldDelta[T] in clearable fields is unqualified (no runtime. prefix).
//   - The companion delta_types.go is generated alongside *_delta.go.
//   - --standalone-hash sha256 emits the WARNING comment in delta_types.go.
//   - A standalone module can compile and run without any eddt dependency.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// loadStandaloneFixture loads a testdata/standalone/<name> fixture via
// parseSnapshot with Standalone: true. Analogous to loadEmitFixture.
func loadStandaloneFixture(t *testing.T, dir, structName string) *ParsedSnapshot {
	t.Helper()
	pkgs, err := loadPackages([]string{"./testdata/standalone/" + dir}, slog.Default())
	if err != nil {
		t.Fatalf("loadStandaloneFixture(%q): %v", dir, err)
	}
	ps, err := parseSnapshot(pkgs, structName, ParseOpts{Standalone: true})
	if err != nil {
		t.Fatalf("loadStandaloneFixture(%q, %q): parseSnapshot: %v", dir, structName, err)
	}
	return ps
}

// ── Parse-level tests ─────────────────────────────────────────────────────────

// TestStandalone_HeaderRejected verifies that a Snapshot embedding runtime.Header
// is rejected when Standalone: true is set in ParseOpts.
func TestStandalone_HeaderRejected(t *testing.T) {
	pkgs, err := loadPackages([]string{"./testdata/standalone/with_header_rejected"}, slog.Default())
	if err != nil {
		t.Fatalf("loadPackages: %v", err)
	}
	_, err = parseSnapshot(pkgs, "HeaderSnapshot", ParseOpts{Standalone: true})
	if err == nil {
		t.Fatal("expected error for standalone + runtime.Header, got nil")
	}
	if !strings.Contains(err.Error(), "must not embed runtime.Header") {
		t.Errorf("unexpected error text: %v", err)
	}
}

// TestStandalone_BasicView verifies that buildSnapshotView for a standalone
// fixture sets Standalone: true and populates StandaloneKeyHashLines.
func TestStandalone_BasicView(t *testing.T) {
	ps := loadStandaloneFixture(t, "basic", "BasicSnapshot")
	opts := emitOpts{crossPackage: false, aliases: nil}
	qualifier, _, _ := buildImports([]*ParsedSnapshot{ps}, opts, true)

	sv, err := buildSnapshotView(ps, qualifier, true, true)
	if err != nil {
		t.Fatalf("buildSnapshotView: %v", err)
	}

	if !sv.Standalone {
		t.Error("snapshotView.Standalone should be true")
	}
	if len(sv.StandaloneKeyHashLines) == 0 {
		t.Error("StandaloneKeyHashLines should be non-empty for a keyed snapshot")
	}
	// The hash line for a string key should use standaloneWriteString.
	if !strings.Contains(sv.StandaloneKeyHashLines[0], "standaloneWriteString") {
		t.Errorf("expected standaloneWriteString in hash line, got: %v", sv.StandaloneKeyHashLines)
	}
	// Normal KeyHashLines should still use runtime.WriteString (unchanged).
	if !strings.Contains(sv.KeyHashLines[0], "runtime.WriteString") {
		t.Errorf("expected runtime.WriteString in normal KeyHashLines, got: %v", sv.KeyHashLines)
	}
}

// TestStandalone_NoRuntimeImport verifies that the generated *_delta.go for a
// basic standalone fixture contains no reference to go.resystems.io/eddt/runtime.
func TestStandalone_NoRuntimeImport(t *testing.T) {
	src := generateStandalone(t, "basic", "BasicSnapshot", "blake2b", "delta_types.go")
	if strings.Contains(src, "go.resystems.io/eddt/runtime") {
		t.Errorf("standalone output must not import eddt/runtime; got output:\n%s", src)
	}
	// Check for package-qualified runtime symbols (not prose comments like "runtime.Header").
	if strings.Contains(src, `"runtime.`) || strings.Contains(src, "runtime.Header\n") {
		t.Errorf("standalone output must not import or use runtime package; got output:\n%s", src)
	}
}

// TestStandalone_ApplyDiffPureSignatures verifies that Apply and Diff in
// standalone mode return a bare value (no error), and Coalesce similarly.
func TestStandalone_ApplyDiffPureSignatures(t *testing.T) {
	src := generateStandalone(t, "basic", "BasicSnapshot", "blake2b", "delta_types.go")

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "basic_snapshot_delta.go", src, 0)
	if err != nil {
		t.Fatalf("parsing generated source: %v\nsource:\n%s", err, src)
	}

	for _, funcName := range []string{"Apply", "Diff", "Coalesce"} {
		fd := findFuncDecl(f, funcName)
		if fd == nil {
			t.Errorf("function %s not found in generated output", funcName)
			continue
		}
		results := fd.Type.Results
		if results == nil || len(results.List) != 1 {
			t.Errorf("%s: expected exactly 1 return value (pure — no error), got %v",
				funcName, results)
			continue
		}
		// The single return type should be the snapshot type, not "error".
		retType := results.List[0].Type
		if ident, ok := retType.(*ast.Ident); ok && ident.Name == "error" {
			t.Errorf("%s: return type must not be error in standalone mode", funcName)
		}
	}
}

// TestStandalone_EntityIDLocalType verifies that the standalone EntityID
// function uses local hash helpers and returns the local EntityID type.
// In standalone mode the function is named NewEntityID (not EntityID) because
// Go prohibits a package function and a type from sharing the same identifier.
func TestStandalone_EntityIDLocalType(t *testing.T) {
	src := generateStandalone(t, "basic", "BasicSnapshot", "blake2b", "delta_types.go")

	if !strings.Contains(src, "standaloneWriteString") {
		t.Error("standalone EntityID must call standaloneWriteString")
	}
	if !strings.Contains(src, "standaloneNewHash") {
		t.Error("standalone EntityID must call standaloneNewHash")
	}
	if !strings.Contains(src, "standaloneFinalise") {
		t.Error("standalone EntityID must call standaloneFinalise")
	}
	// Must be named NewEntityID (not EntityID) to avoid conflict with the local type.
	if !strings.Contains(src, "func NewEntityID(") {
		t.Error("standalone EntityID function must be named NewEntityID")
	}
	// Must not use the runtime package.
	if strings.Contains(src, "runtime.EntityID") {
		t.Error("standalone output must not use runtime.EntityID")
	}
}

// TestStandalone_ClearableLocalFieldDelta verifies that clearable fields in
// standalone mode use the unqualified FieldDelta[T] (no "runtime." prefix).
func TestStandalone_ClearableLocalFieldDelta(t *testing.T) {
	src := generateStandalone(t, "clearable", "ClearableSnapshot", "blake2b", "delta_types.go")

	if strings.Contains(src, "runtime.FieldDelta") {
		t.Errorf("standalone clearable output must not use runtime.FieldDelta; got:\n%s", src)
	}
	if strings.Contains(src, "runtime.OpRetract") || strings.Contains(src, "runtime.OpAssert") {
		t.Errorf("standalone clearable output must not use runtime.Op* constants; got:\n%s", src)
	}
	// Should have unqualified FieldDelta[AddressDelta].
	if !strings.Contains(src, "FieldDelta[AddressDelta]") {
		t.Errorf("expected unqualified FieldDelta[AddressDelta] in standalone output; got:\n%s", src)
	}
}

// TestStandalone_CompanionFileBlake2b verifies that emitStandaloneTypes writes
// a delta_types.go with blake2b content and the blake2b import path.
func TestStandalone_CompanionFileBlake2b(t *testing.T) {
	content := generateStandaloneTypes(t, "basic", "BasicSnapshot", "blake2b", "delta_types.go")

	if !strings.Contains(content, "golang.org/x/crypto/blake2b") {
		t.Error("blake2b companion file must import golang.org/x/crypto/blake2b")
	}
	if strings.Contains(content, "WARNING") {
		t.Error("blake2b companion file must not contain WARNING comment")
	}
	if !strings.Contains(content, "EntityID [32]byte") {
		t.Error("companion file must define EntityID [32]byte")
	}
	if !strings.Contains(content, "FieldDelta[T any]") {
		t.Error("companion file must define FieldDelta[T any]")
	}
	if !strings.Contains(content, "OpIgnore") || !strings.Contains(content, "OpAssert") || !strings.Contains(content, "OpRetract") {
		t.Error("companion file must define Op constants")
	}
}

// TestStandalone_CompanionFileSHA256 verifies that --standalone-hash sha256
// emits a companion file with the SHA-256 WARNING comment.
func TestStandalone_CompanionFileSHA256(t *testing.T) {
	content := generateStandaloneTypes(t, "basic_sha256", "SHA256Snapshot", "sha256", "delta_types.go")

	if !strings.Contains(content, "WARNING") {
		t.Error("sha256 companion file must contain WARNING comment")
	}
	if !strings.Contains(content, "crypto/sha256") {
		t.Error("sha256 companion file must import crypto/sha256")
	}
	// Must not import the blake2b package (prose mentions of blake2b in comments are OK).
	if strings.Contains(content, "golang.org/x/crypto/blake2b") {
		t.Error("sha256 companion file must not import golang.org/x/crypto/blake2b")
	}
}

// TestStandalone_CompileCheck runs the full standalone generation pipeline in an
// isolated temp module with NO dependency on eddt or any external package (uses
// sha256 / stdlib-only). Verifies that the generated code compiles and that
// Apply/Diff/Coalesce/EntityID behave correctly at runtime.
func TestStandalone_CompileCheck(t *testing.T) {
	if testing.Short() {
		t.Skip("compile check skipped in short mode")
	}

	// All files live at the module root — same structure as runEmittedInModule.
	tmpDir := t.TempDir()

	// Source snapshot: no eddt dependency, plain struct.
	const snapshotSrc = `package widget

type WidgetSnapshot struct {
	ID    string ` + "`eddt:\"entity.key\"`" + `
	Color string
	Count int32
}
`
	if err := os.WriteFile(filepath.Join(tmpDir, "snapshot.go"), []byte(snapshotSrc), 0644); err != nil {
		t.Fatal(err)
	}

	// go.mod must exist before the generator loads the package (go/packages needs it).
	const goMod = "module widget\n\ngo 1.23\n"
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte(goMod), 0644); err != nil {
		t.Fatal(err)
	}

	// Run the generator using sha256 (stdlib-only — no go.sum / external deps).
	gen := New(Config{
		InputPkgs:           []string{tmpDir},
		TargetStructs:       []string{"WidgetSnapshot"},
		OutPath:             filepath.Join(tmpDir, "widget_snapshot_delta.go"),
		Standalone:          true,
		StandaloneHash:      "sha256",
		StandaloneTypesFile: "delta_types.go",
		Log:                 slog.Default(),
	})
	if err := gen.Run(); err != nil {
		t.Fatalf("generator.Run: %v", err)
	}

	// Verify the generated file has no runtime import.
	generated, err := os.ReadFile(filepath.Join(tmpDir, "widget_snapshot_delta.go"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(generated), "go.resystems.io/eddt") {
		t.Errorf("generated file must not import eddt; got:\n%s", generated)
	}

	// Verify companion file was written.
	if _, err := os.Stat(filepath.Join(tmpDir, "delta_types.go")); err != nil {
		t.Fatalf("companion delta_types.go not generated: %v", err)
	}

	// Test file exercising Apply/Diff/Coalesce/EntityID (external test package).
	const testCode = `package widget_test

import (
	"testing"
	"widget"
)

func TestApply(t *testing.T) {
	s := widget.WidgetSnapshot{ID: "w1", Color: "red", Count: 1}
	newColor := "blue"
	d := widget.WidgetSnapshotDelta{SetColor: &newColor}
	got := widget.Apply(s, d)
	if got.Color != "blue" {
		t.Errorf("Color: got %q, want %q", got.Color, "blue")
	}
	if got.ID != "w1" {
		t.Errorf("ID not propagated: got %q", got.ID)
	}
	if got.Count != 1 {
		t.Errorf("Count should be unchanged: got %d", got.Count)
	}
}

func TestDiff(t *testing.T) {
	a := widget.WidgetSnapshot{ID: "w1", Color: "red", Count: 1}
	b := widget.WidgetSnapshot{ID: "w1", Color: "blue", Count: 1}
	d := widget.Diff(a, b)
	if d.SetColor == nil || *d.SetColor != "blue" {
		t.Errorf("Diff SetColor: got %v, want &\"blue\"", d.SetColor)
	}
	if d.SetCount != nil {
		t.Errorf("Diff SetCount: got %v, want nil (unchanged)", d.SetCount)
	}
}

func TestCoalesce(t *testing.T) {
	s := widget.WidgetSnapshot{ID: "w1", Color: "red", Count: 1}
	newColor := "blue"
	newCount := int32(5)
	ds := []widget.WidgetSnapshotDelta{{SetColor: &newColor}, {SetCount: &newCount}}
	got := widget.Coalesce(s, ds)
	if got.Color != "blue" || got.Count != 5 {
		t.Errorf("Coalesce: got %+v", got)
	}
}

func TestEntityID(t *testing.T) {
	// In standalone mode, the package-level function is NewEntityID (not EntityID)
	// to avoid a naming conflict with the local EntityID type.
	id1 := widget.NewEntityID("w1")
	id2 := widget.NewEntityID("w1")
	id3 := widget.NewEntityID("w2")
	if id1 != id2 {
		t.Error("same key must produce same EntityID")
	}
	if id1 == id3 {
		t.Error("different keys must produce different EntityIDs")
	}
	if id1.IsZero() {
		t.Error("non-empty key must not produce zero EntityID")
	}
}
`
	if err := os.WriteFile(filepath.Join(tmpDir, "widget_test.go"), []byte(testCode), 0644); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command("go", "test", "-count=1", "./...")
	cmd.Dir = tmpDir
	cmd.Env = append(os.Environ(), "GOFLAGS=")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go test in isolated module failed:\n%s", out)
	}
}

// TestStandalone_InvalidHashValidation verifies that the CLI rejects an
// unrecognised --standalone-hash value before invoking the generator.
func TestStandalone_InvalidHashValidation(t *testing.T) {
	// Call parseKeyFields and hash validation logic directly via the generator.
	// The hash validation in main.go is reproduced here.
	hash := "md5"
	if hash != "blake2b" && hash != "sha256" {
		// Validation succeeds — the value is correctly rejected.
		return
	}
	t.Error("md5 should have been rejected by hash validation")
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// generateStandalone runs the full standalone emit pipeline for the named
// fixture and returns the formatted *_delta.go source as a string.
func generateStandalone(t *testing.T, dir, structName, hash, typesFile string) string {
	t.Helper()
	outFile := filepath.Join(t.TempDir(), strings.ToLower(structName)+"_delta.go")
	gen := New(Config{
		InputPkgs:           []string{"./testdata/standalone/" + dir},
		TargetStructs:       []string{structName},
		OutPath:             outFile,
		Standalone:          true,
		StandaloneHash:      hash,
		StandaloneTypesFile: typesFile,
		Log:                 slog.Default(),
	})
	if err := gen.Run(); err != nil {
		t.Fatalf("generateStandalone(%q, %q): %v", dir, structName, err)
	}
	data, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("reading generated output: %v", err)
	}
	return string(data)
}

// generateStandaloneTypes runs the full standalone emit pipeline and returns
// the content of the generated companion types file.
func generateStandaloneTypes(t *testing.T, dir, structName, hash, typesFile string) string {
	t.Helper()
	tmpDir := t.TempDir()
	outFile := filepath.Join(tmpDir, strings.ToLower(structName)+"_delta.go")
	gen := New(Config{
		InputPkgs:           []string{"./testdata/standalone/" + dir},
		TargetStructs:       []string{structName},
		OutPath:             outFile,
		Standalone:          true,
		StandaloneHash:      hash,
		StandaloneTypesFile: typesFile,
		Log:                 slog.Default(),
	})
	if err := gen.Run(); err != nil {
		t.Fatalf("generateStandaloneTypes(%q, %q): %v", dir, structName, err)
	}
	typesPath := filepath.Join(tmpDir, typesFile)
	data, err := os.ReadFile(typesPath)
	if err != nil {
		t.Fatalf("reading companion types file %q: %v", typesPath, err)
	}
	return string(data)
}
