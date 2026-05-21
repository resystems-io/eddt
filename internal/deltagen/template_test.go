package deltagen

// template_test.go exercises the EM-01 / EM-02 code-emission pipeline:
//
//   - TestBuildSnapshotView: table-driven view-construction unit tests covering
//     the §1.6.3 atomic-row emission matrix row by row (V01-V10); also checks
//     Suppressed flag for delta.omit / delta.retired (EM-02) and KeyName.
//   - TestBuildSnapshotView_KeyName: asserts sv.KeyName == "Key" for atomic_all.
//   - TestEmitTemplate_AtomicAll: end-to-end pipeline test against the
//     atomic_all fixture; asserts TDelta AST shape, Apply function and method
//     wrapper presence, per-field Apply contributions (EM-02).
//   - TestEmitTemplate_AtomicApply_CrossPackage: asserts Apply in cross-package
//     mode: qualified signature, no method wrapper (E-12, EM-02).
//   - TestEmitTemplate_NestedNotYet: asserts that delta.nested triggers the
//     Phase-5 sentinel error.
//   - TestEmitTemplate_CrossPackageQualifier: asserts type-string qualification
//     in cross-package mode.
//   - TestEmitTemplate_CompileCheck: runs go test in an isolated temp module
//     with a replace directive; exercises Apply round-trip and
//     HeaderAfterApply error propagation (EM-02).

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// ── Group EM: view construction (TestBuildSnapshotView) ──────────────────────

// TestBuildSnapshotView exercises buildSnapshotView against the atomic_all
// fixture, verifying the §1.6.3 atomic-row emission matrix row by row.
// Covers: R-19, E-14, E-15, E-16
func TestBuildSnapshotView(t *testing.T) {
	// Load and parse the atomic_all fixture; use same-package qualifier.
	ps := loadEmitFixture(t, "atomic_all", "AtomicAllSnapshot")
	opts := emitOpts{crossPackage: false, aliases: nil}
	qualifier, _ := buildImports([]*ParsedSnapshot{ps}, opts)

	sv, err := buildSnapshotView(ps, qualifier)
	if err != nil {
		t.Fatalf("buildSnapshotView: unexpected error: %v", err)
	}

	// DeltaName is always source name + "Delta".
	if sv.DeltaName != "AtomicAllSnapshotDelta" {
		t.Errorf("DeltaName: got %q, want %q", sv.DeltaName, "AtomicAllSnapshotDelta")
	}

	// Build a lookup for quick assertion.
	byName := make(map[string]fieldView, len(sv.Fields))
	for _, f := range sv.Fields {
		byName[f.Name] = f
	}

	cases := []struct {
		// V-number label, field name, expected DeltaName, DeltaType, Suppressed flag.
		label      string
		fieldName  string
		deltaName  string
		deltaType  string
		suppressed bool // true for delta.omit / delta.retired: in view, Suppressed: true (EM-02)
	}{
		// V01 — ShapeScalar: emits *T
		{label: "V01_Scalar", fieldName: "Scalar", deltaName: "SetScalar", deltaType: "*int32"},
		// V02 — ShapePointer: emits **T (double-pointer wrap)
		{label: "V02_Pointer", fieldName: "Pointer", deltaName: "SetPointer", deltaType: "**string"},
		// V03 — ShapeStructValue (local, same-pkg): no qualifier, emits *Inner
		{label: "V03_Struct", fieldName: "Struct", deltaName: "SetStruct", deltaType: "*Inner"},
		// V05 — ShapeSlice (atomic per E-15): []byte rendered as []byte (not normalised)
		{label: "V05_Slice", fieldName: "Slice", deltaName: "SetSlice", deltaType: "*[]byte"},
		// V06 — ShapeMap (atomic per E-16): emits *map[string]int32
		{label: "V06_Map", fieldName: "Map", deltaName: "SetMap", deltaType: "*map[string]int32"},
		// V07 — delta.omit: present in view with Suppressed: true (EM-02)
		{label: "V07_Omitted", fieldName: "Omitted", suppressed: true},
		// V08 — delta.retired: present in view with Suppressed: true (EM-02)
		{label: "V08_Retired", fieldName: "Retired", suppressed: true},
		// V09 — delta.commutative: emits as if untagged (§9.5)
		{label: "V09_Commute", fieldName: "Commute", deltaName: "SetCommute", deltaType: "*int32"},
	}

	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			fv, ok := byName[tc.fieldName]
			if !ok {
				t.Fatalf("field %q missing from view; view has: %v", tc.fieldName, viewNames(sv))
			}
			if tc.suppressed {
				// Suppressed fields must carry Suppressed: true and empty Set name/type.
				if !fv.Suppressed {
					t.Errorf("field %q: want Suppressed=true, got false", tc.fieldName)
				}
				if fv.DeltaName != "" {
					t.Errorf("field %q: suppressed should have empty DeltaName, got %q", tc.fieldName, fv.DeltaName)
				}
				return
			}
			// Non-suppressed: Suppressed must be false; check Set name and type.
			if fv.Suppressed {
				t.Errorf("field %q: want Suppressed=false, got true", tc.fieldName)
			}
			if fv.DeltaName != tc.deltaName {
				t.Errorf("DeltaName: got %q, want %q", fv.DeltaName, tc.deltaName)
			}
			if fv.DeltaType != tc.deltaType {
				t.Errorf("DeltaType: got %q, want %q", fv.DeltaType, tc.deltaType)
			}
		})
	}

	// The entity-key field (Key) must never appear in Fields — it is extracted
	// by the parse stage into KeyVar, not included in the field list.
	if _, ok := byName["Key"]; ok {
		t.Errorf("entity-key field Key should not appear in the snapshot view")
	}
}

// TestBuildSnapshotView_NestedError verifies that delta.nested triggers the
// Phase-5 sentinel (V10 case).
// Covers: R-19
func TestBuildSnapshotView_NestedError(t *testing.T) {
	ps := loadEmitFixture(t, "nested_nyi", "NestedNYISnapshot")
	opts := emitOpts{crossPackage: false}
	qualifier, _ := buildImports([]*ParsedSnapshot{ps}, opts)

	_, err := buildSnapshotView(ps, qualifier)
	if err == nil {
		t.Fatal("expected Phase-5 error for delta.nested, got nil")
	}
	if !strings.Contains(err.Error(), "Phase 5") {
		t.Errorf("error should mention Phase 5, got: %v", err)
	}
	if !strings.Contains(err.Error(), "delta.nested") {
		t.Errorf("error should mention delta.nested, got: %v", err)
	}
}

// TestBuildSnapshotView_KeyName verifies that buildSnapshotView populates
// sv.KeyName from ps.KeyVar.Name() (EM-02 contract).
// Covers: R-20
func TestBuildSnapshotView_KeyName(t *testing.T) {
	ps := loadEmitFixture(t, "atomic_all", "AtomicAllSnapshot")
	opts := emitOpts{crossPackage: false}
	qualifier, _ := buildImports([]*ParsedSnapshot{ps}, opts)

	sv, err := buildSnapshotView(ps, qualifier)
	if err != nil {
		t.Fatalf("buildSnapshotView: %v", err)
	}
	if sv.KeyName != "Key" {
		t.Errorf("KeyName: got %q, want %q", sv.KeyName, "Key")
	}
}

// ── Group EM: end-to-end template tests ──────────────────────────────────────

// TestEmitTemplate_AtomicAll runs the full emit pipeline against the atomic_all
// fixture, parses the generated file with go/parser, and asserts:
//   - TDelta struct shape: Header embed, Set* fields, suppressed fields absent.
//   - Apply function signature and body structure (EM-02).
//   - Apply method wrapper present (same-package mode, E-12, EM-02).
//   - Generated file is gofmt-clean (R-11).
//
// Covers: R-19, R-20, R-25, E-12, E-14, E-15, E-16
func TestEmitTemplate_AtomicAll(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "atomic_all_delta.go")

	cfg := Config{
		InputPkgs:     []string{"./testdata/emit/atomic_all"},
		TargetStructs: []string{"AtomicAllSnapshot"},
		OutPath:       outPath,
	}
	if err := New(cfg).Run(); err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	// R-11: generated file must be gofmt-clean as written.
	assertGofmtClean(t, outPath)

	src, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("reading output file: %v", err)
	}

	// Parse the generated file to validate it is syntactically valid Go.
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, outPath, src, 0)
	if err != nil {
		t.Fatalf("generated file is not valid Go: %v\n--- source ---\n%s", err, src)
	}

	// ── TDelta struct shape ───────────────────────────────────────────────────

	deltaDecl := findStructDecl(f, "AtomicAllSnapshotDelta")
	if deltaDecl == nil {
		t.Fatalf("AtomicAllSnapshotDelta type not found in generated file")
	}

	fields := structFieldNames(deltaDecl)

	// Header embed must be present (first field).
	if len(fields) == 0 || fields[0] != "Header" {
		t.Errorf("first field should be Header embed, got: %v", fields)
	}

	// Expected Set* fields (atomic payload fields).
	for _, want := range []string{"SetScalar", "SetPointer", "SetStruct", "SetSlice", "SetMap", "SetCommute"} {
		if !contains(fields, want) {
			t.Errorf("field %q missing from AtomicAllSnapshotDelta; fields: %v", want, fields)
		}
	}

	// Suppressed fields must be absent from TDelta.
	for _, absent := range []string{"SetOmitted", "SetRetired", "Omitted", "Retired"} {
		if contains(fields, absent) {
			t.Errorf("suppressed field %q should not appear in AtomicAllSnapshotDelta; fields: %v", absent, fields)
		}
	}

	// entity.key field must be absent (extracted by parse stage).
	if contains(fields, "Key") || contains(fields, "SetKey") {
		t.Errorf("entity-key field should not appear in TDelta; fields: %v", fields)
	}

	// ── Apply function shape (EM-02) ──────────────────────────────────────────

	applyFn := findFuncDecl(f, "Apply")
	if applyFn == nil {
		t.Fatalf("Apply function not found in generated file")
	}

	// Signature: func Apply(s AtomicAllSnapshot, d AtomicAllSnapshotDelta) (AtomicAllSnapshot, error)
	if applyFn.Type.Params.NumFields() != 2 {
		t.Errorf("Apply: want 2 params, got %d", applyFn.Type.Params.NumFields())
	}
	if applyFn.Type.Results.NumFields() != 2 {
		t.Errorf("Apply: want 2 results, got %d", applyFn.Type.Results.NumFields())
	}

	// Body must contain the HeaderAfterApply call and entity-key propagation.
	srcStr := string(src)
	if !strings.Contains(srcStr, "runtime.HeaderAfterApply(s.Header, d.Header)") {
		t.Errorf("Apply body missing runtime.HeaderAfterApply(s.Header, d.Header)")
	}
	if !strings.Contains(srcStr, "result.Key = s.Key") {
		t.Errorf("Apply body missing entity-key propagation: result.Key = s.Key")
	}

	// Each atomic field must have an if/else contribution.
	for _, name := range []string{"SetScalar", "SetPointer", "SetStruct", "SetSlice", "SetMap", "SetCommute"} {
		if !strings.Contains(srcStr, "d."+name+" != nil") {
			t.Errorf("Apply body missing nil-check for d.%s", name)
		}
	}

	// Suppressed fields must have propagation-only lines.
	if !strings.Contains(srcStr, "result.Omitted = s.Omitted") {
		t.Errorf("Apply body missing suppressed-field propagation: result.Omitted = s.Omitted")
	}
	if !strings.Contains(srcStr, "result.Retired = s.Retired") {
		t.Errorf("Apply body missing suppressed-field propagation: result.Retired = s.Retired")
	}

	// ── Method wrapper present (same-package mode, E-12) ─────────────────────

	if findMethodDecl(f, "AtomicAllSnapshot", "Apply") == nil {
		t.Errorf("Apply method wrapper not found (expected in same-package mode)")
	}

	t.Run("CompileCheck", func(t *testing.T) {
		compileCheckEmit(t, src)
	})
}

// TestEmitTemplate_NestedNotYet verifies that the emit pipeline returns the
// Phase-5 sentinel error when the Snapshot contains a delta.nested field.
// Covers: R-19
func TestEmitTemplate_NestedNotYet(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "nested_nyi_delta.go")

	cfg := Config{
		InputPkgs:     []string{"./testdata/emit/nested_nyi"},
		TargetStructs: []string{"NestedNYISnapshot"},
		OutPath:       outPath,
	}
	err := New(cfg).Run()
	if err == nil {
		t.Fatal("expected Phase-5 sentinel error, got nil")
	}
	if !strings.Contains(err.Error(), "Phase 5") {
		t.Errorf("error should mention Phase 5, got: %v", err)
	}
}

// TestEmitTemplate_CrossPackageQualifier verifies that in cross-package mode
// the generated file qualifies source-package types (e.g. *model.Address) and
// imports the source package.
// Covers: R-19, E-12
func TestEmitTemplate_CrossPackageQualifier(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "cross_pkg_delta.go")

	cfg := Config{
		InputPkgs:          []string{"./testdata/emit/cross_pkg/model"},
		TargetStructs:      []string{"CrossPkgSnapshot"},
		OutPath:            outPath,
		OutPkgNameOverride: "deltas",
	}
	if err := New(cfg).Run(); err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	// R-11: generated file must be gofmt-clean as written.
	assertGofmtClean(t, outPath)

	src, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("reading output file: %v", err)
	}

	// Package declaration must be the override.
	if !strings.Contains(string(src), "package deltas") {
		t.Errorf("expected 'package deltas', got:\n%s", src)
	}

	// The struct-value field type must be qualified.
	if !strings.Contains(string(src), "*model.Address") {
		t.Errorf("expected '*model.Address' qualified reference, got:\n%s", src)
	}

	// Import block must include the source model package.
	if !strings.Contains(string(src), "cross_pkg/model") {
		t.Errorf("expected source-package import in output, got:\n%s", src)
	}
}

// TestEmitTemplate_AtomicApply_CrossPackage verifies Apply emission in
// cross-package mode: source-package types are qualified in the function
// signature, and no method wrapper is emitted (E-12).
// Covers: R-20, E-12
func TestEmitTemplate_AtomicApply_CrossPackage(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "cross_pkg_delta.go")

	cfg := Config{
		InputPkgs:          []string{"./testdata/emit/cross_pkg/model"},
		TargetStructs:      []string{"CrossPkgSnapshot"},
		OutPath:            outPath,
		OutPkgNameOverride: "deltas",
	}
	if err := New(cfg).Run(); err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	// R-11: generated file must be gofmt-clean as written.
	assertGofmtClean(t, outPath)

	src, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("reading output file: %v", err)
	}
	srcStr := string(src)

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, outPath, src, 0)
	if err != nil {
		t.Fatalf("generated file is not valid Go: %v\n--- source ---\n%s", err, src)
	}

	// Apply must be present as a package-level function.
	if findFuncDecl(f, "Apply") == nil {
		t.Fatalf("Apply function not found in generated file")
	}

	// Signature must qualify source-package types.
	if !strings.Contains(srcStr, "model.CrossPkgSnapshot") {
		t.Errorf("expected 'model.CrossPkgSnapshot' in Apply signature, got:\n%s", srcStr)
	}

	// No method wrapper in cross-package mode (E-12).
	if findMethodDecl(f, "CrossPkgSnapshot", "Apply") != nil {
		t.Errorf("Apply method wrapper must not be emitted in cross-package mode")
	}
}

// ── Compile-check helper ──────────────────────────────────────────────────────

// compileCheckEmit writes the generated source (plus a matching source
// Snapshot package) into an isolated temp module with a replace directive
// for go.resystems.io/eddt, then:
//   - asserts the generated delta.go is gofmt-clean (R-11),
//   - runs go test ./... to type-check and exercise Apply round-trip behaviour
//     (R-20) and HeaderAfterApply error propagation (E-19).
//
// The temp module reuses the eddt module's go.sum so that transitive
// dependencies (e.g. golang.org/x/crypto) resolve without network access.
func compileCheckEmit(t *testing.T, generatedSrc []byte) {
	t.Helper()

	tmpDir := t.TempDir()

	// Locate the eddt module root: two levels above the package directory
	// (internal/deltagen → internal → module root).
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	moduleRoot := filepath.Clean(filepath.Join(wd, "..", ".."))

	// Write the source Snapshot file.  Package must match the generated file
	// (atomic_all — the fixture package name, same-package mode).
	// The generated file refers to Inner and other local types defined here.
	srcCode := `package atomic_all

import eddt "go.resystems.io/eddt/runtime"

type Inner struct{ A, B int32 }

// Keep the eddt import live so the compiler does not drop it.
var _ eddt.Header

type AtomicAllSnapshot struct {
	eddt.Header
	Key     string           ` + "`eddt:\"entity.key\"`" + `
	Scalar  int32
	Pointer *string
	Struct  Inner
	Slice   []byte
	Map     map[string]int32
	Omitted string           ` + "`eddt:\"delta.omit\"`" + `
	Retired string           ` + "`eddt:\"delta.retired,since=2026-05-20\"`" + `
	Commute int32            ` + "`eddt:\"delta.commutative\"`" + `
}
`
	if err := os.WriteFile(filepath.Join(tmpDir, "snapshot.go"), []byte(srcCode), 0644); err != nil {
		t.Fatalf("write snapshot.go: %v", err)
	}

	// Write the generated Delta file.
	deltaPath := filepath.Join(tmpDir, "delta.go")
	if err := os.WriteFile(deltaPath, generatedSrc, 0644); err != nil {
		t.Fatalf("write delta.go: %v", err)
	}

	// R-11: the generated delta.go must be gofmt-clean as written.
	assertGofmtClean(t, deltaPath)

	// Write a behaviour test exercising Apply round-trip (R-20) and
	// HeaderAfterApply error propagation (E-19). The test is placed in the
	// atomic_all_test package (external test package) to prove the generated
	// package-level Apply function is callable from outside the package.
	testCode := `package atomic_all_test

import (
	"testing"
	"time"

	"atomic_all"
	eddt "go.resystems.io/eddt/runtime"
)

func TestApplyRoundTrip(t *testing.T) {
	id := eddt.EntityID{1}
	now := time.Now()

	var s atomic_all.AtomicAllSnapshot
	s.Header = eddt.Header{EntityID: id, ChainID: "c", Sequence: 1, EffectiveAt: now}
	s.Key = "k1"
	s.Scalar = 10
	s.Omitted = "omit-val"
	s.Retired = "retire-val"

	newScalar := int32(99)
	var d atomic_all.AtomicAllSnapshotDelta
	d.Header = eddt.Header{EntityID: id, ChainID: "c", Sequence: 2, EffectiveAt: now}
	d.SetScalar = &newScalar

	result, err := atomic_all.Apply(s, d)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if result.Header.Sequence != 2 {
		t.Errorf("Sequence: got %d, want 2", result.Header.Sequence)
	}
	if result.Header.EntityID != id {
		t.Errorf("EntityID not propagated")
	}
	if result.Key != s.Key {
		t.Errorf("Key not propagated: got %q", result.Key)
	}
	if result.Scalar != 99 {
		t.Errorf("Scalar (set): got %d, want 99", result.Scalar)
	}
	if result.Omitted != s.Omitted {
		t.Errorf("Omitted (suppressed): got %q, want %q", result.Omitted, s.Omitted)
	}
	if result.Retired != s.Retired {
		t.Errorf("Retired (suppressed): got %q, want %q", result.Retired, s.Retired)
	}
}

// TestApplyHeaderValidationError verifies that a non-monotone Sequence causes
// Apply to return a non-nil error (E-19: Apply returns (T, error)).
// Covers: R-20
func TestApplyHeaderValidationError(t *testing.T) {
	id := eddt.EntityID{1}
	now := time.Now()
	var s atomic_all.AtomicAllSnapshot
	s.Header = eddt.Header{EntityID: id, ChainID: "c", Sequence: 5, EffectiveAt: now}
	var d atomic_all.AtomicAllSnapshotDelta
	// d.Sequence == s.Sequence violates strict monotonicity.
	d.Header = eddt.Header{EntityID: id, ChainID: "c", Sequence: 5, EffectiveAt: now}
	_, err := atomic_all.Apply(s, d)
	if err == nil {
		t.Fatal("Apply: want error for non-monotone Sequence, got nil")
	}
}
`
	if err := os.WriteFile(filepath.Join(tmpDir, "apply_test.go"), []byte(testCode), 0644); err != nil {
		t.Fatalf("write apply_test.go: %v", err)
	}

	// Write go.mod with a replace directive pointing at the local module root.
	modContent := "module atomic_all\n\ngo 1.25.0\n\nrequire go.resystems.io/eddt v0.0.0\n\nreplace go.resystems.io/eddt => " + moduleRoot + "\n"
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte(modContent), 0644); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}

	// Copy the eddt module's go.sum so that transitive deps resolve locally
	// without network access (the replace directive covers the module itself,
	// but its deps are looked up via go.sum).
	goSum, err := os.ReadFile(filepath.Join(moduleRoot, "go.sum"))
	if err != nil {
		t.Fatalf("read eddt go.sum: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "go.sum"), goSum, 0644); err != nil {
		t.Fatalf("write go.sum: %v", err)
	}

	// go test ./... exercises Apply round-trip and HeaderAfterApply error
	// propagation. -mod=mod lets the toolchain auto-populate transitive require
	// entries without network access (the module cache has all eddt deps).
	// -count=1 disables test caching so the behaviour test always runs.
	runBuildCmd(t, tmpDir, "go", "test", "-mod=mod", "-count=1", "./...")
}

// runBuildCmd runs a command in dir and fatals with combined output on failure.
func runBuildCmd(t *testing.T, dir, command string, args ...string) {
	t.Helper()
	cmd := exec.Command(command, args...)
	cmd.Dir = dir
	var outBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &outBuf
	if err := cmd.Run(); err != nil {
		t.Fatalf("Command %q failed: %v\nOutput:\n%s", command+" "+strings.Join(args, " "), err, outBuf.String())
	}
}

// ── Test helper types and functions ──────────────────────────────────────────

// loadEmitFixture loads the named fixture from testdata/emit/<name> and parses
// the named struct. Fatals on any error.
func loadEmitFixture(t *testing.T, fixtureName, structName string) *ParsedSnapshot {
	t.Helper()
	pkgs, err := loadPackages([]string{"./testdata/emit/" + fixtureName}, false)
	if err != nil {
		t.Fatalf("loadEmitFixture(%q): %v", fixtureName, err)
	}
	ps, err := parseSnapshot(pkgs, structName, ParseOpts{})
	if err != nil {
		t.Fatalf("loadEmitFixture(%q, %q): parseSnapshot: %v", fixtureName, structName, err)
	}
	return ps
}

// viewNames returns the Name values of all fieldViews in sv, for error messages.
func viewNames(sv snapshotView) []string {
	names := make([]string, len(sv.Fields))
	for i, f := range sv.Fields {
		names[i] = f.Name
	}
	return names
}

// findStructDecl locates the ast.StructType node for the named top-level type
// in the given file.
func findStructDecl(f *ast.File, name string) *ast.StructType {
	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.TYPE {
			continue
		}
		for _, spec := range gd.Specs {
			ts, ok := spec.(*ast.TypeSpec)
			if !ok || ts.Name.Name != name {
				continue
			}
			if st, ok := ts.Type.(*ast.StructType); ok {
				return st
			}
		}
	}
	return nil
}

// structFieldNames returns the field / embed names from an ast.StructType.
// Embedded fields (anonymous) are returned by their type name.
func structFieldNames(st *ast.StructType) []string {
	var names []string
	for _, f := range st.Fields.List {
		if len(f.Names) == 0 {
			// Anonymous embed: derive name from type expression.
			names = append(names, exprName(f.Type))
		}
		for _, n := range f.Names {
			names = append(names, n.Name)
		}
	}
	return names
}

// exprName extracts a simple name from a type expression (selector or ident).
func exprName(e ast.Expr) string {
	switch v := e.(type) {
	case *ast.Ident:
		return v.Name
	case *ast.SelectorExpr:
		return v.Sel.Name
	}
	return ""
}

// contains reports whether s appears in the slice.
func contains(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

// assertGofmtClean fails if running gofmt -l on path produces any output
// (i.e. the file is not gofmt-clean as written). When the file is dirty the
// helper also runs gofmt -d and includes the diff in the failure message so
// the template defect is immediately diagnosable.
// Covers: R-11
func assertGofmtClean(t *testing.T, path string) {
	t.Helper()
	out, err := exec.Command("gofmt", "-l", path).CombinedOutput()
	if err != nil {
		t.Fatalf("gofmt -l %s: %v\n%s", path, err, out)
	}
	if len(bytes.TrimSpace(out)) != 0 {
		diff, _ := exec.Command("gofmt", "-d", path).CombinedOutput()
		t.Errorf("generated file %s is not gofmt-clean:\n%s", path, diff)
	}
}

// findFuncDecl locates a top-level function declaration by name in the file.
// Returns nil if not found.
func findFuncDecl(f *ast.File, name string) *ast.FuncDecl {
	for _, decl := range f.Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if ok && fd.Recv == nil && fd.Name.Name == name {
			return fd
		}
	}
	return nil
}

// findMethodDecl locates a method declaration with the given receiver type name
// and method name. Returns nil if not found.
func findMethodDecl(f *ast.File, recvType, methodName string) *ast.FuncDecl {
	for _, decl := range f.Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if !ok || fd.Recv == nil || fd.Name.Name != methodName {
			continue
		}
		if fd.Recv.NumFields() == 1 {
			if exprName(fd.Recv.List[0].Type) == recvType {
				return fd
			}
		}
	}
	return nil
}
