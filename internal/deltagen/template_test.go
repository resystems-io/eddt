package deltagen

// template_test.go exercises the EM-01 code-emission pipeline:
//
//   - TestBuildSnapshotView: table-driven view-construction unit tests covering
//     the §1.6.3 atomic-row emission matrix row by row (V01-V10).
//   - TestEmitTemplate_AtomicAll: end-to-end pipeline test against the
//     atomic_all fixture; asserts AST shape via go/parser.
//   - TestEmitTemplate_NestedNotYet: asserts that delta.nested triggers the
//     Phase-5 sentinel error.
//   - TestEmitTemplate_CrossPackageQualifier: asserts type-string qualification
//     in cross-package mode.
//   - TestEmitTemplate_CompileCheck: runs go build in an isolated temp module
//     with a replace directive to prove the generated TDelta type-checks against
//     the local runtime package.

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
		// V-number label, field name, expected DeltaName, expected DeltaType
		label     string
		fieldName string
		deltaName string
		deltaType string
		absent    bool // true when the field must NOT be in the view
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
		// V07 — delta.omit: suppressed, absent from view
		{label: "V07_Omitted", fieldName: "Omitted", absent: true},
		// V08 — delta.retired: suppressed, absent from view
		{label: "V08_Retired", fieldName: "Retired", absent: true},
		// V09 — delta.commutative: emits as if untagged (§9.5)
		{label: "V09_Commute", fieldName: "Commute", deltaName: "SetCommute", deltaType: "*int32"},
	}

	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			fv, ok := byName[tc.fieldName]
			if tc.absent {
				if ok {
					t.Errorf("field %q should be suppressed but appears in view", tc.fieldName)
				}
				return
			}
			if !ok {
				t.Fatalf("field %q missing from view; view has: %v", tc.fieldName, viewNames(sv))
				return
			}
			if fv.DeltaName != tc.deltaName {
				t.Errorf("DeltaName: got %q, want %q", fv.DeltaName, tc.deltaName)
			}
			if fv.DeltaType != tc.deltaType {
				t.Errorf("DeltaType: got %q, want %q", fv.DeltaType, tc.deltaType)
			}
		})
	}

	// The entity-key field (Key) must never appear in the Delta view — it is
	// extracted by the parse stage into KeyVar, not included in Fields.
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

// ── Group EM: end-to-end template tests ──────────────────────────────────────

// TestEmitTemplate_AtomicAll runs the full emit pipeline against the atomic_all
// fixture, parses the generated file with go/parser, and asserts AST structure:
// embedded Header, expected Set* fields, suppressed fields absent.
// Covers: R-19, R-25, E-14, E-15, E-16
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

	// Locate the TDelta type declaration.
	deltaDecl := findStructDecl(f, "AtomicAllSnapshotDelta")
	if deltaDecl == nil {
		t.Fatalf("AtomicAllSnapshotDelta type not found in generated file")
	}

	// Collect field names from the struct.
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

	// Suppressed fields must be absent.
	for _, absent := range []string{"SetOmitted", "SetRetired", "Omitted", "Retired"} {
		if contains(fields, absent) {
			t.Errorf("suppressed field %q should not appear in AtomicAllSnapshotDelta; fields: %v", absent, fields)
		}
	}

	// entity.key field must be absent (extracted by parse stage).
	if contains(fields, "Key") || contains(fields, "SetKey") {
		t.Errorf("entity-key field should not appear in TDelta; fields: %v", fields)
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

// ── Compile-check helper ──────────────────────────────────────────────────────

// compileCheckEmit writes the generated source (plus a matching source
// Snapshot package) into an isolated temp module with a replace directive
// for go.resystems.io/eddt, runs go build ./..., and fatals on failure.
// Mirrors the arrow-writer-gen setupIntegrationTest + runCmd pattern.
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
	if err := os.WriteFile(filepath.Join(tmpDir, "delta.go"), generatedSrc, 0644); err != nil {
		t.Fatalf("write delta.go: %v", err)
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

	// go build ./... must succeed.  -mod=mod lets the toolchain auto-populate
	// the transitive-require entries that Go 1.17+ strict mode demands without
	// requiring network access — the module cache already has all eddt deps.
	runBuildCmd(t, tmpDir, "go", "build", "-mod=mod", "./...")
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
