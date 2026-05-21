package deltagen

// template_test.go exercises the EM-01 / EM-02 / EM-03 code-emission pipeline:
//
//   - TestBuildSnapshotView: table-driven view-construction unit tests covering
//     the §1.6.3 atomic-row emission matrix row by row (V01-V09); also checks
//     Suppressed and UseReflectEq flags (EM-02, EM-03) and KeyName.
//   - TestBuildSnapshotView_KeyName: asserts sv.KeyName == "Key" for atomic_all.
//   - TestBuildSnapshotView_NeedsReflect: asserts NeedsReflect aggregate flag.
//   - TestEmitTemplate_AtomicAll: end-to-end pipeline test against the
//     atomic_all fixture; asserts TDelta AST shape, Apply function and method
//     wrapper presence, per-field Apply contributions (EM-02), Diff function
//     and method wrapper presence, per-field Diff contributions (EM-03).
//   - TestEmitTemplate_AtomicDiff_CrossPackage: asserts Diff in cross-package
//     mode: qualified signature, no method wrapper (E-12, EM-03).
//   - TestEmitTemplate_NoReflectImport_AllScalar: asserts that the "reflect"
//     import is absent when the Snapshot has only scalar fields.
//   - TestEmitTemplate_ReflectImport_WhenNeeded: asserts that the "reflect"
//     import is present when non-scalar fields exist.
//   - TestEmitTemplate_AtomicApply_CrossPackage: asserts Apply in cross-package
//     mode: qualified signature, no method wrapper (E-12, EM-02).
//   - TestEmitTemplate_NestedNotYet: asserts that delta.nested triggers the
//     Phase-5 sentinel error.
//   - TestEmitTemplate_CrossPackageQualifier: asserts type-string qualification
//     in cross-package mode.
//   - compileCheckEmit: runs go test in an isolated temp module with a replace
//     directive; exercises Apply round-trip and HeaderAfterApply error
//     propagation (EM-02); also exercises Diff round-trip, identity-diff
//     minimality, partial-diff minimality, and HeaderForDiff error propagation
//     (EM-03).

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
	qualifier, _, _ := buildImports([]*ParsedSnapshot{ps}, opts)

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
		// V-number label, field name, expected DeltaName, DeltaType, flags.
		label        string
		fieldName    string
		deltaName    string
		deltaType    string
		suppressed   bool // true for delta.omit / delta.retired: in view, Suppressed: true (EM-02)
		useReflectEq bool // true for non-scalar shapes: UseReflectEq: true (EM-03)
	}{
		// V01 — ShapeScalar: emits *T; no reflect comparison needed.
		{label: "V01_Scalar", fieldName: "Scalar", deltaName: "SetScalar", deltaType: "*int32"},
		// V02 — ShapePointer: emits **T; reflect.DeepEqual needed (pointer identity != value equality).
		{label: "V02_Pointer", fieldName: "Pointer", deltaName: "SetPointer", deltaType: "**string", useReflectEq: true},
		// V03 — ShapeStructValue (local, same-pkg): no qualifier, emits *Inner; reflect needed.
		{label: "V03_Struct", fieldName: "Struct", deltaName: "SetStruct", deltaType: "*Inner", useReflectEq: true},
		// V05 — ShapeSlice (atomic per E-15): []byte rendered as []byte; reflect needed.
		{label: "V05_Slice", fieldName: "Slice", deltaName: "SetSlice", deltaType: "*[]byte", useReflectEq: true},
		// V06 — ShapeMap (atomic per E-16): emits *map[string]int32; reflect needed.
		{label: "V06_Map", fieldName: "Map", deltaName: "SetMap", deltaType: "*map[string]int32", useReflectEq: true},
		// V07 — delta.omit: present in view with Suppressed: true; UseReflectEq irrelevant.
		{label: "V07_Omitted", fieldName: "Omitted", suppressed: true},
		// V08 — delta.retired: present in view with Suppressed: true; UseReflectEq irrelevant.
		{label: "V08_Retired", fieldName: "Retired", suppressed: true},
		// V09 — delta.commutative: emits as if untagged (§9.5); ShapeScalar, no reflect.
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
			// Non-suppressed: Suppressed must be false; check Set name, type, and reflect flag.
			if fv.Suppressed {
				t.Errorf("field %q: want Suppressed=false, got true", tc.fieldName)
			}
			if fv.DeltaName != tc.deltaName {
				t.Errorf("DeltaName: got %q, want %q", fv.DeltaName, tc.deltaName)
			}
			if fv.DeltaType != tc.deltaType {
				t.Errorf("DeltaType: got %q, want %q", fv.DeltaType, tc.deltaType)
			}
			if fv.UseReflectEq != tc.useReflectEq {
				t.Errorf("UseReflectEq: got %v, want %v", fv.UseReflectEq, tc.useReflectEq)
			}
		})
	}

	// Non-suppressed fields must appear in DiffFields; suppressed must not.
	diffByName := make(map[string]fieldView, len(sv.DiffFields))
	for _, f := range sv.DiffFields {
		diffByName[f.Name] = f
	}
	for _, name := range []string{"Scalar", "Pointer", "Struct", "Slice", "Map", "Commute"} {
		if _, ok := diffByName[name]; !ok {
			t.Errorf("non-suppressed field %q missing from DiffFields", name)
		}
	}
	for _, name := range []string{"Omitted", "Retired"} {
		if _, ok := diffByName[name]; ok {
			t.Errorf("suppressed field %q must not appear in DiffFields", name)
		}
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
	qualifier, _, _ := buildImports([]*ParsedSnapshot{ps}, opts)

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
	qualifier, _, _ := buildImports([]*ParsedSnapshot{ps}, opts)

	sv, err := buildSnapshotView(ps, qualifier)
	if err != nil {
		t.Fatalf("buildSnapshotView: %v", err)
	}
	if sv.KeyName != "Key" {
		t.Errorf("KeyName: got %q, want %q", sv.KeyName, "Key")
	}
}

// TestBuildSnapshotView_NeedsReflect verifies that sv.NeedsReflect is true when
// the fixture has non-scalar fields and false when only scalar / suppressed
// fields are present.
// Covers: R-21, E-20
func TestBuildSnapshotView_NeedsReflect(t *testing.T) {
	// atomic_all has pointer, struct-value, slice, and map fields → NeedsReflect.
	t.Run("HasNonScalar", func(t *testing.T) {
		ps := loadEmitFixture(t, "atomic_all", "AtomicAllSnapshot")
		opts := emitOpts{crossPackage: false}
		qualifier, _, _ := buildImports([]*ParsedSnapshot{ps}, opts)
		sv, err := buildSnapshotView(ps, qualifier)
		if err != nil {
			t.Fatalf("buildSnapshotView: %v", err)
		}
		if !sv.NeedsReflect {
			t.Errorf("NeedsReflect: got false, want true for fixture with non-scalar fields")
		}
	})

	// scalar_only has only scalar + suppressed fields → no reflect needed.
	t.Run("AllScalar", func(t *testing.T) {
		ps := loadEmitFixture(t, "scalar_only", "ScalarOnlySnapshot")
		opts := emitOpts{crossPackage: false}
		qualifier, _, _ := buildImports([]*ParsedSnapshot{ps}, opts)
		sv, err := buildSnapshotView(ps, qualifier)
		if err != nil {
			t.Fatalf("buildSnapshotView: %v", err)
		}
		if sv.NeedsReflect {
			t.Errorf("NeedsReflect: got true, want false for all-scalar fixture")
		}
	})
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

	// ── Diff function shape (EM-03) ───────────────────────────────────────────

	diffFn := findFuncDecl(f, "Diff")
	if diffFn == nil {
		t.Fatalf("Diff function not found in generated file")
	}

	// Signature: func Diff(a, b AtomicAllSnapshot) (AtomicAllSnapshotDelta, error)
	if diffFn.Type.Params.NumFields() != 2 {
		t.Errorf("Diff: want 2 params, got %d", diffFn.Type.Params.NumFields())
	}
	if diffFn.Type.Results.NumFields() != 2 {
		t.Errorf("Diff: want 2 results, got %d", diffFn.Type.Results.NumFields())
	}

	// Body must contain the HeaderForDiff call.
	if !strings.Contains(srcStr, "runtime.HeaderForDiff(a.Header, b.Header)") {
		t.Errorf("Diff body missing runtime.HeaderForDiff(a.Header, b.Header)")
	}

	// Scalar field uses !=; non-scalar fields use reflect.DeepEqual.
	if !strings.Contains(srcStr, "a.Scalar != b.Scalar") {
		t.Errorf("Diff body missing scalar comparison: a.Scalar != b.Scalar")
	}
	for _, name := range []string{"Pointer", "Struct", "Slice", "Map", "Commute"} {
		// Commute is ShapeScalar (int32), so it uses !=; the others use DeepEqual.
		if name == "Commute" {
			if !strings.Contains(srcStr, "a.Commute != b.Commute") {
				t.Errorf("Diff body missing scalar comparison for commutative field: a.Commute != b.Commute")
			}
			continue
		}
		if !strings.Contains(srcStr, "reflect.DeepEqual(a."+name+", b."+name+")") {
			t.Errorf("Diff body missing reflect.DeepEqual for %s", name)
		}
	}

	// Suppressed and entity-key fields must NOT appear in the Diff body.
	for _, name := range []string{"Omitted", "Retired", "Key"} {
		if strings.Contains(srcStr, "a."+name) || strings.Contains(srcStr, "b."+name) {
			t.Errorf("Diff body must not reference suppressed/key field %q", name)
		}
	}

	// "reflect" import must be present (non-scalar fields trigger it).
	if !strings.Contains(srcStr, `"reflect"`) {
		t.Errorf("generated file missing \"reflect\" import")
	}

	// ── Diff method wrapper present (same-package mode, E-12) ────────────────

	if findMethodDecl(f, "AtomicAllSnapshot", "Diff") == nil {
		t.Errorf("Diff method wrapper not found (expected in same-package mode)")
	}

	// ── Coalesce function shape (EM-04) ───────────────────────────────────────

	coalesceFn := findFuncDecl(f, "Coalesce")
	if coalesceFn == nil {
		t.Fatalf("Coalesce function not found in generated file")
	}

	// Signature: func Coalesce(s AtomicAllSnapshot, ds []AtomicAllSnapshotDelta) (AtomicAllSnapshot, error)
	if coalesceFn.Type.Params.NumFields() != 2 {
		t.Errorf("Coalesce: want 2 params, got %d", coalesceFn.Type.Params.NumFields())
	}
	if coalesceFn.Type.Results.NumFields() != 2 {
		t.Errorf("Coalesce: want 2 results, got %d", coalesceFn.Type.Results.NumFields())
	}

	// Body must contain a for-range loop over ds with an Apply call.
	if !strings.Contains(srcStr, "for _, d := range ds") {
		t.Errorf("Coalesce body missing for-range loop: for _, d := range ds")
	}
	if !strings.Contains(srcStr, "Apply(result, d)") {
		t.Errorf("Coalesce body missing Apply(result, d)")
	}

	// ── Coalesce method wrapper present (same-package mode, E-12) ────────────

	if findMethodDecl(f, "AtomicAllSnapshot", "Coalesce") == nil {
		t.Errorf("Coalesce method wrapper not found (expected in same-package mode)")
	}
	if !strings.Contains(srcStr, "return Coalesce(s, ds)") {
		t.Errorf("Coalesce method wrapper body missing 'return Coalesce(s, ds)'")
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

// TestEmitTemplate_AtomicDiff_CrossPackage verifies Diff emission in
// cross-package mode: source-package types are qualified in the function
// signature, and no Diff method wrapper is emitted (E-12).
// Covers: R-21, E-12, E-20
func TestEmitTemplate_AtomicDiff_CrossPackage(t *testing.T) {
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

	// Diff must be present as a package-level function.
	if findFuncDecl(f, "Diff") == nil {
		t.Fatalf("Diff function not found in generated file")
	}

	// Signature must qualify source-package types (E-12).
	if !strings.Contains(srcStr, "model.CrossPkgSnapshot") {
		t.Errorf("expected 'model.CrossPkgSnapshot' in Diff signature, got:\n%s", srcStr)
	}
	// Diff(a, b T) (TDelta, error) — both params use the qualified type.
	if !strings.Contains(srcStr, "func Diff(a, b model.CrossPkgSnapshot)") {
		t.Errorf("expected 'func Diff(a, b model.CrossPkgSnapshot)' in generated file, got:\n%s", srcStr)
	}

	// No Diff method wrapper in cross-package mode (E-12).
	if findMethodDecl(f, "CrossPkgSnapshot", "Diff") != nil {
		t.Errorf("Diff method wrapper must not be emitted in cross-package mode")
	}

	// CrossPkgSnapshot has Location Address (ShapeStructValue) → reflect needed.
	if !strings.Contains(srcStr, `"reflect"`) {
		t.Errorf("expected \"reflect\" import for non-scalar field, got:\n%s", srcStr)
	}
}

// TestEmitTemplate_AtomicCoalesce_CrossPackage verifies Coalesce emission in
// cross-package mode: source-package types are qualified in the function
// signature, and no Coalesce method wrapper is emitted (E-12).
// Covers: R-22, E-12, E-21
func TestEmitTemplate_AtomicCoalesce_CrossPackage(t *testing.T) {
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

	// Coalesce must be present as a package-level function.
	if findFuncDecl(f, "Coalesce") == nil {
		t.Fatalf("Coalesce function not found in generated file")
	}

	// Signature must qualify source-package types for both parameter and return (E-12).
	if !strings.Contains(srcStr, "func Coalesce(s model.CrossPkgSnapshot, ds []CrossPkgSnapshotDelta) (model.CrossPkgSnapshot, error)") {
		t.Errorf("expected qualified Coalesce signature, got:\n%s", srcStr)
	}

	// No Coalesce method wrapper in cross-package mode (E-12).
	if findMethodDecl(f, "CrossPkgSnapshot", "Coalesce") != nil {
		t.Errorf("Coalesce method wrapper must not be emitted in cross-package mode")
	}
}

// TestEmitTemplate_NoReflectImport_AllScalar verifies that the "reflect" import
// is absent when the Snapshot contains only scalar and suppressed fields.
// Covers: R-21, E-20
func TestEmitTemplate_NoReflectImport_AllScalar(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "scalar_only_delta.go")

	cfg := Config{
		InputPkgs:     []string{"./testdata/emit/scalar_only"},
		TargetStructs: []string{"ScalarOnlySnapshot"},
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
	srcStr := string(src)

	// No "reflect" import must be present for an all-scalar Snapshot.
	if strings.Contains(srcStr, `"reflect"`) {
		t.Errorf("unexpected \"reflect\" import for all-scalar Snapshot:\n%s", srcStr)
	}
	// No reflect.DeepEqual call anywhere in the Diff body.
	if strings.Contains(srcStr, "reflect.DeepEqual") {
		t.Errorf("unexpected reflect.DeepEqual call for all-scalar Snapshot:\n%s", srcStr)
	}
	// Scalar comparisons use !=.
	if !strings.Contains(srcStr, "a.Count != b.Count") {
		t.Errorf("expected '!= ' comparison for scalar Count field:\n%s", srcStr)
	}
}

// TestEmitTemplate_ReflectImport_WhenNeeded verifies that the "reflect" import
// is present when the Snapshot contains non-scalar fields.
// Covers: R-21, E-20
func TestEmitTemplate_ReflectImport_WhenNeeded(t *testing.T) {
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
	srcStr := string(src)

	// "reflect" import must be present.
	if !strings.Contains(srcStr, `"reflect"`) {
		t.Errorf("expected \"reflect\" import for Snapshot with non-scalar fields:\n%s", srcStr)
	}
	// At least one reflect.DeepEqual call must be in the Diff body.
	if !strings.Contains(srcStr, "reflect.DeepEqual") {
		t.Errorf("expected reflect.DeepEqual in Diff body for non-scalar fields:\n%s", srcStr)
	}
}

// ── Compile-check helper ──────────────────────────────────────────────────────

// compileCheckEmit writes the generated source (plus a matching source
// Snapshot package) into an isolated temp module with a replace directive
// for go.resystems.io/eddt, then:
//   - asserts the generated delta.go is gofmt-clean (R-11),
//   - runs go test ./... to type-check and exercise Apply round-trip behaviour
//     (R-20) and HeaderAfterApply error propagation (E-19),
//   - exercises Diff round-trip Apply(a, Diff(a, b)) == b across all five
//     atomic shapes (R-28), identity-diff Set* nilness (R-29 / E-06),
//     partial-diff minimality, and HeaderForDiff error propagation (E-20).
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

	// Write a behaviour test exercising Diff round-trip (R-28), identity-diff
	// minimality (R-29 / E-06), partial-diff minimality, and HeaderForDiff
	// error propagation (E-20).
	diffTestCode := `package atomic_all_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"atomic_all"
	eddt "go.resystems.io/eddt/runtime"
)

// makeSnap constructs an AtomicAllSnapshot with a unique filler value per
// field so that two snapshots with different fillers differ in every payload
// field. seq and t set the chain position; id is the EntityID.
func makeSnap(id eddt.EntityID, seq uint64, t time.Time, filler int) atomic_all.AtomicAllSnapshot {
	ptr := "hello" + string(rune('A'+filler))
	inner := atomic_all.Inner{A: int32(filler), B: int32(filler + 1)}
	sl := []byte{byte(filler), byte(filler + 1), byte(filler + 2)}
	m := map[string]int32{"k": int32(filler)}
	var s atomic_all.AtomicAllSnapshot
	s.Header = eddt.Header{EntityID: id, ChainID: "c", Sequence: seq, EffectiveAt: t}
	s.Key = "key"
	s.Scalar = int32(filler * 10)
	s.Pointer = &ptr
	s.Struct = inner
	s.Slice = sl
	s.Map = m
	s.Commute = int32(filler * 3)
	s.Omitted = "omitted"
	s.Retired = "retired"
	return s
}

// TestDiffApplyRoundTrip verifies Apply(a, Diff(a, b)) payload-equals b across
// all five atomic shapes plus delta.commutative (R-28).
// Suppressed fields must equal a (propagated by Apply; Diff emits nothing for them).
// Header equality is not asserted — it advances by construction (E-06).
// Covers: R-28, R-21, E-20
func TestDiffApplyRoundTrip(t *testing.T) {
	id := eddt.EntityID{1}
	t1 := time.Now()
	t2 := t1.Add(time.Second)
	a := makeSnap(id, 1, t1, 1)
	b := makeSnap(id, 2, t2, 2)

	delta, err := atomic_all.Diff(a, b)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}

	result, err := atomic_all.Apply(a, delta)
	if err != nil {
		t.Fatalf("Apply(a, Diff(a,b)): %v", err)
	}

	// Payload equality with b for all atomic + commutative fields.
	if result.Scalar != b.Scalar {
		t.Errorf("Scalar: got %d, want %d", result.Scalar, b.Scalar)
	}
	if !reflect.DeepEqual(result.Pointer, b.Pointer) {
		t.Errorf("Pointer: got %v, want %v", result.Pointer, b.Pointer)
	}
	if result.Struct != b.Struct {
		t.Errorf("Struct: got %v, want %v", result.Struct, b.Struct)
	}
	if !reflect.DeepEqual(result.Slice, b.Slice) {
		t.Errorf("Slice: got %v, want %v", result.Slice, b.Slice)
	}
	if !reflect.DeepEqual(result.Map, b.Map) {
		t.Errorf("Map: got %v, want %v", result.Map, b.Map)
	}
	if result.Commute != b.Commute {
		t.Errorf("Commute: got %d, want %d", result.Commute, b.Commute)
	}
	// Entity-key propagated from a (== b.Key by HeaderForDiff EntityID contract).
	if result.Key != a.Key {
		t.Errorf("Key: got %q, want %q", result.Key, a.Key)
	}
	// Suppressed fields propagate from a (Diff emits nothing for them).
	if result.Omitted != a.Omitted {
		t.Errorf("Omitted (suppressed): got %q, want %q", result.Omitted, a.Omitted)
	}
	if result.Retired != a.Retired {
		t.Errorf("Retired (suppressed): got %q, want %q", result.Retired, a.Retired)
	}
}

// TestDiffIdentity verifies Diff(a, a) produces a TDelta with all Set* fields
// nil (minimality of the identity diff, R-29). Apply(a, Diff(a, a)) is NOT
// called — that would violate HeaderAfterApply's strict Sequence monotonicity
// precondition (E-06: identity diff Sequence == a.Sequence).
// Covers: R-29, E-06
func TestDiffIdentity(t *testing.T) {
	id := eddt.EntityID{1}
	now := time.Now()
	a := makeSnap(id, 1, now, 5)

	delta, err := atomic_all.Diff(a, a)
	if err != nil {
		t.Fatalf("Diff(a, a): %v", err)
	}

	// Every Set* field must be nil — no change between identical snapshots.
	if delta.SetScalar != nil {
		t.Errorf("SetScalar: want nil for identity diff, got %v", delta.SetScalar)
	}
	if delta.SetPointer != nil {
		t.Errorf("SetPointer: want nil for identity diff, got %v", delta.SetPointer)
	}
	if delta.SetStruct != nil {
		t.Errorf("SetStruct: want nil for identity diff, got %v", delta.SetStruct)
	}
	if delta.SetSlice != nil {
		t.Errorf("SetSlice: want nil for identity diff, got %v", delta.SetSlice)
	}
	if delta.SetMap != nil {
		t.Errorf("SetMap: want nil for identity diff, got %v", delta.SetMap)
	}
	if delta.SetCommute != nil {
		t.Errorf("SetCommute: want nil for identity diff, got %v", delta.SetCommute)
	}
	// Note: Apply(a, delta) is intentionally not called here.
	// delta.Header.Sequence == a.Header.Sequence, violating HeaderAfterApply's
	// strict monotonicity precondition (E-06).
}

// TestDiffPartial verifies that Diff produces a minimal delta: only the one
// field that differs between a and c has a non-nil Set* value.
// Covers: R-28, R-21
func TestDiffPartial(t *testing.T) {
	id := eddt.EntityID{1}
	t1 := time.Now()
	t2 := t1.Add(time.Second)
	a := makeSnap(id, 1, t1, 1)
	// c copies a exactly, then changes only Scalar.
	c := a
	c.Header = eddt.Header{EntityID: id, ChainID: "c", Sequence: 2, EffectiveAt: t2}
	c.Scalar = a.Scalar + 100

	delta, err := atomic_all.Diff(a, c)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}

	// Only SetScalar must be non-nil.
	if delta.SetScalar == nil || *delta.SetScalar != c.Scalar {
		t.Errorf("SetScalar: want &%d, got %v", c.Scalar, delta.SetScalar)
	}
	// All other Set* fields must be nil.
	if delta.SetPointer != nil {
		t.Errorf("SetPointer: want nil (unchanged), got non-nil")
	}
	if delta.SetStruct != nil {
		t.Errorf("SetStruct: want nil (unchanged), got non-nil")
	}
	if delta.SetSlice != nil {
		t.Errorf("SetSlice: want nil (unchanged), got non-nil")
	}
	if delta.SetMap != nil {
		t.Errorf("SetMap: want nil (unchanged), got non-nil")
	}
	if delta.SetCommute != nil {
		t.Errorf("SetCommute: want nil (unchanged), got non-nil")
	}
}

// TestDiffHeaderForDiffError verifies that Diff returns a non-nil error when
// HeaderForDiff rejects the inputs (e.g. mismatched EntityID).
// This pins the (TDelta, error) signature behaviour under E-20.
// Covers: R-21, E-20
func TestDiffHeaderForDiffError(t *testing.T) {
	id1 := eddt.EntityID{1}
	id2 := eddt.EntityID{2}
	now := time.Now()
	a := makeSnap(id1, 1, now, 1)
	b := makeSnap(id2, 2, now.Add(time.Second), 2) // different EntityID

	_, err := atomic_all.Diff(a, b)
	if err == nil {
		t.Fatal("Diff: want error for mismatched EntityID, got nil")
	}
	if !strings.Contains(err.Error(), "EntityID") {
		t.Errorf("error should mention EntityID, got: %v", err)
	}
}

// TestDiffApplyRoundTrip_FromZero verifies Apply(zero, Diff(zero, x)) == x,
// where zero has valid Header metadata but all payload fields at their Go zero
// values (Scalar=0, Pointer=nil, Struct=Inner{}, Slice=nil, Map=nil,
// Commute=0). This exercises the nil-pointer, nil-slice, and nil-map branches
// of reflect.DeepEqual in the Diff body — cases not covered by
// TestDiffApplyRoundTrip, which uses non-zero payload on both sides.
// Covers: R-28
func TestDiffApplyRoundTrip_FromZero(t *testing.T) {
	id := eddt.EntityID{1}
	t1 := time.Now()
	t2 := t1.Add(time.Second)

	// zero: valid header, all payload fields at Go zero values.
	var zero atomic_all.AtomicAllSnapshot
	zero.Header = eddt.Header{EntityID: id, ChainID: "c", Sequence: 1, EffectiveAt: t1}
	zero.Key = "key"
	// Scalar=0, Pointer=nil, Struct=Inner{}, Slice=nil, Map=nil, Commute=0

	x := makeSnap(id, 2, t2, 3)

	delta, err := atomic_all.Diff(zero, x)
	if err != nil {
		t.Fatalf("Diff(zero, x): %v", err)
	}

	result, err := atomic_all.Apply(zero, delta)
	if err != nil {
		t.Fatalf("Apply(zero, Diff(zero, x)): %v", err)
	}

	// All payload fields must equal x.
	if result.Scalar != x.Scalar {
		t.Errorf("Scalar: got %d, want %d", result.Scalar, x.Scalar)
	}
	if !reflect.DeepEqual(result.Pointer, x.Pointer) {
		t.Errorf("Pointer: got %v, want %v", result.Pointer, x.Pointer)
	}
	if result.Struct != x.Struct {
		t.Errorf("Struct: got %v, want %v", result.Struct, x.Struct)
	}
	if !reflect.DeepEqual(result.Slice, x.Slice) {
		t.Errorf("Slice: got %v, want %v", result.Slice, x.Slice)
	}
	if !reflect.DeepEqual(result.Map, x.Map) {
		t.Errorf("Map: got %v, want %v", result.Map, x.Map)
	}
	if result.Commute != x.Commute {
		t.Errorf("Commute: got %d, want %d", result.Commute, x.Commute)
	}
}
`
	if err := os.WriteFile(filepath.Join(tmpDir, "diff_test.go"), []byte(diffTestCode), 0644); err != nil {
		t.Fatalf("write diff_test.go: %v", err)
	}

	// coalesceTestCode exercises the generated Coalesce function against the
	// atomic_all fixture. makeSnap and other helpers are defined in diff_test.go
	// (same package atomic_all_test), so they are directly accessible here.
	coalesceTestCode := `package atomic_all_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"atomic_all"
	eddt "go.resystems.io/eddt/runtime"
)

// TestCoalesceEmpty verifies that Coalesce with a nil or empty delta slice
// returns (s, nil) without advancing the Header — the monoidal identity
// element of the fold: Coalesce(x, []) == x (byte-equal).
// Covers: R-22, R-30, E-21
func TestCoalesceEmpty(t *testing.T) {
	id := eddt.EntityID{1}
	s := makeSnap(id, 1, time.Now(), 5)

	// nil slice
	got, err := atomic_all.Coalesce(s, nil)
	if err != nil {
		t.Fatalf("Coalesce(s, nil): unexpected error: %v", err)
	}
	if !reflect.DeepEqual(got, s) {
		t.Errorf("Coalesce(s, nil): result differs from input")
	}

	// empty non-nil slice
	got2, err2 := atomic_all.Coalesce(s, []atomic_all.AtomicAllSnapshotDelta{})
	if err2 != nil {
		t.Fatalf("Coalesce(s, []): unexpected error: %v", err2)
	}
	if !reflect.DeepEqual(got2, s) {
		t.Errorf("Coalesce(s, []): result differs from input")
	}
}

// TestCoalesceSingleDelta_EqualsApply verifies that Coalesce with a single
// delta is equivalent to a direct Apply call: the one-step fold equals Apply.
// Covers: R-22, R-30, E-21
func TestCoalesceSingleDelta_EqualsApply(t *testing.T) {
	id := eddt.EntityID{1}
	t1 := time.Now()
	t2 := t1.Add(time.Second)
	a := makeSnap(id, 1, t1, 1)
	b := makeSnap(id, 2, t2, 2)

	d, err := atomic_all.Diff(a, b)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}

	viaCoalesce, err := atomic_all.Coalesce(a, []atomic_all.AtomicAllSnapshotDelta{d})
	if err != nil {
		t.Fatalf("Coalesce: %v", err)
	}

	viaApply, err := atomic_all.Apply(a, d)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if !reflect.DeepEqual(viaCoalesce, viaApply) {
		t.Errorf("Coalesce(a,[d]) != Apply(a,d)")
	}
}

// TestCoalesceMultiStep_ProgressionOfChanges verifies that a Coalesce fold
// across six snapshots — where each transition mutates exactly one atomic
// shape — produces the final snapshot's payload. This covers all five atomic
// shapes (scalar, pointer, struct, slice, map) plus delta.commutative, and
// verifies that suppressed fields propagate from the seed unchanged.
// Covers: R-22, R-30, E-21
func TestCoalesceMultiStep_ProgressionOfChanges(t *testing.T) {
	id := eddt.EntityID{1}
	t0 := time.Now()
	tick := func(i int) time.Time { return t0.Add(time.Duration(i) * time.Second) }

	// s1 is the seed; each subsequent snapshot changes exactly one payload field.
	s1 := makeSnap(id, 1, tick(1), 1)

	s2 := s1
	s2.Header = eddt.Header{EntityID: id, ChainID: "c", Sequence: 2, EffectiveAt: tick(2)}
	s2.Scalar = 999

	ptr3 := "new_ptr"
	s3 := s2
	s3.Header = eddt.Header{EntityID: id, ChainID: "c", Sequence: 3, EffectiveAt: tick(3)}
	s3.Pointer = &ptr3

	s4 := s3
	s4.Header = eddt.Header{EntityID: id, ChainID: "c", Sequence: 4, EffectiveAt: tick(4)}
	s4.Struct = atomic_all.Inner{A: 100, B: 200}

	s5 := s4
	s5.Header = eddt.Header{EntityID: id, ChainID: "c", Sequence: 5, EffectiveAt: tick(5)}
	s5.Slice = []byte{9, 8, 7}

	s6 := s5
	s6.Header = eddt.Header{EntityID: id, ChainID: "c", Sequence: 6, EffectiveAt: tick(6)}
	s6.Map = map[string]int32{"z": 42}
	s6.Commute = 77

	computeDiff := func(a, b atomic_all.AtomicAllSnapshot) atomic_all.AtomicAllSnapshotDelta {
		t.Helper()
		d, err := atomic_all.Diff(a, b)
		if err != nil {
			t.Fatalf("Diff: %v", err)
		}
		return d
	}

	ds := []atomic_all.AtomicAllSnapshotDelta{
		computeDiff(s1, s2),
		computeDiff(s2, s3),
		computeDiff(s3, s4),
		computeDiff(s4, s5),
		computeDiff(s5, s6),
	}

	result, err := atomic_all.Coalesce(s1, ds)
	if err != nil {
		t.Fatalf("Coalesce: %v", err)
	}

	// Payload must equal s6.
	if result.Scalar != s6.Scalar {
		t.Errorf("Scalar: got %d, want %d", result.Scalar, s6.Scalar)
	}
	if !reflect.DeepEqual(result.Pointer, s6.Pointer) {
		t.Errorf("Pointer: got %v, want %v", result.Pointer, s6.Pointer)
	}
	if result.Struct != s6.Struct {
		t.Errorf("Struct: got %v, want %v", result.Struct, s6.Struct)
	}
	if !reflect.DeepEqual(result.Slice, s6.Slice) {
		t.Errorf("Slice: got %v, want %v", result.Slice, s6.Slice)
	}
	if !reflect.DeepEqual(result.Map, s6.Map) {
		t.Errorf("Map: got %v, want %v", result.Map, s6.Map)
	}
	if result.Commute != s6.Commute {
		t.Errorf("Commute: got %d, want %d", result.Commute, s6.Commute)
	}

	// Suppressed fields propagate from the seed (Apply carries s.X at every step).
	if result.Omitted != s1.Omitted {
		t.Errorf("Omitted: got %q, want %q (seed value)", result.Omitted, s1.Omitted)
	}
	if result.Retired != s1.Retired {
		t.Errorf("Retired: got %q, want %q (seed value)", result.Retired, s1.Retired)
	}
}

// TestCoalesceNoOpPayload verifies the spirit of Coalesce(x, [Diff(y,y)]) ==
// x (payload-wise). Taken literally, Diff(y,y) collides with E-06 when
// y.Sequence == x.Sequence because HeaderAfterApply requires strict monotonicity.
// We therefore construct y with y.Sequence > x.Sequence and identical payload:
// Diff(y,y) has all Set* nil (identity-diff, R-29), and applying it to x leaves
// the payload unchanged while advancing the Header.
// Covers: R-22, R-30, E-21
func TestCoalesceNoOpPayload(t *testing.T) {
	id := eddt.EntityID{1}
	t1 := time.Now()
	t2 := t1.Add(time.Second)

	x := makeSnap(id, 1, t1, 5)

	// y: same EntityID/ChainID/payload, but Sequence advanced past x.Sequence.
	y := x
	y.Header = eddt.Header{EntityID: id, ChainID: "c", Sequence: 2, EffectiveAt: t2}

	noop, err := atomic_all.Diff(y, y)
	if err != nil {
		t.Fatalf("Diff(y,y): %v", err)
	}
	// Identity-diff: all Set* must be nil (R-29 / E-06 documents this).
	if noop.SetScalar != nil || noop.SetPointer != nil || noop.SetStruct != nil ||
		noop.SetSlice != nil || noop.SetMap != nil || noop.SetCommute != nil {
		t.Error("Diff(y,y): expected all Set* nil (identity-diff)")
	}

	result, err := atomic_all.Coalesce(x, []atomic_all.AtomicAllSnapshotDelta{noop})
	if err != nil {
		t.Fatalf("Coalesce(x,[Diff(y,y)]): %v", err)
	}

	// Payload must equal x (no-op delta does not change any field).
	if result.Scalar != x.Scalar {
		t.Errorf("Scalar: got %d, want %d", result.Scalar, x.Scalar)
	}
	if !reflect.DeepEqual(result.Pointer, x.Pointer) {
		t.Errorf("Pointer: got %v, want %v", result.Pointer, x.Pointer)
	}
	if result.Struct != x.Struct {
		t.Errorf("Struct: got %v, want %v", result.Struct, x.Struct)
	}
	if !reflect.DeepEqual(result.Slice, x.Slice) {
		t.Errorf("Slice: got %v, want %v", result.Slice, x.Slice)
	}
	if !reflect.DeepEqual(result.Map, x.Map) {
		t.Errorf("Map: got %v, want %v", result.Map, x.Map)
	}
	if result.Commute != x.Commute {
		t.Errorf("Commute: got %d, want %d", result.Commute, x.Commute)
	}
	// Header advances to y.Sequence (Apply always advances the Header).
	if result.Header.Sequence != y.Header.Sequence {
		t.Errorf("Sequence: got %d, want %d", result.Header.Sequence, y.Header.Sequence)
	}
}

// TestCoalesceAssociativity verifies that Coalesce is associative (chunkable):
// Coalesce(Coalesce(s, ds1), ds2) == Coalesce(s, append(ds1, ds2...)).
// This confirms that the fold can be split at any point with identical results.
// Covers: R-22, R-30, E-21
func TestCoalesceAssociativity(t *testing.T) {
	id := eddt.EntityID{1}
	t0 := time.Now()
	tick := func(i int) time.Time { return t0.Add(time.Duration(i) * time.Second) }

	snaps := make([]atomic_all.AtomicAllSnapshot, 6)
	for i := range snaps {
		snaps[i] = makeSnap(id, uint64(i+1), tick(i+1), i+1)
	}

	ds := make([]atomic_all.AtomicAllSnapshotDelta, 5)
	for i := range ds {
		d, err := atomic_all.Diff(snaps[i], snaps[i+1])
		if err != nil {
			t.Fatalf("Diff(snaps[%d], snaps[%d]): %v", i, i+1, err)
		}
		ds[i] = d
	}

	// Full fold: Coalesce(snaps[0], ds[0..4]).
	full, err := atomic_all.Coalesce(snaps[0], ds)
	if err != nil {
		t.Fatalf("Coalesce(full): %v", err)
	}

	// Chunked fold: first two deltas, then last three.
	mid, err := atomic_all.Coalesce(snaps[0], ds[:2])
	if err != nil {
		t.Fatalf("Coalesce(first 2): %v", err)
	}
	full2, err := atomic_all.Coalesce(mid, ds[2:])
	if err != nil {
		t.Fatalf("Coalesce(last 3): %v", err)
	}

	// Both folds must produce the same result (Header and payload).
	if !reflect.DeepEqual(full, full2) {
		t.Errorf("associativity violated: full fold != chunked fold")
	}
}

// TestCoalesceErrorAtFirst verifies that a delta with a mismatched EntityID as
// the first element causes Coalesce to return (zero T, non-nil error). No
// subsequent deltas are applied. Pins the E-21 zero-return-on-error contract.
// Covers: R-22, E-21
func TestCoalesceErrorAtFirst(t *testing.T) {
	id := eddt.EntityID{1}
	otherId := eddt.EntityID{2}
	t1 := time.Now()
	s := makeSnap(id, 1, t1, 1)

	var bad atomic_all.AtomicAllSnapshotDelta
	bad.Header = eddt.Header{EntityID: otherId, ChainID: "c", Sequence: 2, EffectiveAt: t1.Add(time.Second)}

	result, err := atomic_all.Coalesce(s, []atomic_all.AtomicAllSnapshotDelta{bad})
	if err == nil {
		t.Fatal("expected error for mismatched EntityID, got nil")
	}
	if !strings.Contains(err.Error(), "EntityID") {
		t.Errorf("expected error to mention EntityID, got: %v", err)
	}

	// E-21: zero T returned on error, not a partial result.
	var zero atomic_all.AtomicAllSnapshot
	if !reflect.DeepEqual(result, zero) {
		t.Errorf("expected zero AtomicAllSnapshot on error")
	}
}

// TestCoalesceErrorMidFold verifies that a sequence regression in the second
// delta stops the fold and returns (zero T, non-nil error). The first delta is
// valid and has already been applied. Coalesce returns the zero value rather
// than the partial intermediate state — pins the E-21 contract.
// Covers: R-22, E-21
func TestCoalesceErrorMidFold(t *testing.T) {
	id := eddt.EntityID{1}
	t0 := time.Now()
	tick := func(i int) time.Time { return t0.Add(time.Duration(i) * time.Second) }

	s1 := makeSnap(id, 1, tick(1), 1)
	s2 := makeSnap(id, 2, tick(2), 2)
	d1, err := atomic_all.Diff(s1, s2)
	if err != nil {
		t.Fatalf("Diff(s1,s2): %v", err)
	}

	// bad: regression — Sequence 1 is <= the current state Sequence of 2.
	var bad atomic_all.AtomicAllSnapshotDelta
	bad.Header = eddt.Header{EntityID: id, ChainID: "c", Sequence: 1, EffectiveAt: tick(1)}

	// d3 would be applied after bad, but is never reached.
	var d3 atomic_all.AtomicAllSnapshotDelta
	d3.Header = eddt.Header{EntityID: id, ChainID: "c", Sequence: 5, EffectiveAt: tick(5)}

	result, err := atomic_all.Coalesce(s1, []atomic_all.AtomicAllSnapshotDelta{d1, bad, d3})
	if err == nil {
		t.Fatal("expected error for sequence regression, got nil")
	}

	// E-21: zero T returned on error, not the partial intermediate state.
	var zero atomic_all.AtomicAllSnapshot
	if !reflect.DeepEqual(result, zero) {
		t.Errorf("expected zero AtomicAllSnapshot on error, got non-zero result")
	}
}
`
	if err := os.WriteFile(filepath.Join(tmpDir, "coalesce_test.go"), []byte(coalesceTestCode), 0644); err != nil {
		t.Fatalf("write coalesce_test.go: %v", err)
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
