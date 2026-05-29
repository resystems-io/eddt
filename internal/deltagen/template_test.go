package deltagen

// template_test.go exercises the R-DG-015 / R-DG-012, R-DG-013 / R-DG-012, R-DG-013 code-emission pipeline:
//
//   - TestBuildSnapshotView: table-driven view-construction unit tests covering
//     the §1.6.3 atomic-row emission matrix row by row (V01-V09); also checks
//     Suppressed and UseReflectEq flags (R-DG-012, R-DG-013, R-DG-012, R-DG-013) and KeyName.
//   - TestBuildSnapshotView_KeyName: asserts sv.KeyName == "Key" for atomic_all.
//   - TestBuildSnapshotView_NeedsReflect: asserts NeedsReflect aggregate flag.
//   - TestEmitTemplate_AtomicAll: end-to-end pipeline test against the
//     atomic_all fixture; asserts TDelta AST shape, Apply function and method
//     wrapper presence, per-field Apply contributions (R-DG-012, R-DG-013), Diff function
//     and method wrapper presence, per-field Diff contributions (R-DG-012, R-DG-013).
//   - TestEmitTemplate_AtomicDiff_CrossPackage: asserts Diff in cross-package
//     mode: qualified signature, no method wrapper (R-DG-012, R-DG-013, R-DG-019, R-DG-012, R-DG-013).
//   - TestEmitTemplate_NoReflectImport_AllScalar: asserts that the "reflect"
//     import is absent when the Snapshot has only scalar fields.
//   - TestEmitTemplate_ReflectImport_WhenNeeded: asserts that the "reflect"
//     import is present when non-scalar fields exist.
//   - TestEmitTemplate_AtomicApply_CrossPackage: asserts Apply in cross-package
//     mode: qualified signature, no method wrapper (R-DG-012, R-DG-013, R-DG-019, R-DG-012, R-DG-013).
//   - TestEmitTemplate_NestedNotYet: asserts that delta.nested triggers the
//     Phase-5 sentinel error.
//   - TestEmitTemplate_CrossPackageQualifier: asserts type-string qualification
//     in cross-package mode.
//   - TestEmitTemplate_Nested_Map_SamePkg: asserts R-DG-016 map delta encoding —
//     UpdatedX/RemovedX fields in TDelta, no companion EntryDelta type, reflect
//     import absent (Entry is comparable, R-DG-016); backed by compileCheckEmitNestedMap.
//   - TestEmitTemplate_Nested_Map_CrossPkg: asserts R-DG-016 cross-package mode —
//     no method wrappers, map operation fragments present (R-DG-012, R-DG-013, R-DG-019).
//   - TestEmitTemplate_Nested_Slice_SamePkg: asserts R-DG-016, R-DG-028 slice delta encoding —
//     AddedX/RemovedX fields in TDelta, no reflect import (comparable elements),
//     method wrappers present; backed by compileCheckEmitNestedSlice.
//   - TestEmitTemplate_Nested_Slice_CrossPkg: asserts R-DG-016, R-DG-028 cross-package mode —
//     no method wrappers, AddedX/RemovedX fragments present (R-DG-012, R-DG-013, R-DG-019).
//   - TestEmitTemplate_Nested_Slice_Reflect_SamePkg: asserts R-DG-016, R-DG-028 non-comparable
//     element path — reflect import present, reflect.DeepEqual in generated code;
//     backed by compileCheckEmitNestedSliceReflect runtime tests (§5.2).
//   - compileCheckEmit: runs go test in an isolated temp module with a replace
//     directive; exercises Apply round-trip and HeaderAfterApply error
//     propagation (R-DG-012, R-DG-013); also exercises Diff round-trip, identity-diff
//     minimality, partial-diff minimality, and HeaderForDiff error propagation
//     (R-DG-012, R-DG-013).
//   - compileCheckEmitNestedMap: isolated-module compile-and-run for R-DG-016;
//     covers add/remove/update entries, round-trip on Tags and Scores, and
//     atomic-field coexistence (R-DG-006, R-DG-016 upsert semantics).
//   - compileCheckEmitNestedSlice: isolated-module compile-and-run for R-DG-016, R-DG-028;
//     covers add/remove elements, simultaneous add+remove, round-trip on Names
//     and Tags, and atomic-field coexistence (R-DG-006, R-DG-016 set-diff semantics).
//   - TestEmitTemplate_Clearable_Struct_SamePkg: asserts R-DG-016..07 struct shape —
//     FieldDelta[AddressDelta] field, AddressDelta companion emitted via R-DG-016
//     dedup, ApplyAddress/DiffAddress present, Op-switch in Apply, three-branch
//     in Diff, no reflect import (Address is comparable); backed by
//     compileCheckEmitClearableStruct tri-state truth table.
//   - TestEmitTemplate_Clearable_Map_SamePkg: asserts R-DG-016..07 map shape —
//     FieldDelta[TagsMapDelta] field, TagsMapDelta wrapper with UpdatedTags/
//     RemovedTags, IsEmpty/ApplyTagsMapDelta/DiffTagsMapDelta emitted, no reflect;
//     backed by compileCheckEmitClearableMap.
//   - TestEmitTemplate_Clearable_Slice_SamePkg: asserts R-DG-016..07 slice shape —
//     FieldDelta[GroupsSliceDelta] field, GroupsSliceDelta wrapper with
//     AddedGroups/RemovedGroups, IsEmpty/ApplyGroupsSliceDelta/DiffGroupsSliceDelta,
//     no reflect; backed by compileCheckEmitClearableSlice.
//   - TestEmitTemplate_Clearable_Map_Reflect_SamePkg: asserts reflect import
//     present when map value type is non-comparable (Bag contains a slice).
//   - TestEmitTemplate_Clearable_Slice_Reflect_SamePkg: asserts reflect import
//     present when slice element type is non-comparable ([]byte).
//   - TestEmitTemplate_NestedOnly_NoFieldDelta: regression guard — nested_map and
//     nested_slice output must not contain runtime.FieldDelta or IsEmpty tokens
//     (byte-identical guarantee for R-DG-016..07).
//   - compileCheckEmitClearableStruct/Map/Slice: isolated-module compile-and-run
//     covering the tri-state truth table (OpIgnore/OpRetract/OpAssert via
//     Diff+Apply) plus round-trip and atomic-field coexistence.

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// ── Group EM: view construction (TestBuildSnapshotView) ──────────────────────

// TestBuildSnapshotView exercises buildSnapshotView against the atomic_all
// fixture, verifying the §1.6.3 atomic-row emission matrix row by row.
// Covers: R-DG-015, R-DG-004, R-DG-005, R-DG-006, R-DG-006, R-DG-016, R-DG-006, R-DG-016
func TestBuildSnapshotView(t *testing.T) {
	// Load and parse the atomic_all fixture; use same-package qualifier.
	ps := loadEmitFixture(t, "atomic_all", "AtomicAllSnapshot")
	opts := emitOpts{crossPackage: false, aliases: nil}
	qualifier, _, _ := buildImports([]*ParsedSnapshot{ps}, opts)

	sv, err := buildSnapshotView(ps, qualifier, true)
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
		label               string
		fieldName           string
		deltaName           string
		deltaType           string
		suppressed          bool // true for delta.omit / delta.retired: in view, Suppressed: true (R-DG-012, R-DG-013)
		useReflectEq        bool // true when !types.Comparable(GoType): UseReflectEq: true (R-DG-012, R-DG-013, R-DG-016)
		isPointer           bool // true for ShapePointer: nil-equivalence + deref comparison (R-DG-026, R-DG-016)
		pointeeUseReflectEq bool // true when pointee is non-comparable: reflect.DeepEqual(*a,*b)
	}{
		// V01 — ShapeScalar int32: comparable → !=, no reflect.
		{label: "V01_Scalar", fieldName: "Scalar", deltaName: "SetScalar", deltaType: "*int32"},
		// V02 — ShapePointer *string: nil-equivalence + *a == *b comparison (R-DG-026, R-DG-016).
		// UseReflectEq stays false (pointer identity via != is not used); IsPointer drives its own branch.
		{label: "V02_Pointer", fieldName: "Pointer", deltaName: "SetPointer", deltaType: "**string", isPointer: true},
		// V03 — ShapeStructValue Inner{A,B int32}: all-scalar struct, comparable → !=, no reflect (R-DG-016).
		{label: "V03_Struct", fieldName: "Struct", deltaName: "SetStruct", deltaType: "*Inner"},
		// V05 — ShapeSlice []byte: slices are not comparable → reflect.DeepEqual.
		{label: "V05_Slice", fieldName: "Slice", deltaName: "SetSlice", deltaType: "*[]byte", useReflectEq: true},
		// V06 — ShapeMap map[string]int32: maps are not comparable → reflect.DeepEqual.
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
			// Non-suppressed: Suppressed must be false; check Set name, type, and flags.
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
			if (fv.Shape == fieldShapePointer) != tc.isPointer {
				t.Errorf("Shape==fieldShapePointer: got %v, want %v", fv.Shape == fieldShapePointer, tc.isPointer)
			}
			if fv.PointeeUseReflectEq != tc.pointeeUseReflectEq {
				t.Errorf("PointeeUseReflectEq: got %v, want %v", fv.PointeeUseReflectEq, tc.pointeeUseReflectEq)
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

// TestBuildSnapshotView_KeyName verifies that buildSnapshotView populates
// sv.KeyName from ps.KeyVar.Name() (R-DG-012, R-DG-013 contract).
// Covers: R-DG-012
func TestBuildSnapshotView_KeyName(t *testing.T) {
	ps := loadEmitFixture(t, "atomic_all", "AtomicAllSnapshot")
	opts := emitOpts{crossPackage: false}
	qualifier, _, _ := buildImports([]*ParsedSnapshot{ps}, opts)

	sv, err := buildSnapshotView(ps, qualifier, true)
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
// Covers: R-DG-012, R-DG-013, R-DG-012
func TestBuildSnapshotView_NeedsReflect(t *testing.T) {
	// atomic_all has slice ([]byte) and map (map[string]int32) fields which are
	// non-comparable and require reflect.DeepEqual → NeedsReflect.
	t.Run("HasNonScalar", func(t *testing.T) {
		ps := loadEmitFixture(t, "atomic_all", "AtomicAllSnapshot")
		opts := emitOpts{crossPackage: false}
		qualifier, _, _ := buildImports([]*ParsedSnapshot{ps}, opts)
		sv, err := buildSnapshotView(ps, qualifier, true)
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
		sv, err := buildSnapshotView(ps, qualifier, true)
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
//   - Apply function signature and body structure (R-DG-012, R-DG-013).
//   - Apply method wrapper present (same-package mode, R-DG-012, R-DG-013, R-DG-019, R-DG-012, R-DG-013).
//   - Generated file is gofmt-clean (R-DG-037).
//
// Covers: R-DG-015, R-DG-012, R-DG-022, R-DG-012, R-DG-013, R-DG-019, R-DG-004, R-DG-005, R-DG-006, R-DG-006, R-DG-016, R-DG-006, R-DG-016
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

	// R-DG-037: generated file must be gofmt-clean as written.
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

	fields := assertDeltaShape(t, f, "AtomicAllSnapshotDelta",
		[]string{"Header", "SetScalar", "SetPointer", "SetStruct", "SetSlice", "SetMap", "SetCommute"})

	// Header embed must be present (first field).
	if len(fields) == 0 || fields[0] != "Header" {
		t.Errorf("first field should be Header embed, got: %v", fields)
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

	// ── Apply function shape (R-DG-012, R-DG-013) ──────────────────────────────────────────

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

	// ── Diff function shape (R-DG-012, R-DG-013) ───────────────────────────────────────────

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

	// Comparable scalar/struct fields use !=; non-comparable fields use reflect.DeepEqual (R-DG-016).
	// Pointer fields use nil-equivalence + dereferenced comparison, NOT plain != (R-DG-026, R-DG-016).
	// Comparable (!=): Scalar (int32), Struct (Inner{int32,int32}), Commute (int32).
	// Pointer (*string): nil-equivalence guard + *a.Pointer == *b.Pointer.
	// Non-comparable (reflect): Slice ([]byte), Map (map[string]int32).
	for _, name := range []string{"Scalar", "Struct", "Commute"} {
		if !strings.Contains(srcStr, "a."+name+" != b."+name) {
			t.Errorf("Diff body missing != comparison for comparable field %s", name)
		}
		if strings.Contains(srcStr, "reflect.DeepEqual(a."+name+", b."+name+")") {
			t.Errorf("Diff body must not use reflect.DeepEqual for comparable field %s (R-DG-016)", name)
		}
	}
	// Pointer field: nil-equivalence guard (R-DG-026). Must NOT use plain identity comparison.
	if strings.Contains(srcStr, "a.Pointer != b.Pointer") {
		t.Errorf("Diff body must not compare Pointer by identity (pointer address); want nil-equivalence + deref (R-DG-026)")
	}
	if strings.Contains(srcStr, "reflect.DeepEqual(a.Pointer, b.Pointer)") {
		t.Errorf("Diff body must not use whole-pointer reflect.DeepEqual for Pointer; want deref comparison (R-DG-026)")
	}
	if !strings.Contains(srcStr, "*a.Pointer == *b.Pointer") {
		t.Errorf("Diff body missing dereferenced pointer comparison *a.Pointer == *b.Pointer (R-DG-026)")
	}
	if !strings.Contains(srcStr, "a.Pointer == nil") {
		t.Errorf("Diff body missing nil-equivalence guard for Pointer (R-DG-026)")
	}
	for _, name := range []string{"Slice", "Map"} {
		if !strings.Contains(srcStr, "reflect.DeepEqual(a."+name+", b."+name+")") {
			t.Errorf("Diff body missing reflect.DeepEqual for non-comparable field %s", name)
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

	// ── Coalesce function shape (R-DG-012, R-DG-013) ───────────────────────────────────────

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

	if !strings.Contains(srcStr, "return Coalesce(s, ds)") {
		t.Errorf("Coalesce method wrapper body missing 'return Coalesce(s, ds)'")
	}

	// ── Method wrappers present (same-package mode, R-DG-012, R-DG-013, R-DG-019) ────────────────────

	assertHasMethods(t, f, "AtomicAllSnapshot", []string{"Apply", "Diff", "Coalesce"})

	// ── EntityID function shape (R-DG-034) ───────────────────────────────────────
	// atomic_all has Key string (raw basic) — function emitted, no method.

	entityIDFn := findFuncDecl(f, "EntityID")
	if entityIDFn == nil {
		t.Fatalf("EntityID function not found in generated file")
	}
	// Signature: func EntityID(k string) runtime.EntityID
	if entityIDFn.Type.Params.NumFields() != 1 {
		t.Errorf("EntityID: want 1 param, got %d", entityIDFn.Type.Params.NumFields())
	}
	if entityIDFn.Type.Results.NumFields() != 1 {
		t.Errorf("EntityID: want 1 result, got %d", entityIDFn.Type.Results.NumFields())
	}

	// Body must contain the three expected lines.
	if !strings.Contains(srcStr, "h := runtime.NewHash()") {
		t.Errorf("EntityID body missing: h := runtime.NewHash()")
	}
	if !strings.Contains(srcStr, "runtime.WriteString(h, k)") {
		t.Errorf("EntityID body missing: runtime.WriteString(h, k)")
	}
	if !strings.Contains(srcStr, "return runtime.Finalise(h)") {
		t.Errorf("EntityID body missing: return runtime.Finalise(h)")
	}

	// crypto/blake2b must NOT be imported — the abstraction barrier is runtime.
	if strings.Contains(srcStr, "blake2b") {
		t.Errorf("generated file must not import crypto/blake2b directly")
	}

	// ── No EntityID method for raw-basic key (R-DG-034, R-DG-012, R-DG-014) ───────────────────

	if findMethodDecl(f, "string", "EntityID") != nil {
		t.Errorf("EntityID method must not be emitted for raw-basic key type string")
	}

	t.Run("CompileCheck", func(t *testing.T) {
		compileCheckEmit(t, src)
	})
}

// TestEmitTemplate_Nested_Map_SamePkg verifies end-to-end generation for
// delta.nested map fields (R-DG-016) in same-package mode:
//   - NestedMapSnapshotDelta carries UpdatedTags/RemovedTags and
//     UpdatedScores/RemovedScores (R-DG-006, R-DG-016 upsert encoding), plus SetCount *int32.
//   - No companion type is emitted for the map value types (V is atomic).
//   - Apply body references both the removed-keys slice and the updated-entries map.
//   - Generated file is gofmt-clean and the reflect import is ABSENT: Entry is a
//     comparable struct (all-scalar fields), so Diff uses != not reflect.DeepEqual.
//
// Covers: R-DG-016, R-DG-006, R-DG-016, R-DG-016
func TestEmitTemplate_Nested_Map_SamePkg(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "nested_map_delta.go")

	cfg := Config{
		InputPkgs:     []string{"./testdata/emit/nested_map"},
		TargetStructs: []string{"NestedMapSnapshot"},
		OutPath:       outPath,
	}
	if err := New(cfg).Run(); err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	assertGofmtClean(t, outPath)

	src, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("reading output: %v", err)
	}
	srcStr := string(src)

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, outPath, src, 0)
	if err != nil {
		t.Fatalf("generated file is not valid Go: %v\n--- source ---\n%s", err, src)
	}

	// NestedMapSnapshotDelta must contain the R-DG-016 map encoding fields.
	deltaFields := assertDeltaShape(t, f, "NestedMapSnapshotDelta",
		[]string{"UpdatedTags", "RemovedTags", "UpdatedScores", "RemovedScores", "SetCount"})
	// Raw source field names must not appear in the Delta struct.
	for _, absent := range []string{"Tags", "Scores", "Count"} {
		if contains(deltaFields, absent) {
			t.Errorf("NestedMapSnapshotDelta must not have raw field %q; fields: %v", absent, deltaFields)
		}
	}

	// No companion type for the map value type Entry (V is treated atomically).
	if findStructDecl(f, "EntryDelta") != nil {
		t.Errorf("EntryDelta must not be emitted: R-DG-016 treats map value type atomically")
	}

	// Apply body must include the three map-apply steps for each map field.
	for _, fragment := range []string{"RemovedTags", "UpdatedTags", "RemovedScores", "UpdatedScores"} {
		if !strings.Contains(srcStr, fragment) {
			t.Errorf("Apply body missing %q reference", fragment)
		}
	}

	// reflect import must be ABSENT: Entry is a comparable struct (all-scalar fields),
	// so the generated Diff uses != for Scores value comparison (R-DG-016).
	if strings.Contains(srcStr, `"reflect"`) {
		t.Errorf(`unexpected "reflect" import: Entry is comparable, Diff must use != not reflect.DeepEqual`)
	}
	if strings.Contains(srcStr, "reflect.DeepEqual") {
		t.Errorf("unexpected reflect.DeepEqual in generated code: Entry is comparable (R-DG-016)")
	}

	t.Run("CompileCheck", func(t *testing.T) {
		compileCheckEmitNestedMap(t, src)
	})
}

// TestEmitTemplate_Nested_Map_CrossPkg verifies R-DG-016 generation in cross-package
// mode: no method wrappers are emitted (R-DG-012, R-DG-013, R-DG-019), and the Apply/Diff function bodies
// still contain the map-copy/delete/upsert logic.
// Covers: R-DG-016, R-DG-012, R-DG-013, R-DG-019
func TestEmitTemplate_Nested_Map_CrossPkg(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "nested_map_cross_delta.go")

	cfg := Config{
		InputPkgs:          []string{"./testdata/emit/nested_map"},
		TargetStructs:      []string{"NestedMapSnapshot"},
		OutPath:            outPath,
		OutPkgNameOverride: "deltas",
	}
	if err := New(cfg).Run(); err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	assertGofmtClean(t, outPath)

	src, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("reading output: %v", err)
	}
	srcStr := string(src)

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, outPath, src, 0)
	if err != nil {
		t.Fatalf("generated file is not valid Go: %v\n--- source ---\n%s", err, src)
	}

	// Package-level Apply and Diff must exist.
	if findFuncDecl(f, "Apply") == nil {
		t.Errorf("Apply function not found in cross-pkg output")
	}
	if findFuncDecl(f, "Diff") == nil {
		t.Errorf("Diff function not found in cross-pkg output")
	}

	// Method wrappers must NOT be emitted (R-DG-012, R-DG-013, R-DG-019).
	if findMethodDecl(f, "NestedMapSnapshot", "Apply") != nil {
		t.Errorf("Apply method wrapper must not be emitted in cross-pkg mode")
	}
	if findMethodDecl(f, "NestedMapSnapshot", "Diff") != nil {
		t.Errorf("Diff method wrapper must not be emitted in cross-pkg mode")
	}

	// Apply and Diff bodies must still contain the map operation fragments.
	for _, fragment := range []string{"RemovedTags", "UpdatedTags", "RemovedScores", "UpdatedScores"} {
		if !strings.Contains(srcStr, fragment) {
			t.Errorf("cross-pkg output missing %q reference in Apply/Diff body", fragment)
		}
	}
}

// TestEmitTemplate_Nested_Slice_SamePkg verifies end-to-end generation for
// delta.nested slice fields (R-DG-016, R-DG-028) in same-package mode:
//   - NestedSliceSnapshotDelta carries AddedNames/RemovedNames and
//     AddedTags/RemovedTags (R-DG-006, R-DG-016 set-diff encoding), plus SetCount *int32.
//   - No companion type is emitted for the slice element types (V is atomic).
//   - Apply body references both the removed-elements path and the added-elements append.
//   - Generated file is gofmt-clean and the reflect import is ABSENT: Names (string)
//     and Tags (comparable struct) both use the O(n) map[T]struct{} path (R-DG-016).
//
// Covers: R-DG-016, R-DG-028, R-DG-006, R-DG-016, R-DG-016
func TestEmitTemplate_Nested_Slice_SamePkg(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "nested_slice_delta.go")

	cfg := Config{
		InputPkgs:     []string{"./testdata/emit/nested_slice"},
		TargetStructs: []string{"NestedSliceSnapshot"},
		OutPath:       outPath,
	}
	if err := New(cfg).Run(); err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	assertGofmtClean(t, outPath)

	src, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("reading output: %v", err)
	}
	srcStr := string(src)

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, outPath, src, 0)
	if err != nil {
		t.Fatalf("generated file is not valid Go: %v\n--- source ---\n%s", err, src)
	}

	// NestedSliceSnapshotDelta must contain the R-DG-016, R-DG-028 set-diff encoding fields.
	deltaFields := assertDeltaShape(t, f, "NestedSliceSnapshotDelta",
		[]string{"AddedNames", "RemovedNames", "AddedTags", "RemovedTags", "SetCount"})
	// Raw source field names must not appear in the Delta struct.
	for _, absent := range []string{"Names", "Tags", "Count"} {
		if contains(deltaFields, absent) {
			t.Errorf("NestedSliceSnapshotDelta must not have raw field %q; fields: %v", absent, deltaFields)
		}
	}

	// No companion type for the slice element types.
	if findStructDecl(f, "TagDelta") != nil {
		t.Errorf("TagDelta must not be emitted: R-DG-016, R-DG-028 treats slice element type atomically")
	}

	// Apply body must reference the removed and added slice fields.
	for _, fragment := range []string{"RemovedNames", "AddedNames", "RemovedTags", "AddedTags"} {
		if !strings.Contains(srcStr, fragment) {
			t.Errorf("Apply body missing %q reference", fragment)
		}
	}

	// reflect import must be ABSENT: Names (string) and Tags (comparable struct)
	// both use the O(n) map[T]struct{} path (R-DG-016, §5.2).
	if strings.Contains(srcStr, `"reflect"`) {
		t.Errorf(`unexpected "reflect" import: element types are comparable, must use map-set path`)
	}
	if strings.Contains(srcStr, "reflect.DeepEqual") {
		t.Errorf("unexpected reflect.DeepEqual in generated code: element types are comparable (R-DG-016)")
	}

	// Method wrappers must be present in same-package mode (R-DG-012, R-DG-013, R-DG-019).
	assertHasMethods(t, f, "NestedSliceSnapshot", []string{"Apply", "Diff"})

	t.Run("CompileCheck", func(t *testing.T) {
		compileCheckEmitNestedSlice(t, src)
	})
}

// TestEmitTemplate_Nested_Slice_CrossPkg verifies R-DG-016, R-DG-028 generation in cross-package
// mode: no method wrappers are emitted (R-DG-012, R-DG-013, R-DG-019), and the Apply/Diff function bodies
// still contain the slice set-diff logic.
// Covers: R-DG-016, R-DG-028, R-DG-012, R-DG-013, R-DG-019
func TestEmitTemplate_Nested_Slice_CrossPkg(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "nested_slice_cross_delta.go")

	cfg := Config{
		InputPkgs:          []string{"./testdata/emit/nested_slice"},
		TargetStructs:      []string{"NestedSliceSnapshot"},
		OutPath:            outPath,
		OutPkgNameOverride: "deltas",
	}
	if err := New(cfg).Run(); err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	assertGofmtClean(t, outPath)

	src, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("reading output: %v", err)
	}
	srcStr := string(src)

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, outPath, src, 0)
	if err != nil {
		t.Fatalf("generated file is not valid Go: %v\n--- source ---\n%s", err, src)
	}

	// Package-level Apply and Diff must exist.
	if findFuncDecl(f, "Apply") == nil {
		t.Errorf("Apply function not found in cross-pkg output")
	}
	if findFuncDecl(f, "Diff") == nil {
		t.Errorf("Diff function not found in cross-pkg output")
	}

	// Method wrappers must NOT be emitted (R-DG-012, R-DG-013, R-DG-019).
	if findMethodDecl(f, "NestedSliceSnapshot", "Apply") != nil {
		t.Errorf("Apply method wrapper must not be emitted in cross-pkg mode")
	}
	if findMethodDecl(f, "NestedSliceSnapshot", "Diff") != nil {
		t.Errorf("Diff method wrapper must not be emitted in cross-pkg mode")
	}

	// Apply and Diff bodies must still contain the slice operation fragments.
	for _, fragment := range []string{"RemovedNames", "AddedNames", "RemovedTags", "AddedTags"} {
		if !strings.Contains(srcStr, fragment) {
			t.Errorf("cross-pkg output missing %q reference in Apply/Diff body", fragment)
		}
	}
}

// TestEmitTemplate_Nested_Slice_Reflect_SamePkg verifies R-DG-016, R-DG-028 generation for a
// slice field whose element type is not comparable ([][]byte, element type []byte).
// The generator must set SliceElemUseReflectEq=true, inject the reflect import,
// and emit reflect.DeepEqual calls in both Apply and Diff bodies (§5.2).
//
// Covers: R-DG-016, R-DG-028, §5.2 (non-comparable element fallback)
func TestEmitTemplate_Nested_Slice_Reflect_SamePkg(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "nested_slice_reflect_delta.go")

	cfg := Config{
		InputPkgs:     []string{"./testdata/emit/nested_slice_reflect"},
		TargetStructs: []string{"NestedSliceReflectSnapshot"},
		OutPath:       outPath,
	}
	if err := New(cfg).Run(); err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	assertGofmtClean(t, outPath)

	src, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("reading output: %v", err)
	}
	srcStr := string(src)

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, outPath, src, 0)
	if err != nil {
		t.Fatalf("generated file is not valid Go: %v\n--- source ---\n%s", err, src)
	}

	// NestedSliceReflectSnapshotDelta must contain AddedBlobs and RemovedBlobs.
	assertDeltaShape(t, f, "NestedSliceReflectSnapshotDelta", []string{"AddedBlobs", "RemovedBlobs"})

	// reflect import must be PRESENT: []byte is not comparable (§5.2).
	if !strings.Contains(srcStr, `"reflect"`) {
		t.Errorf(`expected "reflect" import: []byte element type is not comparable`)
	}
	if !strings.Contains(srcStr, "reflect.DeepEqual") {
		t.Errorf("expected reflect.DeepEqual in generated code: []byte element type is not comparable (§5.2)")
	}

	// Apply and Diff bodies must reference the slice delta fields.
	for _, fragment := range []string{"RemovedBlobs", "AddedBlobs"} {
		if !strings.Contains(srcStr, fragment) {
			t.Errorf("generated code missing %q reference", fragment)
		}
	}

	t.Run("CompileCheck", func(t *testing.T) {
		compileCheckEmitNestedSliceReflect(t, src)
	})
}

// TestEmitTemplate_CrossPackageQualifier verifies that in cross-package mode
// the generated file qualifies source-package types (e.g. *model.Address) and
// imports the source package.
// Covers: R-DG-015, R-DG-012, R-DG-013, R-DG-019
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

	// R-DG-037: generated file must be gofmt-clean as written.
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
// signature, and no method wrapper is emitted (R-DG-012, R-DG-013, R-DG-019).
// Covers: R-DG-012, R-DG-012, R-DG-013, R-DG-019
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

	// R-DG-037: generated file must be gofmt-clean as written.
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

	// No method wrapper in cross-package mode (R-DG-012, R-DG-013, R-DG-019).
	if findMethodDecl(f, "CrossPkgSnapshot", "Apply") != nil {
		t.Errorf("Apply method wrapper must not be emitted in cross-package mode")
	}
}

// TestEmitTemplate_AtomicDiff_CrossPackage verifies Diff emission in
// cross-package mode: source-package types are qualified in the function
// signature, and no Diff method wrapper is emitted (R-DG-012, R-DG-013, R-DG-019).
// Covers: R-DG-012, R-DG-013, R-DG-012, R-DG-013, R-DG-019, R-DG-012
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

	// R-DG-037: generated file must be gofmt-clean as written.
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

	// Signature must qualify source-package types (R-DG-012, R-DG-013, R-DG-019).
	if !strings.Contains(srcStr, "model.CrossPkgSnapshot") {
		t.Errorf("expected 'model.CrossPkgSnapshot' in Diff signature, got:\n%s", srcStr)
	}
	// Diff(a, b T) (TDelta, error) — both params use the qualified type.
	if !strings.Contains(srcStr, "func Diff(a, b model.CrossPkgSnapshot)") {
		t.Errorf("expected 'func Diff(a, b model.CrossPkgSnapshot)' in generated file, got:\n%s", srcStr)
	}

	// No Diff method wrapper in cross-package mode (R-DG-012, R-DG-013, R-DG-019).
	if findMethodDecl(f, "CrossPkgSnapshot", "Diff") != nil {
		t.Errorf("Diff method wrapper must not be emitted in cross-package mode")
	}

	// CrossPkgSnapshot.Location Address is a comparable struct (Street, City string),
	// so Diff uses != — no reflect import (R-DG-016).
	if strings.Contains(srcStr, `"reflect"`) {
		t.Errorf("unexpected \"reflect\" import: Address is comparable, Diff must use != (R-DG-016):\n%s", srcStr)
	}
}

// TestEmitTemplate_AtomicCoalesce_CrossPackage verifies Coalesce emission in
// cross-package mode: source-package types are qualified in the function
// signature, and no Coalesce method wrapper is emitted (R-DG-012, R-DG-013, R-DG-019).
// Covers: R-DG-012, R-DG-013, R-DG-012, R-DG-013, R-DG-019, R-DG-012
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

	// R-DG-037: generated file must be gofmt-clean as written.
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

	// Signature must qualify source-package types for both parameter and return (R-DG-012, R-DG-013, R-DG-019).
	if !strings.Contains(srcStr, "func Coalesce(s model.CrossPkgSnapshot, ds []CrossPkgSnapshotDelta) (model.CrossPkgSnapshot, error)") {
		t.Errorf("expected qualified Coalesce signature, got:\n%s", srcStr)
	}

	// No Coalesce method wrapper in cross-package mode (R-DG-012, R-DG-013, R-DG-019).
	if findMethodDecl(f, "CrossPkgSnapshot", "Coalesce") != nil {
		t.Errorf("Coalesce method wrapper must not be emitted in cross-package mode")
	}
}

// TestEmitTemplate_NamedPrimitive_KeyMethodEmitted verifies that a named-primitive
// entity key (Key IMSI, type IMSI string) causes the EntityID function to emit a
// string(k) conversion and the same-package method wrapper to be generated.
// Covers: R-DG-012, R-DG-014, R-DG-012, R-DG-013, R-DG-019
func TestEmitTemplate_NamedPrimitive_KeyMethodEmitted(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "entityid_named_prim_delta.go")
	cfg := Config{
		InputPkgs:     []string{"./testdata/emit/entityid_named_primitive"},
		TargetStructs: []string{"EntityIDNamedPrimSnapshot"},
		OutPath:       outPath,
	}
	if err := New(cfg).Run(); err != nil {
		t.Fatalf("Run() failed: %v", err)
	}
	assertGofmtClean(t, outPath)

	src, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("reading output: %v", err)
	}
	srcStr := string(src)

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, outPath, src, 0)
	if err != nil {
		t.Fatalf("generated file is not valid Go: %v\n--- source ---\n%s", err, src)
	}

	// EntityID function must exist with one param and one result.
	entityIDFn := findFuncDecl(f, "EntityID")
	if entityIDFn == nil {
		t.Fatalf("EntityID function not found")
	}
	if entityIDFn.Type.Params.NumFields() != 1 {
		t.Errorf("EntityID: want 1 param, got %d", entityIDFn.Type.Params.NumFields())
	}

	// Function body must emit the string(k) conversion for the named-string key.
	if !strings.Contains(srcStr, "runtime.WriteString(h, string(k))") {
		t.Errorf("EntityID body missing named-to-basic conversion: runtime.WriteString(h, string(k))")
	}

	// Same-package method wrapper must be emitted for a named key type.
	if findMethodDecl(f, "IMSI", "EntityID") == nil {
		t.Errorf("EntityID method wrapper not found (expected for named-primitive key IMSI)")
	}
	if !strings.Contains(srcStr, "return EntityID(k)") {
		t.Errorf("EntityID method wrapper body missing 'return EntityID(k)'")
	}
}

// TestEmitTemplate_StructKey_SamePkg verifies that a struct entity key emits an
// EntityID function walking sub-fields in lexicographic field-name order with
// appropriate Write* calls, plus a same-package method wrapper.
// Covers: R-DG-012, R-DG-014, R-DG-012, R-DG-013, R-DG-019
func TestEmitTemplate_StructKey_SamePkg(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "entityid_struct_key_delta.go")
	cfg := Config{
		InputPkgs:     []string{"./testdata/emit/entityid_struct_key"},
		TargetStructs: []string{"EntityIDStructKeySnapshot"},
		OutPath:       outPath,
	}
	if err := New(cfg).Run(); err != nil {
		t.Fatalf("Run() failed: %v", err)
	}
	assertGofmtClean(t, outPath)

	src, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("reading output: %v", err)
	}
	srcStr := string(src)

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, outPath, src, 0)
	if err != nil {
		t.Fatalf("generated file is not valid Go: %v\n--- source ---\n%s", err, src)
	}

	// EntityID function: func EntityID(k SomeKey) runtime.EntityID.
	entityIDFn := findFuncDecl(f, "EntityID")
	if entityIDFn == nil {
		t.Fatalf("EntityID function not found")
	}
	if entityIDFn.Type.Params.NumFields() != 1 {
		t.Errorf("EntityID: want 1 param, got %d", entityIDFn.Type.Params.NumFields())
	}

	// Body must contain both sub-field hash writes in lexicographic field-name order.
	if !strings.Contains(srcStr, "runtime.WriteString(h, k.IMSI)") {
		t.Errorf("EntityID body missing: runtime.WriteString(h, k.IMSI)")
	}
	if !strings.Contains(srcStr, "runtime.WriteUint64(h, k.SubID)") {
		t.Errorf("EntityID body missing: runtime.WriteUint64(h, k.SubID)")
	}
	// IMSI < SubID lexicographically, so IMSI must be hashed first.
	imsiPos := strings.Index(srcStr, "k.IMSI")
	subIDPos := strings.Index(srcStr, "k.SubID")
	if imsiPos < 0 || subIDPos < 0 || imsiPos > subIDPos {
		t.Errorf("EntityID body: IMSI write must precede SubID write (lexicographic field-name order)")
	}

	// Same-package method wrapper on SomeKey.
	if findMethodDecl(f, "SomeKey", "EntityID") == nil {
		t.Errorf("EntityID method wrapper not found (expected for named-struct key SomeKey)")
	}
	if !strings.Contains(srcStr, "return EntityID(k)") {
		t.Errorf("EntityID method body missing 'return EntityID(k)'")
	}

	t.Run("CompileCheck", func(t *testing.T) {
		compileCheckEmitStructKey(t, src)
	})
}

// TestEmitTemplate_StructKey_FieldOrderStability verifies that the emitter
// produces identical KeyHashLines for a struct key regardless of the sub-field
// source declaration order. entityid_struct_key declares IMSI before SubID;
// entityid_struct_key_reversed declares SubID before IMSI. Both must emit
// IMSI first (lexicographic order), so the hash lines must be byte-equal.
// Covers: R-DG-012, R-DG-014
func TestEmitTemplate_StructKey_FieldOrderStability(t *testing.T) {
	emitAndGetEntityIDBody := func(t *testing.T, inputPkg, structName string) string {
		t.Helper()
		outPath := filepath.Join(t.TempDir(), "delta.go")
		cfg := Config{
			InputPkgs:     []string{inputPkg},
			TargetStructs: []string{structName},
			OutPath:       outPath,
		}
		if err := New(cfg).Run(); err != nil {
			t.Fatalf("Run() failed for %s: %v", structName, err)
		}
		src, err := os.ReadFile(outPath)
		if err != nil {
			t.Fatalf("reading output: %v", err)
		}
		// Extract the EntityID function body (between the opening and closing braces).
		srcStr := string(src)
		start := strings.Index(srcStr, "func EntityID(")
		if start < 0 {
			t.Fatalf("EntityID function not found in output for %s", structName)
		}
		// Advance to opening brace.
		braceOpen := strings.Index(srcStr[start:], "{")
		if braceOpen < 0 {
			t.Fatalf("no opening brace found after EntityID for %s", structName)
		}
		body := srcStr[start+braceOpen:]
		// Find matching closing brace.
		depth := 0
		for i, ch := range body {
			switch ch {
			case '{':
				depth++
			case '}':
				depth--
				if depth == 0 {
					return body[:i+1]
				}
			}
		}
		t.Fatalf("no matching closing brace for EntityID in %s", structName)
		return ""
	}

	normalBody := emitAndGetEntityIDBody(t,
		"./testdata/emit/entityid_struct_key",
		"EntityIDStructKeySnapshot",
	)
	reversedBody := emitAndGetEntityIDBody(t,
		"./testdata/emit/entityid_struct_key_reversed",
		"EntityIDReversedKeySnapshot",
	)

	// Strip the parameter type name (SomeKey vs ReversedKey) before comparing,
	// since the key types have different names even though the hash logic is identical.
	normalNorm := strings.ReplaceAll(normalBody, "SomeKey", "KEY")
	reversedNorm := strings.ReplaceAll(reversedBody, "ReversedKey", "KEY")

	if normalNorm != reversedNorm {
		t.Errorf("EntityID body differs between normal and reversed field order:\n--- normal ---\n%s\n--- reversed ---\n%s",
			normalBody, reversedBody)
	}

	// Additionally confirm reversed fixture emits IMSI before SubID (lexicographic).
	imsiPos := strings.Index(reversedBody, "k.IMSI")
	subIDPos := strings.Index(reversedBody, "k.SubID")
	if imsiPos < 0 || subIDPos < 0 || imsiPos > subIDPos {
		t.Errorf("reversed fixture: IMSI write must still precede SubID write (lexicographic order); body:\n%s", reversedBody)
	}

	t.Run("CompileCheck", func(t *testing.T) {
		outPath := filepath.Join(t.TempDir(), "delta.go")
		cfg := Config{
			InputPkgs:     []string{"./testdata/emit/entityid_struct_key_reversed"},
			TargetStructs: []string{"EntityIDReversedKeySnapshot"},
			OutPath:       outPath,
		}
		if err := New(cfg).Run(); err != nil {
			t.Fatalf("Run() for reversed fixture failed: %v", err)
		}
		assertGofmtClean(t, outPath)
		src, err := os.ReadFile(outPath)
		if err != nil {
			t.Fatalf("reading output: %v", err)
		}
		compileCheckEmitStructKeyReversed(t, src)
	})
}

// TestEmitTemplate_EntityID_CrossPackage verifies EntityID emission in cross-
// package mode: the key type is qualified and no method wrapper is emitted (R-DG-012, R-DG-013, R-DG-019).
// Covers: R-DG-012, R-DG-014, R-DG-012, R-DG-013, R-DG-019
func TestEmitTemplate_EntityID_CrossPackage(t *testing.T) {
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
	assertGofmtClean(t, outPath)

	src, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("reading output: %v", err)
	}
	srcStr := string(src)

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, outPath, src, 0)
	if err != nil {
		t.Fatalf("generated file is not valid Go: %v\n--- source ---\n%s", err, src)
	}

	// EntityID function must be present with qualified parameter type.
	if findFuncDecl(f, "EntityID") == nil {
		t.Fatalf("EntityID function not found in cross-package generated file")
	}
	if !strings.Contains(srcStr, "func EntityID(k model.ModelKey) runtime.EntityID") {
		t.Errorf("expected qualified 'func EntityID(k model.ModelKey) runtime.EntityID', got:\n%s", srcStr)
	}

	// Hash body: ModelKey has one sub-field (ID string).
	if !strings.Contains(srcStr, "runtime.WriteString(h, k.ID)") {
		t.Errorf("EntityID body missing: runtime.WriteString(h, k.ID)")
	}

	// No EntityID method wrapper in cross-package mode (R-DG-012, R-DG-013, R-DG-019).
	if findMethodDecl(f, "ModelKey", "EntityID") != nil {
		t.Errorf("EntityID method wrapper must not be emitted in cross-package mode")
	}
}

// TestEmitTemplate_EntityID_TagVsOverridePathEquivalence verifies that
// identifying the entity-key field via the eddt:"entity.key" tag versus via
// ParseOpts.KeyFieldOverride produces byte-equal EntityID hash lines. Both
// parse paths converge on the same KeyVar (parse_key.go:107), so emission
// must be identical regardless of how the field was identified. This covers
// the "untagged key via --key-field" case from the R-DG-034 plan (R-DG-040).
// Covers: R-DG-012, R-DG-014, R-DG-040
func TestEmitTemplate_EntityID_TagVsOverridePathEquivalence(t *testing.T) {
	pkgs, err := loadPackages([]string{"./testdata/parse/valid"}, slog.Default())
	if err != nil {
		t.Fatalf("loadPackages: %v", err)
	}

	// Tag path: key identified by eddt:"entity.key" tag.
	psTag, err := parseSnapshot(pkgs, "ValidSnapshot", ParseOpts{})
	if err != nil {
		t.Fatalf("parseSnapshot (tag path): %v", err)
	}

	// Override path: key identified by --key-field "Key" (same field, no tag scan).
	psOverride, err := parseSnapshot(pkgs, "ValidSnapshot", ParseOpts{KeyFieldOverride: "Key"})
	if err != nil {
		t.Fatalf("parseSnapshot (override path): %v", err)
	}

	// Build views for both parse results.
	opts := emitOpts{crossPackage: false}
	qualTag, _, _ := buildImports([]*ParsedSnapshot{psTag}, opts)
	qualOverride, _, _ := buildImports([]*ParsedSnapshot{psOverride}, opts)

	svTag, err := buildSnapshotView(psTag, qualTag, true)
	if err != nil {
		t.Fatalf("buildSnapshotView (tag): %v", err)
	}
	svOverride, err := buildSnapshotView(psOverride, qualOverride, true)
	if err != nil {
		t.Fatalf("buildSnapshotView (override): %v", err)
	}

	// KeyHashLines must be byte-equal regardless of identification path.
	if len(svTag.KeyHashLines) != len(svOverride.KeyHashLines) {
		t.Fatalf("KeyHashLines length: tag=%d override=%d", len(svTag.KeyHashLines), len(svOverride.KeyHashLines))
	}
	for i, line := range svTag.KeyHashLines {
		if line != svOverride.KeyHashLines[i] {
			t.Errorf("KeyHashLines[%d]: tag=%q override=%q", i, line, svOverride.KeyHashLines[i])
		}
	}

	// KeyTypeName must also be identical.
	if svTag.KeyTypeName != svOverride.KeyTypeName {
		t.Errorf("KeyTypeName: tag=%q override=%q", svTag.KeyTypeName, svOverride.KeyTypeName)
	}
}

// TestBuildSnapshotView_UnsupportedKeyUnderlying verifies that a key whose
// underlying type is outside the R-DG-034 support matrix (e.g. float64, which is
// comparable so the parser accepts it but the hash renderer cannot map it)
// causes buildSnapshotView to return a descriptive error.
// Covers: R-DG-012, R-DG-014
func TestBuildSnapshotView_UnsupportedKeyUnderlying(t *testing.T) {
	// Construct a ParsedSnapshot whose entity-key field has underlying float64.
	// float64 is a basic comparable type so the parser would accept it, but
	// buildKeyHashLines returns an error for it (R-DG-034 support matrix).
	flt := types.Typ[types.Float64]
	keyVar := types.NewVar(token.NoPos, nil, "Key", flt)
	headerVar := types.NewVar(token.NoPos, nil, "Header", flt) // dummy; not used

	ps := &ParsedSnapshot{
		Name:      "TestSnapshot",
		PkgPath:   "test",
		PkgName:   "test",
		HeaderVar: headerVar,
		KeyVar:    keyVar,
		KeyShape:  ShapeScalar,
		Fields:    nil,
	}

	opts := emitOpts{crossPackage: false}
	qualifier, _, _ := buildImports([]*ParsedSnapshot{ps}, opts)

	_, err := buildSnapshotView(ps, qualifier, true)
	if err == nil {
		t.Fatal("expected error for unsupported float64 key underlying type, got nil")
	}
	if !strings.Contains(err.Error(), "unsupported underlying type") {
		t.Errorf("error should mention 'unsupported underlying type', got: %v", err)
	}
}

// TestEmitTemplate_NoReflectImport_AllScalar verifies that the "reflect" import
// is absent when the Snapshot contains only scalar and suppressed fields.
// Covers: R-DG-012, R-DG-013, R-DG-012
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

	// R-DG-037: generated file must be gofmt-clean as written.
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
// Covers: R-DG-012, R-DG-013, R-DG-012
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

	// R-DG-037: generated file must be gofmt-clean as written.
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

// TestEmitTemplate_PtrNonComparable exercises the PointeeUseReflectEq path
// (R-DG-026): a *SliceBag field whose pointee is non-comparable (contains a
// slice). The generated Diff must emit reflect.DeepEqual(*a.Bag, *b.Bag)
// inside the nil-equivalence guard, and the "reflect" import must be present.
func TestEmitTemplate_PtrNonComparable(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "delta.go")
	cfg := Config{
		InputPkgs:     []string{"./testdata/emit/ptr_noncomparable"},
		TargetStructs: []string{"PtrNonComparableSnapshot"},
		OutPath:       outPath,
	}
	if err := New(cfg).Run(); err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	assertGofmtClean(t, outPath)

	src, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("reading output: %v", err)
	}
	srcStr := string(src)

	// "reflect" import must be injected (PointeeUseReflectEq → NeedsReflect).
	if !strings.Contains(srcStr, `"reflect"`) {
		t.Errorf("expected \"reflect\" import for *SliceBag (non-comparable pointee):\n%s", srcStr)
	}
	// Diff must use dereferenced reflect.DeepEqual, not whole-pointer comparison.
	if !strings.Contains(srcStr, "reflect.DeepEqual(*a.Bag, *b.Bag)") {
		t.Errorf("Diff body missing reflect.DeepEqual(*a.Bag, *b.Bag) for non-comparable pointee:\n%s", srcStr)
	}
	// Nil-equivalence guard must be present.
	if !strings.Contains(srcStr, "a.Bag == nil") {
		t.Errorf("Diff body missing nil-equivalence guard for Bag:\n%s", srcStr)
	}
	// Must NOT use plain pointer identity comparison.
	if strings.Contains(srcStr, "a.Bag != b.Bag") {
		t.Errorf("Diff body must not compare Bag by pointer identity:\n%s", srcStr)
	}

	// Compile-check: write an isolated module and verify go build succeeds.
	t.Run("CompileCheck", func(t *testing.T) {
		tmpDir := t.TempDir()
		wd, err := os.Getwd()
		if err != nil {
			t.Fatalf("getwd: %v", err)
		}
		moduleRoot := filepath.Clean(filepath.Join(wd, "..", ".."))

		snapshotSrc := `package ptrnoncomparable

import eddt "go.resystems.io/eddt/runtime"

type SliceBag struct{ Tags []string }

var _ eddt.Header

type PtrNonComparableSnapshot struct {
	eddt.Header
	Key string ` + "`eddt:\"entity.key\"`" + `
	Bag *SliceBag
}
`
		if err := os.WriteFile(filepath.Join(tmpDir, "snapshot.go"), []byte(snapshotSrc), 0644); err != nil {
			t.Fatalf("write snapshot.go: %v", err)
		}
		if err := os.WriteFile(filepath.Join(tmpDir, "delta.go"), src, 0644); err != nil {
			t.Fatalf("write delta.go: %v", err)
		}
		modContent := "module ptrnoncomparable\n\ngo 1.25.0\n\nrequire go.resystems.io/eddt v0.0.0\n\nreplace go.resystems.io/eddt => " + moduleRoot + "\n"
		if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte(modContent), 0644); err != nil {
			t.Fatalf("write go.mod: %v", err)
		}
		goSum, err := os.ReadFile(filepath.Join(moduleRoot, "go.sum"))
		if err != nil {
			t.Fatalf("read go.sum: %v", err)
		}
		if err := os.WriteFile(filepath.Join(tmpDir, "go.sum"), goSum, 0644); err != nil {
			t.Fatalf("write go.sum: %v", err)
		}
		runBuildCmd(t, tmpDir, "go", "build", "-mod=mod", "./...")
	})
}

// ── Compile-check helper ──────────────────────────────────────────────────────

// compileCheckEmit writes the generated source (plus a matching source
// Snapshot package) into an isolated temp module with a replace directive
// for go.resystems.io/eddt, then:
//   - asserts the generated delta.go is gofmt-clean (R-DG-037),
//   - runs go test ./... to type-check and exercise Apply round-trip behaviour
//     (R-DG-012) and HeaderAfterApply error propagation (R-DG-012),
//   - exercises Diff round-trip Apply(a, Diff(a, b)) == b across all five
//     atomic shapes (R-DG-023), identity-diff Set* nilness (R-DG-024 / R-DG-024),
//     partial-diff minimality, and HeaderForDiff error propagation (R-DG-012).
//
// The temp module reuses the eddt module's go.sum so that transitive
// dependencies (e.g. golang.org/x/crypto) resolve without network access.
func compileCheckEmit(t *testing.T, generatedSrc []byte) {
	t.Helper()

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

	// Write a behaviour test exercising Apply round-trip (R-DG-012) and
	// HeaderAfterApply error propagation (R-DG-012). The test is placed in the
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
// Apply to return a non-nil error (R-DG-012: Apply returns (T, error)).
// Covers: R-DG-012
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
	applyTestCode := testCode

	// Write a behaviour test exercising Diff round-trip (R-DG-023), identity-diff
	// minimality (R-DG-024 / R-DG-024), partial-diff minimality, and HeaderForDiff
	// error propagation (R-DG-012).
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
// all five atomic shapes plus delta.commutative (R-DG-023).
// Suppressed fields must equal a (propagated by Apply; Diff emits nothing for them).
// Header equality is not asserted — it advances by construction (R-DG-024).
// Covers: R-DG-023, R-DG-012, R-DG-013, R-DG-012
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
// nil (minimality of the identity diff, R-DG-024). Apply(a, Diff(a, a)) is NOT
// called — that would violate HeaderAfterApply's strict Sequence monotonicity
// precondition (R-DG-024: identity diff Sequence == a.Sequence).
// Covers: R-DG-024, R-DG-024
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
	// strict monotonicity precondition (R-DG-024).
}

// TestDiffPartial verifies that Diff produces a minimal delta: only the one
// field that differs between a and c has a non-nil Set* value.
// Covers: R-DG-023, R-DG-012, R-DG-013
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
// This pins the (TDelta, error) signature behaviour under R-DG-012.
// Covers: R-DG-012, R-DG-013, R-DG-012
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
// Covers: R-DG-023
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

// TestDiffPointerMinimality verifies that Diff uses value equality for pointer
// fields, not pointer identity (R-DG-026, R-DG-016, R-DG-016). Two independently-allocated
// strings with equal content must diff as unchanged (SetPointer==nil); differing
// values and nil↔non-nil transitions must diff as changed.
func TestDiffPointerMinimality(t *testing.T) {
	id := eddt.EntityID{1}
	t1 := time.Now()
	t2 := t1.Add(time.Second)

	makePtr := func(s string) *string { v := s; return &v }
	base := func(seq uint64, ts time.Time) atomic_all.AtomicAllSnapshot {
		var s atomic_all.AtomicAllSnapshot
		s.Header = eddt.Header{EntityID: id, ChainID: "c", Sequence: seq, EffectiveAt: ts}
		s.Key = "key"
		return s
	}

	// Case 1: equal values at different addresses → SetPointer must be nil.
	a1 := base(1, t1)
	b1 := base(2, t2)
	a1.Pointer = makePtr("hello")
	b1.Pointer = makePtr("hello") // different allocation, same content
	d1, err := atomic_all.Diff(a1, b1)
	if err != nil {
		t.Fatalf("case1 Diff: %v", err)
	}
	if d1.SetPointer != nil {
		t.Errorf("case1: equal-value/different-address pointers: want SetPointer=nil, got non-nil (identity comparison bug)")
	}

	// Case 2: differing values → SetPointer must be non-nil.
	a2 := base(1, t1)
	b2 := base(2, t2)
	a2.Pointer = makePtr("hello")
	b2.Pointer = makePtr("world")
	d2, err := atomic_all.Diff(a2, b2)
	if err != nil {
		t.Fatalf("case2 Diff: %v", err)
	}
	if d2.SetPointer == nil {
		t.Errorf("case2: differing values: want SetPointer non-nil, got nil")
	}

	// Case 3: nil → non-nil → SetPointer must be non-nil.
	a3 := base(1, t1)
	b3 := base(2, t2)
	b3.Pointer = makePtr("hello")
	d3, err := atomic_all.Diff(a3, b3)
	if err != nil {
		t.Fatalf("case3 Diff: %v", err)
	}
	if d3.SetPointer == nil {
		t.Errorf("case3: nil→non-nil: want SetPointer non-nil, got nil")
	}

	// Case 4: non-nil → nil → SetPointer must be non-nil.
	a4 := base(1, t1)
	b4 := base(2, t2)
	a4.Pointer = makePtr("hello")
	d4, err := atomic_all.Diff(a4, b4)
	if err != nil {
		t.Fatalf("case4 Diff: %v", err)
	}
	if d4.SetPointer == nil {
		t.Errorf("case4: non-nil→nil: want SetPointer non-nil, got nil")
	}
}
`
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
// Covers: R-DG-012, R-DG-013, R-DG-025, R-DG-012
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
// Covers: R-DG-012, R-DG-013, R-DG-025, R-DG-012
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
// Covers: R-DG-012, R-DG-013, R-DG-025, R-DG-012
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
// x (payload-wise). Taken literally, Diff(y,y) collides with R-DG-024 when
// y.Sequence == x.Sequence because HeaderAfterApply requires strict monotonicity.
// We therefore construct y with y.Sequence > x.Sequence and identical payload:
// Diff(y,y) has all Set* nil (identity-diff, R-DG-024), and applying it to x leaves
// the payload unchanged while advancing the Header.
// Covers: R-DG-012, R-DG-013, R-DG-025, R-DG-012
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
	// Identity-diff: all Set* must be nil (R-DG-024 / R-DG-024 documents this).
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
// Covers: R-DG-012, R-DG-013, R-DG-025, R-DG-012
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
// subsequent deltas are applied. Pins the R-DG-012 zero-return-on-error contract.
// Covers: R-DG-012, R-DG-013, R-DG-012
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

	// R-DG-012: zero T returned on error, not a partial result.
	var zero atomic_all.AtomicAllSnapshot
	if !reflect.DeepEqual(result, zero) {
		t.Errorf("expected zero AtomicAllSnapshot on error")
	}
}

// TestCoalesceErrorMidFold verifies that a sequence regression in the second
// delta stops the fold and returns (zero T, non-nil error). The first delta is
// valid and has already been applied. Coalesce returns the zero value rather
// than the partial intermediate state — pins the R-DG-012 contract.
// Covers: R-DG-012, R-DG-013, R-DG-012
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

	// R-DG-012: zero T returned on error, not the partial intermediate state.
	var zero atomic_all.AtomicAllSnapshot
	if !reflect.DeepEqual(result, zero) {
		t.Errorf("expected zero AtomicAllSnapshot on error, got non-zero result")
	}
}
`
	// entityIDTestCode exercises EntityID generation against the atomic_all
	// fixture (Key string, raw basic). makeSnap is defined in diff_test.go
	// (same package atomic_all_test).
	entityIDTestCode := `package atomic_all_test

import (
	"testing"

	"atomic_all"
	eddt "go.resystems.io/eddt/runtime"
)

// TestEntityID_Determinism verifies that EntityID returns the same value for
// the same input across 100 calls.
// Covers: R-DG-012, R-DG-014
func TestEntityID_Determinism(t *testing.T) {
	want := atomic_all.EntityID("ABC")
	for i := 0; i < 100; i++ {
		if atomic_all.EntityID("ABC") != want {
			t.Fatalf("EntityID not deterministic on call %d", i)
		}
	}
}

// TestEntityID_DistinctOnDifferentInput verifies that distinct string inputs
// produce distinct EntityIDs. Length-prefix in runtime.WriteString prevents
// concatenation collisions.
// Covers: R-DG-012, R-DG-014
func TestEntityID_DistinctOnDifferentInput(t *testing.T) {
	ids := []eddt.EntityID{
		atomic_all.EntityID(""),
		atomic_all.EntityID("A"),
		atomic_all.EntityID("B"),
		atomic_all.EntityID("AB"),
		atomic_all.EntityID("BA"),
	}
	for i := range ids {
		for j := i + 1; j < len(ids); j++ {
			if ids[i] == ids[j] {
				t.Errorf("EntityID collision: ids[%d] == ids[%d] (%x)", i, j, ids[i])
			}
		}
	}
}

// TestEntityID_ZeroValueIsNonZero verifies that EntityID for a zero-value string
// key produces a non-zero EntityID. Blake2b-256 of the length-prefix encoding
// of "" is not all-zero, so the zero-key hash is not a sentinel value.
// Covers: R-DG-012, R-DG-014
func TestEntityID_ZeroValueIsNonZero(t *testing.T) {
	id := atomic_all.EntityID("")
	if id.IsZero() {
		t.Error("EntityID(\"\") must not be zero; zero EntityID is not a sentinel for unset keys")
	}
}

// TestEntityID_GoldenBytes verifies that the generated EntityID function
// produces the same digest as manually invoking the runtime helpers. This pins
// the hash across process boundaries: if the generated code or the runtime
// changes incompatibly, this test catches the divergence.
// Covers: R-DG-012, R-DG-014
func TestEntityID_GoldenBytes(t *testing.T) {
	// Compute the expected digest using the same runtime helpers the generated
	// code uses. If the generated code and the reference compute identically,
	// both produce the same Blake2b-256 digest.
	h := eddt.NewHash()
	eddt.WriteString(h, "hello")
	expected := eddt.Finalise(h)

	got := atomic_all.EntityID("hello")
	if got != expected {
		t.Errorf("EntityID(\"hello\") = %x, want %x", got, expected)
	}
}
`
	runEmittedInModule(t, runOpts{
		pkgName:      "atomic_all",
		snapshotSrc:  srcCode,
		generatedSrc: generatedSrc,
		extraFiles: map[string]string{
			"apply_test.go":     applyTestCode,
			"diff_test.go":      diffTestCode,
			"coalesce_test.go":  coalesceTestCode,
			"entity_id_test.go": entityIDTestCode,
		},
		runArgs: []string{"./..."},
	})
}

// compileCheckEmitStructKey writes the generated source (plus a matching source
// Snapshot package) for the entityid_struct_key fixture into an isolated temp
// module, then runs go test ./... to exercise EntityID behaviour for struct keys:
// method-form delegation (requirement 13), distinctness, and length-prefix
// collision avoidance (requirement 11).
func compileCheckEmitStructKey(t *testing.T, generatedSrc []byte) {
	t.Helper()

	srcCode := `package entityid_struct_key

import eddt "go.resystems.io/eddt/runtime"

var _ eddt.Header

type SomeKey struct {
	IMSI  string
	SubID uint64
}

type EntityIDStructKeySnapshot struct {
	eddt.Header
	Key    SomeKey ` + "`eddt:\"entity.key\"`" + `
	Status int32
}
`

	testCode := `package entityid_struct_key_test

import (
	"testing"

	"entityid_struct_key"
	eddt "go.resystems.io/eddt/runtime"
)

// TestEntityID_StructKey_Method verifies that the same-package method wrapper
// on SomeKey delegates to the package-level EntityID function and produces the
// same result.
// Covers: R-DG-012, R-DG-014
func TestEntityID_StructKey_Method(t *testing.T) {
	k := entityid_struct_key.SomeKey{IMSI: "310260000000001", SubID: 42}
	id1 := entityid_struct_key.EntityID(k)
	id2 := k.EntityID()
	if id1 != id2 {
		t.Errorf("method and function forms diverge: %x vs %x", id1, id2)
	}
	if id1.IsZero() {
		t.Error("EntityID must not be zero for a non-zero key")
	}
}

// TestEntityID_StructKey_DistinctFields verifies that changing a single sub-
// field of a struct key produces a different EntityID.
// Covers: R-DG-012, R-DG-014
func TestEntityID_StructKey_DistinctFields(t *testing.T) {
	base := entityid_struct_key.SomeKey{IMSI: "A", SubID: 0}
	diffIMSI := entityid_struct_key.SomeKey{IMSI: "B", SubID: 0}
	diffSubID := entityid_struct_key.SomeKey{IMSI: "A", SubID: 1}

	if entityid_struct_key.EntityID(base) == entityid_struct_key.EntityID(diffIMSI) {
		t.Error("changing IMSI should produce a different EntityID")
	}
	if entityid_struct_key.EntityID(base) == entityid_struct_key.EntityID(diffSubID) {
		t.Error("changing SubID should produce a different EntityID")
	}
}

// TestEntityID_StructKey_LengthPrefixPreventsConcatCollision verifies that
// runtime.WriteString's length prefix prevents keys that would collide under
// naive concatenation from producing the same EntityID.
// Covers: R-DG-012, R-DG-014
func TestEntityID_StructKey_LengthPrefixPreventsConcatCollision(t *testing.T) {
	// Without length prefix: WriteString("AB")+WriteUint64(0) and
	// WriteString("A")+WriteUint64(0x42) would both start with "A..." bytes.
	// With the 8-byte length prefix the byte streams differ unambiguously.
	k1 := entityid_struct_key.SomeKey{IMSI: "AB", SubID: 0}
	k2 := entityid_struct_key.SomeKey{IMSI: "A", SubID: 0x42}
	if entityid_struct_key.EntityID(k1) == entityid_struct_key.EntityID(k2) {
		t.Error("length-prefix collision: distinct keys produced the same EntityID")
	}
}

// TestEntityID_StructKey_Determinism verifies that EntityID is deterministic
// for struct keys across 100 calls.
// Covers: R-DG-012, R-DG-014
func TestEntityID_StructKey_Determinism(t *testing.T) {
	k := entityid_struct_key.SomeKey{IMSI: "310260000000001", SubID: 7}
	want := entityid_struct_key.EntityID(k)
	for i := 0; i < 100; i++ {
		if entityid_struct_key.EntityID(k) != want {
			t.Fatalf("EntityID not deterministic on call %d", i)
		}
	}
}

// TestEntityID_StructKey_GoldenBytes verifies that the generated EntityID
// matches manually invoking the runtime helpers.
// Covers: R-DG-012, R-DG-014
func TestEntityID_StructKey_GoldenBytes(t *testing.T) {
	k := entityid_struct_key.SomeKey{IMSI: "hello", SubID: 42}

	h := eddt.NewHash()
	eddt.WriteString(h, "hello")
	eddt.WriteUint64(h, 42)
	expected := eddt.Finalise(h)

	got := entityid_struct_key.EntityID(k)
	if got != expected {
		t.Errorf("EntityID struct golden: got %x, want %x", got, expected)
	}
}
`
	runEmittedInModule(t, runOpts{
		pkgName:      "entityid_struct_key",
		snapshotSrc:  srcCode,
		generatedSrc: generatedSrc,
		extraFiles:   map[string]string{"entity_id_struct_test.go": testCode},
		runArgs:      []string{"./..."},
	})
}

// compileCheckEmitStructKeyReversed generates EntityID code for a struct key
// whose sub-fields are declared in reverse-alphabetical order (SubID before
// IMSI) and verifies runtime behaviour. The golden-bytes test asserts the same
// expected digest as TestEntityID_StructKey_GoldenBytes in the non-reversed
// fixture — proving that field declaration order does not affect the hash.
func compileCheckEmitStructKeyReversed(t *testing.T, generatedSrc []byte) {
	t.Helper()

	// SubID is declared before IMSI — the opposite of alphabetical order.
	srcCode := `package entityid_struct_key_reversed

import eddt "go.resystems.io/eddt/runtime"

var _ eddt.Header

type ReversedKey struct {
	SubID uint64
	IMSI  string
}

type EntityIDReversedKeySnapshot struct {
	eddt.Header
	Key    ReversedKey ` + "`eddt:\"entity.key\"`" + `
	Status int32
}
`

	testCode := `package entityid_struct_key_reversed_test

import (
	"testing"

	"entityid_struct_key_reversed"
	eddt "go.resystems.io/eddt/runtime"
)

// TestEntityID_ReversedKey_Method verifies the same-package method wrapper on
// ReversedKey delegates to the package-level EntityID function.
// Covers: R-DG-012, R-DG-014
func TestEntityID_ReversedKey_Method(t *testing.T) {
	k := entityid_struct_key_reversed.ReversedKey{IMSI: "310260000000001", SubID: 42}
	id1 := entityid_struct_key_reversed.EntityID(k)
	id2 := k.EntityID()
	if id1 != id2 {
		t.Errorf("method and function forms diverge: %x vs %x", id1, id2)
	}
}

// TestEntityID_FieldOrderStabilityGolden is the key field-order-stability
// proof: the golden hash for {IMSI:"hello", SubID:42} must be identical
// whether the struct declares IMSI first (entityid_struct_key) or SubID first
// (this package). Both must hash IMSI before SubID (lexicographic order).
// The expected hash is computed inline using the same runtime helpers in
// alphabetical field-name order, matching TestEntityID_StructKey_GoldenBytes.
// Covers: R-DG-012, R-DG-014
func TestEntityID_FieldOrderStabilityGolden(t *testing.T) {
	k := entityid_struct_key_reversed.ReversedKey{IMSI: "hello", SubID: 42}

	// Compute expected hash: IMSI (alphabetically first) then SubID.
	h := eddt.NewHash()
	eddt.WriteString(h, "hello") // IMSI
	eddt.WriteUint64(h, 42)      // SubID
	expected := eddt.Finalise(h)

	got := entityid_struct_key_reversed.EntityID(k)
	if got != expected {
		t.Errorf("field-order stability: got %x, want %x", got, expected)
	}
}
`
	runEmittedInModule(t, runOpts{
		pkgName:      "entityid_struct_key_reversed",
		snapshotSrc:  srcCode,
		generatedSrc: generatedSrc,
		extraFiles:   map[string]string{"entity_id_reversed_test.go": testCode},
		runArgs:      []string{"./..."},
	})
}

// ── R-DG-016: delta.nested struct-value tests ────────────────────────────────────

// TestEmitTemplate_Nested_SamePkg verifies end-to-end R-DG-016 emission for a
// Snapshot with one delta.nested struct-value field in same-package mode.
// Covers: R-DG-015, R-DG-016, R-DG-009, R-DG-021, R-DG-019, R-DG-003, R-DG-006
func TestEmitTemplate_Nested_SamePkg(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "nested_struct_delta.go")

	cfg := Config{
		InputPkgs:     []string{"./testdata/emit/nested_struct"},
		TargetStructs: []string{"NestedStructSnapshot"},
		OutPath:       outPath,
	}
	if err := New(cfg).Run(); err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	assertGofmtClean(t, outPath)

	src, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("reading output: %v", err)
	}
	srcStr := string(src)

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, outPath, src, 0)
	if err != nil {
		t.Fatalf("generated file is not valid Go: %v\n--- source ---\n%s", err, src)
	}

	// InnerDelta companion struct must exist (req 01).
	innerFields := assertDeltaShape(t, f, "InnerDelta", []string{"SetX", "SetY"})
	// InnerDelta must NOT have runtime.Header (not a chain anchor).
	if contains(innerFields, "Header") {
		t.Errorf("InnerDelta must not embed runtime.Header; fields: %v", innerFields)
	}

	// Parent Delta must have Sub InnerDelta (not *InnerDelta, no Set prefix) (req 05).
	parentFields := assertDeltaShape(t, f, "NestedStructSnapshotDelta", []string{"Sub"})
	if contains(parentFields, "SetSub") {
		t.Errorf("nested field must be Sub not SetSub; fields: %v", parentFields)
	}
	if strings.Contains(srcStr, "*InnerDelta") {
		t.Errorf("nested Delta field must not be pointer-wrapped (*InnerDelta)")
	}

	// Package-level ApplyInner function must exist (req 02).
	if findFuncDecl(f, "ApplyInner") == nil {
		t.Errorf("package-level ApplyInner function not found")
	}
	// Package-level DiffInner function must exist (req 03).
	if findFuncDecl(f, "DiffInner") == nil {
		t.Errorf("package-level DiffInner function not found")
	}

	// Same-package method wrappers must exist (req 02, 03).
	assertHasMethods(t, f, "Inner", []string{"Apply", "Diff"})

	// No Coalesce on Inner (req 04).
	if findMethodDecl(f, "Inner", "Coalesce") != nil {
		t.Errorf("Coalesce must not be emitted for nested type Inner")
	}
	if findFuncDecl(f, "CoalesceInner") != nil {
		t.Errorf("CoalesceInner must not be emitted for nested type Inner")
	}

	// Parent Apply uses method call for same-pkg nested (req 05).
	if !strings.Contains(srcStr, "s.Sub.Apply(d.Sub)") {
		t.Errorf("parent Apply body missing s.Sub.Apply(d.Sub)")
	}
	// Parent Diff uses method call for same-pkg nested (req 05).
	if !strings.Contains(srcStr, "a.Sub.Diff(b.Sub)") {
		t.Errorf("parent Diff body missing a.Sub.Diff(b.Sub)")
	}

	// ApplyInner body: result := u, per-field nil-checks (req 02).
	if !strings.Contains(srcStr, "result := u") {
		t.Errorf("ApplyInner body missing 'result := u'")
	}

	t.Run("CompileCheck", func(t *testing.T) {
		compileCheckEmitNested(t, src)
	})
}

// TestEmitTemplate_Nested_Dedup verifies that two delta.nested fields of the
// same type emit a single companion Delta type, not two copies (req 09).
// Covers: R-DG-016
func TestEmitTemplate_Nested_Dedup(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "nested_multi_delta.go")

	cfg := Config{
		InputPkgs:     []string{"./testdata/emit/nested_multi"},
		TargetStructs: []string{"NestedMultiSnapshot"},
		OutPath:       outPath,
	}
	if err := New(cfg).Run(); err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	assertGofmtClean(t, outPath)

	src, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("reading output: %v", err)
	}
	srcStr := string(src)

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, outPath, src, 0)
	if err != nil {
		t.Fatalf("generated file is not valid Go: %v\n--- source ---\n%s", err, src)
	}

	// AddressDelta must be declared exactly once.
	count := strings.Count(srcStr, "type AddressDelta struct")
	if count != 1 {
		t.Errorf("AddressDelta declared %d times, want exactly 1", count)
	}
	// MetaDelta must also be declared.
	if findStructDecl(f, "MetaDelta") == nil {
		t.Errorf("MetaDelta not found")
	}

	// Parent Delta must have both Home and Work as AddressDelta.
	assertDeltaShape(t, f, "NestedMultiSnapshotDelta", []string{"Home", "Work", "Info"})
}

// TestEmitTemplate_Nested_Deep verifies two-level nested emission: Level2Delta
// and Level1Delta are both emitted, Level1Delta contains Sub Level2Delta, and
// the root Apply/Diff delegate transitively.
// Covers: R-DG-016 (multi-level)
func TestEmitTemplate_Nested_Deep(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "nested_deep_delta.go")

	cfg := Config{
		InputPkgs:     []string{"./testdata/emit/nested_deep"},
		TargetStructs: []string{"NestedDeepSnapshot"},
		OutPath:       outPath,
	}
	if err := New(cfg).Run(); err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	assertGofmtClean(t, outPath)

	src, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("reading output: %v", err)
	}
	srcStr := string(src)

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, outPath, src, 0)
	if err != nil {
		t.Fatalf("generated file is not valid Go: %v\n--- source ---\n%s", err, src)
	}

	// Both companion types must be emitted (req 01).
	if findStructDecl(f, "Level2Delta") == nil {
		t.Fatalf("Level2Delta not found")
	}

	// Level1Delta must have Sub Level2Delta (not *Level2Delta).
	assertDeltaShape(t, f, "Level1Delta", []string{"Sub"})
	if strings.Contains(srcStr, "*Level2Delta") {
		t.Errorf("Level2Delta must not be pointer-wrapped in Level1Delta")
	}

	// ApplyLevel1 body must delegate to u.Sub.Apply(d.Sub) (same-pkg, req 05).
	if !strings.Contains(srcStr, "u.Sub.Apply(d.Sub)") {
		t.Errorf("ApplyLevel1 body missing u.Sub.Apply(d.Sub)")
	}
	// Root Apply must delegate to s.Inner.Apply(d.Inner).
	if !strings.Contains(srcStr, "s.Inner.Apply(d.Inner)") {
		t.Errorf("root Apply body missing s.Inner.Apply(d.Inner)")
	}

	t.Run("CompileCheck", func(t *testing.T) {
		compileCheckEmitNestedDeep(t, src)
	})
}

// TestEmitTemplate_Nested_Triple verifies three-level nested emission:
// Level3Delta, Level2Delta, and Level1Delta are all emitted, Level2Delta
// contains Stats Level3Delta, and Apply/Diff delegate transitively at all
// levels.
// Covers: R-DG-009
func TestEmitTemplate_Nested_Triple(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "nested_triple_delta.go")

	cfg := Config{
		InputPkgs:     []string{"./testdata/emit/nested_triple"},
		TargetStructs: []string{"NestedTripleSnapshot"},
		OutPath:       outPath,
	}
	if err := New(cfg).Run(); err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	assertGofmtClean(t, outPath)

	src, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("reading output: %v", err)
	}
	srcStr := string(src)

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, outPath, src, 0)
	if err != nil {
		t.Fatalf("generated file is not valid Go: %v\n--- source ---\n%s", err, src)
	}

	// All three companion types must be emitted.
	for _, name := range []string{"Level3Delta", "Level2Delta", "Level1Delta"} {
		if findStructDecl(f, name) == nil {
			t.Fatalf("%s not found in generated output", name)
		}
	}

	// Level2Delta must have Stats Level3Delta (not *Level3Delta).
	assertDeltaShape(t, f, "Level2Delta", []string{"Stats"})
	if strings.Contains(srcStr, "*Level3Delta") {
		t.Errorf("Level3Delta must not be pointer-wrapped in Level2Delta")
	}

	// ApplyLevel2 body must delegate to u.Stats.Apply(d.Stats).
	if !strings.Contains(srcStr, "u.Stats.Apply(d.Stats)") {
		t.Errorf("ApplyLevel2 body missing u.Stats.Apply(d.Stats)")
	}
	// Root Apply must delegate to s.Root.Apply(d.Root).
	if !strings.Contains(srcStr, "s.Root.Apply(d.Root)") {
		t.Errorf("root Apply body missing s.Root.Apply(d.Root)")
	}

	t.Run("CompileCheck", func(t *testing.T) {
		compileCheckEmitNestedTriple(t, src)
	})
}

// TestEmitTemplate_Nested_CrossPkg verifies that in cross-package mode nested
// types emit only package-level functions (no method wrappers), and the parent
// Apply/Diff use function call syntax (req 06).
// Covers: R-DG-006, R-DG-012, R-DG-013, R-DG-019
func TestEmitTemplate_Nested_CrossPkg(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "nested_cross_delta.go")

	cfg := Config{
		InputPkgs:          []string{"./testdata/emit/nested_struct"},
		TargetStructs:      []string{"NestedStructSnapshot"},
		OutPath:            outPath,
		OutPkgNameOverride: "deltas",
	}
	if err := New(cfg).Run(); err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	assertGofmtClean(t, outPath)

	src, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("reading output: %v", err)
	}
	srcStr := string(src)

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, outPath, src, 0)
	if err != nil {
		t.Fatalf("generated file is not valid Go: %v\n--- source ---\n%s", err, src)
	}

	// Package-level functions must exist (req 06).
	if findFuncDecl(f, "ApplyInner") == nil {
		t.Errorf("ApplyInner function not found in cross-pkg output")
	}
	if findFuncDecl(f, "DiffInner") == nil {
		t.Errorf("DiffInner function not found in cross-pkg output")
	}

	// Method wrappers must NOT be emitted (req 06, R-DG-012, R-DG-013, R-DG-019).
	if findMethodDecl(f, "Inner", "Apply") != nil {
		t.Errorf("Apply method wrapper on Inner must not be emitted in cross-pkg mode")
	}
	if findMethodDecl(f, "Inner", "Diff") != nil {
		t.Errorf("Diff method wrapper on Inner must not be emitted in cross-pkg mode")
	}

	// Parent Apply must use function call, not method call (req 05, 06).
	if !strings.Contains(srcStr, "ApplyInner(s.Sub, d.Sub)") {
		t.Errorf("cross-pkg parent Apply body missing ApplyInner(s.Sub, d.Sub)")
	}
	if !strings.Contains(srcStr, "DiffInner(a.Sub, b.Sub)") {
		t.Errorf("cross-pkg parent Diff body missing DiffInner(a.Sub, b.Sub)")
	}
}

// TestEmitTemplate_Nested_AnonymousStruct_Error verifies that a delta.nested
// field with an anonymous struct type returns an error requiring a named type
// (req 08).
// Covers: R-DG-013, R-DG-016
func TestEmitTemplate_Nested_AnonymousStruct_Error(t *testing.T) {
	// Build a ParsedSnapshot with a delta.nested field whose GoType is an
	// anonymous struct (not *types.Named).
	anonSt := types.NewStruct([]*types.Var{
		types.NewVar(0, nil, "X", types.Typ[types.Int32]),
	}, nil)

	flt := types.Typ[types.String]
	keyVar := types.NewVar(0, nil, "Key", flt)
	headerVar := types.NewVar(0, nil, "Header", flt) // dummy

	ps := &ParsedSnapshot{
		Name:      "AnonNestedSnapshot",
		PkgPath:   "test",
		PkgName:   "test",
		HeaderVar: headerVar,
		KeyVar:    keyVar,
		KeyShape:  ShapeScalar,
		Fields: []ParsedField{
			{
				Name:   "Sub",
				Shape:  ShapeStructValue,
				GoType: anonSt,
				Tag:    ParsedTag{Kind: TagKindNested, Raw: "delta.nested"},
			},
		},
	}

	opts := emitOpts{crossPackage: false}
	qualifier, _, _ := buildImports([]*ParsedSnapshot{ps}, opts)

	_, err := buildSnapshotView(ps, qualifier, true)
	if err == nil {
		t.Fatal("expected error for anonymous nested struct type, got nil")
	}
	if !strings.Contains(err.Error(), "named type") {
		t.Errorf("error should mention 'named type', got: %v", err)
	}
}

// TestBuildSnapshotView_CycleDetected verifies that the emit stage returns a
// clear error when the delta.nested type graph contains a cycle (A.F → B,
// B.G → A). Struct-value cycles cannot exist in valid Go source; the graph is
// constructed directly via go/types to exercise the inPath guard (R-DG-009 §3.3.2).
// Covers: R-DG-009
func TestBuildSnapshotView_CycleDetected(t *testing.T) {
	pkg := types.NewPackage("test/cycle", "cycle")

	objA := types.NewTypeName(0, pkg, "A", nil)
	typeA := types.NewNamed(objA, nil, nil)

	objB := types.NewTypeName(0, pkg, "B", nil)
	typeB := types.NewNamed(objB, nil, nil)

	fieldAF := types.NewVar(0, pkg, "F", typeB)
	structA := types.NewStruct([]*types.Var{fieldAF}, []string{`eddt:"delta.nested"`})
	typeA.SetUnderlying(structA)

	fieldBG := types.NewVar(0, pkg, "G", typeA)
	structB := types.NewStruct([]*types.Var{fieldBG}, []string{`eddt:"delta.nested"`})
	typeB.SetUnderlying(structB)

	keyVar := types.NewVar(0, nil, "Key", types.Typ[types.String])
	headerVar := types.NewVar(0, nil, "Header", types.Typ[types.String])

	ps := &ParsedSnapshot{
		Name:      "CycleSnapshot",
		PkgPath:   "test/cycle",
		PkgName:   "cycle",
		HeaderVar: headerVar,
		KeyVar:    keyVar,
		KeyShape:  ShapeScalar,
		Fields: []ParsedField{
			{
				Name:   "Root",
				Shape:  ShapeStructValue,
				GoType: typeA,
				Tag:    ParsedTag{Kind: TagKindNested, Raw: "delta.nested"},
			},
		},
	}

	opts := emitOpts{crossPackage: false}
	qualifier, _, _ := buildImports([]*ParsedSnapshot{ps}, opts)

	_, err := buildSnapshotView(ps, qualifier, true)
	if err == nil {
		t.Fatal("expected cycle error from buildSnapshotView, got nil")
	}
	if !strings.Contains(err.Error(), "cycle") {
		t.Errorf("error should mention 'cycle', got: %v", err)
	}
	if !strings.Contains(err.Error(), "§3.3.2") {
		t.Errorf("error should mention §3.3.2, got: %v", err)
	}
}

// compileCheckEmitNested writes the generated nested_struct source into an
// isolated temp module and runs go test to verify runtime correctness.
// Covers: R-DG-016, R-DG-009, R-DG-021, R-DG-019, R-DG-003, R-DG-006
func compileCheckEmitNested(t *testing.T, generatedSrc []byte) {
	t.Helper()

	srcCode := `package nested_struct

import eddt "go.resystems.io/eddt/runtime"

var _ eddt.Header

type Inner struct {
	X int32
	Y string
}

type NestedStructSnapshot struct {
	eddt.Header
	Key   string ` + "`eddt:\"entity.key\"`" + `
	Sub   Inner  ` + "`eddt:\"delta.nested\"`" + `
	Label string
}
`

	testCode := `package nested_struct_test

import (
	"testing"
	"time"

	"nested_struct"
	eddt "go.resystems.io/eddt/runtime"
)

func id1() eddt.EntityID { return eddt.EntityID{1} }

func makeSnap(seq uint64, labelFill int) nested_struct.NestedStructSnapshot {
	var s nested_struct.NestedStructSnapshot
	s.Header = eddt.Header{EntityID: id1(), ChainID: "c", Sequence: seq, EffectiveAt: time.Now()}
	s.Key = "k"
	s.Sub = nested_struct.Inner{X: int32(labelFill), Y: "v" + string(rune('A'+labelFill))}
	s.Label = "lbl" + string(rune('A'+labelFill))
	return s
}

// TestNested_Apply_ChangesNestedField covers req 02, 05.
func TestNested_Apply_ChangesNestedField(t *testing.T) {
	a := makeSnap(1, 0)
	x := int32(99)
	innerD := nested_struct.InnerDelta{SetX: &x}
	var d nested_struct.NestedStructSnapshotDelta
	d.Header = eddt.Header{EntityID: id1(), ChainID: "c", Sequence: 2, EffectiveAt: time.Now()}
	d.Sub = innerD

	result, err := nested_struct.Apply(a, d)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if result.Sub.X != 99 {
		t.Errorf("Sub.X: got %d, want 99", result.Sub.X)
	}
	if result.Sub.Y != a.Sub.Y {
		t.Errorf("Sub.Y changed unexpectedly: got %q", result.Sub.Y)
	}
	if result.Label != a.Label {
		t.Errorf("Label changed unexpectedly: got %q", result.Label)
	}
}

// TestNested_Diff_RoundTrip covers req 02, 03, 05.
func TestNested_Diff_RoundTrip(t *testing.T) {
	a := makeSnap(1, 0)
	b := makeSnap(2, 1)

	delta, err := nested_struct.Diff(a, b)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	result, err := nested_struct.Apply(a, delta)
	if err != nil {
		t.Fatalf("Apply(a, Diff(a,b)): %v", err)
	}
	if result.Sub.X != b.Sub.X {
		t.Errorf("Sub.X: got %d, want %d", result.Sub.X, b.Sub.X)
	}
	if result.Sub.Y != b.Sub.Y {
		t.Errorf("Sub.Y: got %q, want %q", result.Sub.Y, b.Sub.Y)
	}
	if result.Label != b.Label {
		t.Errorf("Label: got %q, want %q", result.Label, b.Label)
	}
}

// TestNested_Diff_Minimal verifies that Diff only sets changed sub-fields.
// Covers: req 03
func TestNested_Diff_Minimal(t *testing.T) {
	a := makeSnap(1, 0)
	b := a
	b.Header.Sequence = 2
	b.Sub.X = 77 // only X changes

	delta, err := nested_struct.Diff(a, b)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if delta.Sub.SetX == nil {
		t.Errorf("SetX should be non-nil when X changes")
	}
	if *delta.Sub.SetX != 77 {
		t.Errorf("SetX: got %d, want 77", *delta.Sub.SetX)
	}
	if delta.Sub.SetY != nil {
		t.Errorf("SetY should be nil when Y is unchanged; got %v", delta.Sub.SetY)
	}
}

// TestNested_Coalesce_Root_Works covers req 04, 12.
func TestNested_Coalesce_Root_Works(t *testing.T) {
	a := makeSnap(1, 0)

	x1 := int32(10)
	x2 := int32(20)
	y := "updated"

	mkDelta := func(seq uint64, ix *int32, iy *string) nested_struct.NestedStructSnapshotDelta {
		var d nested_struct.NestedStructSnapshotDelta
		d.Header = eddt.Header{EntityID: id1(), ChainID: "c", Sequence: seq, EffectiveAt: time.Now()}
		d.Sub = nested_struct.InnerDelta{SetX: ix, SetY: iy}
		return d
	}
	ds := []nested_struct.NestedStructSnapshotDelta{
		mkDelta(2, &x1, nil),
		mkDelta(3, &x2, nil),
		mkDelta(4, nil, &y),
	}

	result, err := nested_struct.Coalesce(a, ds)
	if err != nil {
		t.Fatalf("Coalesce: %v", err)
	}
	if result.Sub.X != 20 {
		t.Errorf("Sub.X after coalesce: got %d, want 20", result.Sub.X)
	}
	if result.Sub.Y != "updated" {
		t.Errorf("Sub.Y after coalesce: got %q, want updated", result.Sub.Y)
	}
}
`
	runEmittedInModule(t, runOpts{
		pkgName:      "nested_struct",
		snapshotSrc:  srcCode,
		generatedSrc: generatedSrc,
		extraFiles:   map[string]string{"nested_test.go": testCode},
		runArgs:      []string{"./..."},
	})
}

// compileCheckEmitNestedDeep verifies the two-level nesting fixture compiles
// and a round-trip Apply(a, Diff(a,b)) == b works for changes at both levels.
// Covers: R-DG-016 (multi-level at runtime)
func compileCheckEmitNestedDeep(t *testing.T, generatedSrc []byte) {
	t.Helper()

	srcCode := `package nested_deep

import eddt "go.resystems.io/eddt/runtime"

var _ eddt.Header

type Level2 struct{ Val int32 }

type Level1 struct {
	Count int32
	Sub   Level2 ` + "`eddt:\"delta.nested\"`" + `
}

type NestedDeepSnapshot struct {
	eddt.Header
	Key   string ` + "`eddt:\"entity.key\"`" + `
	Inner Level1 ` + "`eddt:\"delta.nested\"`" + `
	Name  string
}
`

	testCode := `package nested_deep_test

import (
	"testing"
	"time"

	"nested_deep"
	eddt "go.resystems.io/eddt/runtime"
)

// TestDeep_RoundTrip exercises Apply(a, Diff(a,b))==b when both Level1.Count
// and Level1.Sub.Val change.
func TestDeep_RoundTrip(t *testing.T) {
	var a nested_deep.NestedDeepSnapshot
	a.Header = eddt.Header{EntityID: eddt.EntityID{1}, ChainID: "c", Sequence: 1, EffectiveAt: time.Now()}
	a.Key = "k"
	a.Inner = nested_deep.Level1{Count: 5, Sub: nested_deep.Level2{Val: 10}}
	a.Name = "before"

	var b nested_deep.NestedDeepSnapshot
	b.Header = eddt.Header{EntityID: eddt.EntityID{1}, ChainID: "c", Sequence: 2, EffectiveAt: time.Now()}
	b.Key = "k"
	b.Inner = nested_deep.Level1{Count: 7, Sub: nested_deep.Level2{Val: 42}}
	b.Name = "after"

	delta, err := nested_deep.Diff(a, b)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	result, err := nested_deep.Apply(a, delta)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if result.Inner.Count != b.Inner.Count {
		t.Errorf("Inner.Count: got %d, want %d", result.Inner.Count, b.Inner.Count)
	}
	if result.Inner.Sub.Val != b.Inner.Sub.Val {
		t.Errorf("Inner.Sub.Val: got %d, want %d", result.Inner.Sub.Val, b.Inner.Sub.Val)
	}
	if result.Name != b.Name {
		t.Errorf("Name: got %q, want %q", result.Name, b.Name)
	}
}
`
	runEmittedInModule(t, runOpts{
		pkgName:      "nested_deep",
		snapshotSrc:  srcCode,
		generatedSrc: generatedSrc,
		extraFiles:   map[string]string{"nested_deep_test.go": testCode},
		runArgs:      []string{"./..."},
	})
}

// compileCheckEmitNestedTriple verifies the three-level nesting fixture compiles
// and that Apply(a, Diff(a,b))==b works for simultaneous changes at all three
// levels (Level1.Count, Level2.Rank, Level3.Score).
// Covers: R-DG-009
func compileCheckEmitNestedTriple(t *testing.T, generatedSrc []byte) {
	t.Helper()

	srcCode := `package nested_triple

import eddt "go.resystems.io/eddt/runtime"

var _ eddt.Header

type Level3 struct{ Score int32 }

type Level2 struct {
	Rank  int32
	Stats Level3 ` + "`eddt:\"delta.nested\"`" + `
}

type Level1 struct {
	Count int32
	Meta  Level2 ` + "`eddt:\"delta.nested\"`" + `
}

type NestedTripleSnapshot struct {
	eddt.Header
	Key  string ` + "`eddt:\"entity.key\"`" + `
	Root Level1 ` + "`eddt:\"delta.nested\"`" + `
	Name string
}
`

	testCode := `package nested_triple_test

import (
	"testing"
	"time"

	"nested_triple"
	eddt "go.resystems.io/eddt/runtime"
)

// TestTriple_RoundTrip exercises Apply(a, Diff(a,b))==b when Level1.Count,
// Level2.Rank, and Level3.Score all change simultaneously.
func TestTriple_RoundTrip(t *testing.T) {
	var a nested_triple.NestedTripleSnapshot
	a.Header = eddt.Header{EntityID: eddt.EntityID{1}, ChainID: "c", Sequence: 1, EffectiveAt: time.Now()}
	a.Key = "k"
	a.Root = nested_triple.Level1{
		Count: 1,
		Meta:  nested_triple.Level2{Rank: 2, Stats: nested_triple.Level3{Score: 3}},
	}
	a.Name = "before"

	var b nested_triple.NestedTripleSnapshot
	b.Header = eddt.Header{EntityID: eddt.EntityID{1}, ChainID: "c", Sequence: 2, EffectiveAt: time.Now()}
	b.Key = "k"
	b.Root = nested_triple.Level1{
		Count: 10,
		Meta:  nested_triple.Level2{Rank: 20, Stats: nested_triple.Level3{Score: 30}},
	}
	b.Name = "after"

	delta, err := nested_triple.Diff(a, b)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	result, err := nested_triple.Apply(a, delta)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if result.Root.Count != b.Root.Count {
		t.Errorf("Root.Count: got %d, want %d", result.Root.Count, b.Root.Count)
	}
	if result.Root.Meta.Rank != b.Root.Meta.Rank {
		t.Errorf("Root.Meta.Rank: got %d, want %d", result.Root.Meta.Rank, b.Root.Meta.Rank)
	}
	if result.Root.Meta.Stats.Score != b.Root.Meta.Stats.Score {
		t.Errorf("Root.Meta.Stats.Score: got %d, want %d", result.Root.Meta.Stats.Score, b.Root.Meta.Stats.Score)
	}
	if result.Name != b.Name {
		t.Errorf("Name: got %q, want %q", result.Name, b.Name)
	}
}
`
	runEmittedInModule(t, runOpts{
		pkgName:      "nested_triple",
		snapshotSrc:  srcCode,
		generatedSrc: generatedSrc,
		extraFiles:   map[string]string{"nested_triple_test.go": testCode},
		runArgs:      []string{"./..."},
	})
}

// compileCheckEmitNestedMap verifies that the generated nested_map delta source
// compiles and satisfies five runtime contracts (R-DG-016, R-DG-006, R-DG-016 upsert semantics):
//
//  1. Add entry: Diff records new key in UpdatedTags; Apply adds it to result.
//  2. Remove entry: Diff records removed key in RemovedTags; Apply removes it.
//  3. Update entry: changed key appears in UpdatedTags only (never in RemovedTags).
//  4. Round-trip: Apply(a, Diff(a,b)) payload-equals b across simultaneous
//     add/remove/update on both Tags (scalar value) and Scores (struct value).
//  5. Atomic coexistence: Count-only change → SetCount non-nil, maps nil.
//
// Covers: R-DG-016, R-DG-006, R-DG-016
func compileCheckEmitNestedMap(t *testing.T, generatedSrc []byte) {
	t.Helper()

	// Inline the fixture source so the isolated module is self-contained.
	srcCode := `package nested_map

import eddt "go.resystems.io/eddt/runtime"

var _ eddt.Header

// Entry is the struct-value map element type; requires reflect.DeepEqual in Diff.
type Entry struct {
	Score int32
	Label string
}

// NestedMapSnapshot carries two delta.nested map fields and one atomic field.
type NestedMapSnapshot struct {
	eddt.Header
	Key    string            ` + "`eddt:\"entity.key\"`" + `
	Tags   map[string]string ` + "`eddt:\"delta.nested\"`" + `
	Scores map[string]Entry  ` + "`eddt:\"delta.nested\"`" + `
	Count  int32
}
`

	testCode := `package nested_map_test

import (
	"reflect"
	"testing"
	"time"

	"nested_map"
	eddt "go.resystems.io/eddt/runtime"
)

// header returns a minimal Header for test snapshots.
func header(seq uint64) eddt.Header {
	return eddt.Header{EntityID: eddt.EntityID{1}, ChainID: "c", Sequence: seq, EffectiveAt: time.Now()}
}

// TestMap_AddEntry: Diff records new key in UpdatedTags; Apply adds it (R-DG-016 req 1).
func TestMap_AddEntry(t *testing.T) {
	a := nested_map.NestedMapSnapshot{Header: header(1), Key: "k", Tags: map[string]string{"x": "1"}}
	b := nested_map.NestedMapSnapshot{Header: header(2), Key: "k", Tags: map[string]string{"x": "1", "y": "2"}}

	d, err := nested_map.Diff(a, b)
	if err != nil { t.Fatalf("Diff: %v", err) }
	if d.UpdatedTags == nil { t.Fatal("UpdatedTags must not be nil when entry added") }
	if d.UpdatedTags["y"] != "2" { t.Errorf("UpdatedTags[y]: got %q want %q", d.UpdatedTags["y"], "2") }
	if len(d.RemovedTags) != 0 { t.Errorf("RemovedTags must be empty for add-only delta; got %v", d.RemovedTags) }

	result, err := nested_map.Apply(a, d)
	if err != nil { t.Fatalf("Apply: %v", err) }
	if result.Tags["y"] != "2" { t.Errorf("Apply: Tags[y]: got %q want %q", result.Tags["y"], "2") }
	if result.Tags["x"] != "1" { t.Errorf("Apply: Tags[x] must be preserved; got %q", result.Tags["x"]) }
}

// TestMap_RemoveEntry: Diff records removed key in RemovedTags; Apply removes it (R-DG-016 req 2).
func TestMap_RemoveEntry(t *testing.T) {
	a := nested_map.NestedMapSnapshot{Header: header(1), Key: "k", Tags: map[string]string{"x": "1", "y": "2"}}
	b := nested_map.NestedMapSnapshot{Header: header(2), Key: "k", Tags: map[string]string{"x": "1"}}

	d, err := nested_map.Diff(a, b)
	if err != nil { t.Fatalf("Diff: %v", err) }
	if len(d.RemovedTags) != 1 || d.RemovedTags[0] != "y" {
		t.Errorf("RemovedTags: got %v want [y]", d.RemovedTags)
	}
	if d.UpdatedTags != nil { t.Errorf("UpdatedTags must be nil for remove-only delta; got %v", d.UpdatedTags) }

	result, err := nested_map.Apply(a, d)
	if err != nil { t.Fatalf("Apply: %v", err) }
	if _, ok := result.Tags["y"]; ok { t.Error("Apply: key y must have been removed") }
	if result.Tags["x"] != "1" { t.Errorf("Apply: Tags[x] must be preserved; got %q", result.Tags["x"]) }
}

// TestMap_UpdateEntry: changed key appears in UpdatedTags only, not RemovedTags (R-DG-006, R-DG-016 upsert, R-DG-016 req 3).
func TestMap_UpdateEntry(t *testing.T) {
	a := nested_map.NestedMapSnapshot{Header: header(1), Key: "k", Tags: map[string]string{"x": "old"}}
	b := nested_map.NestedMapSnapshot{Header: header(2), Key: "k", Tags: map[string]string{"x": "new"}}

	d, err := nested_map.Diff(a, b)
	if err != nil { t.Fatalf("Diff: %v", err) }
	if d.UpdatedTags == nil || d.UpdatedTags["x"] != "new" {
		t.Errorf("UpdatedTags must have x=new; got %v", d.UpdatedTags)
	}
	// R-DG-006, R-DG-016: a value-changed entry must NOT appear in RemovedTags.
	if len(d.RemovedTags) != 0 {
		t.Errorf("RemovedTags must be empty for update-only delta (R-DG-006, R-DG-016 upsert); got %v", d.RemovedTags)
	}
}

// TestMap_RoundTrip: Apply(a, Diff(a,b))==b for simultaneous add/remove/update on both
// Tags (scalar value) and Scores (struct value with reflect.DeepEqual comparison) (R-DG-016 req 4).
func TestMap_RoundTrip(t *testing.T) {
	a := nested_map.NestedMapSnapshot{
		Header: header(1), Key: "k",
		Tags:   map[string]string{"keep": "v", "change": "old", "drop": "gone"},
		Scores: map[string]nested_map.Entry{
			"keep":   {Score: 1, Label: "kept"},
			"change": {Score: 2, Label: "old-label"},
			"drop":   {Score: 3, Label: "dropped"},
		},
		Count: 5,
	}
	b := nested_map.NestedMapSnapshot{
		Header: header(2), Key: "k",
		Tags:   map[string]string{"keep": "v", "change": "new", "added": "fresh"},
		Scores: map[string]nested_map.Entry{
			"keep":   {Score: 1, Label: "kept"},
			"change": {Score: 2, Label: "new-label"},
			"added":  {Score: 9, Label: "brand-new"},
		},
		Count: 5,
	}

	d, err := nested_map.Diff(a, b)
	if err != nil { t.Fatalf("Diff: %v", err) }

	result, err := nested_map.Apply(a, d)
	if err != nil { t.Fatalf("Apply: %v", err) }

	// Tags round-trip.
	if !reflect.DeepEqual(result.Tags, b.Tags) {
		t.Errorf("Tags round-trip failed: got %v want %v", result.Tags, b.Tags)
	}
	// Scores round-trip.
	if !reflect.DeepEqual(result.Scores, b.Scores) {
		t.Errorf("Scores round-trip failed: got %v want %v", result.Scores, b.Scores)
	}
}

// TestMap_AtomicCoexistence: Count-only change yields non-nil SetCount with nil map deltas (R-DG-016 req 5).
func TestMap_AtomicCoexistence(t *testing.T) {
	tags := map[string]string{"x": "1"}
	a := nested_map.NestedMapSnapshot{Header: header(1), Key: "k", Tags: tags, Count: 1}
	b := nested_map.NestedMapSnapshot{Header: header(2), Key: "k", Tags: tags, Count: 2}

	d, err := nested_map.Diff(a, b)
	if err != nil { t.Fatalf("Diff: %v", err) }
	if d.SetCount == nil { t.Error("SetCount must be non-nil when Count changed") }
	if *d.SetCount != 2 { t.Errorf("SetCount: got %d want 2", *d.SetCount) }
	if d.UpdatedTags != nil { t.Errorf("UpdatedTags must be nil when Tags unchanged; got %v", d.UpdatedTags) }
	if len(d.RemovedTags) != 0 { t.Errorf("RemovedTags must be empty when Tags unchanged; got %v", d.RemovedTags) }
}
`
	runEmittedInModule(t, runOpts{
		pkgName:      "nested_map",
		snapshotSrc:  srcCode,
		generatedSrc: generatedSrc,
		extraFiles:   map[string]string{"nested_map_test.go": testCode},
		runArgs:      []string{"./..."},
	})
}

// compileCheckEmitNestedSlice verifies that the generated nested_slice delta source
// compiles and satisfies five runtime contracts (R-DG-016, R-DG-028, R-DG-006, R-DG-016 set-diff semantics):
//
//  1. Add elements: Diff records new elements in AddedNames; Apply adds them.
//  2. Remove elements: Diff records removed elements in RemovedNames; Apply removes them.
//  3. Add and remove simultaneously: both AddedNames and RemovedNames are populated.
//  4. Round-trip: Apply(a, Diff(a,b)) payload-equals b across simultaneous
//     add/remove on both Names (string) and Tags (comparable struct).
//  5. Atomic coexistence: Count-only change → SetCount non-nil, slice deltas nil.
//
// Covers: R-DG-016, R-DG-028, R-DG-006, R-DG-016
func compileCheckEmitNestedSlice(t *testing.T, generatedSrc []byte) {
	t.Helper()

	// Inline the fixture source so the isolated module is self-contained.
	srcCode := `package nested_slice

import eddt "go.resystems.io/eddt/runtime"

var _ eddt.Header

// Tag is the struct element type; all fields are scalar so Tag is comparable.
type Tag struct {
	Key string
	Val string
}

// NestedSliceSnapshot carries two delta.nested slice fields and one atomic field.
type NestedSliceSnapshot struct {
	eddt.Header
	Key   string   ` + "`eddt:\"entity.key\"`" + `
	Names []string ` + "`eddt:\"delta.nested\"`" + `
	Tags  []Tag    ` + "`eddt:\"delta.nested\"`" + `
	Count int32
}
`

	testCode := `package nested_slice_test

import (
	"reflect"
	"sort"
	"testing"
	"time"

	"nested_slice"
	eddt "go.resystems.io/eddt/runtime"
)

// hdr returns a minimal Header for test snapshots.
func hdr(seq uint64) eddt.Header {
	return eddt.Header{EntityID: eddt.EntityID{1}, ChainID: "c", Sequence: seq, EffectiveAt: time.Now()}
}

// sortedStrings returns a sorted copy of ss for order-independent comparison.
func sortedStrings(ss []string) []string {
	out := make([]string, len(ss))
	copy(out, ss)
	sort.Strings(out)
	return out
}

// TestSlice_AddElements: Diff records new elements in AddedNames; Apply adds them (R-DG-016, R-DG-028 req 1).
func TestSlice_AddElements(t *testing.T) {
	a := nested_slice.NestedSliceSnapshot{Header: hdr(1), Key: "k", Names: []string{"x"}}
	b := nested_slice.NestedSliceSnapshot{Header: hdr(2), Key: "k", Names: []string{"x", "y"}}

	d, err := nested_slice.Diff(a, b)
	if err != nil { t.Fatalf("Diff: %v", err) }
	if len(d.AddedNames) != 1 || d.AddedNames[0] != "y" {
		t.Errorf("AddedNames: got %v want [y]", d.AddedNames)
	}
	if len(d.RemovedNames) != 0 { t.Errorf("RemovedNames must be empty for add-only delta; got %v", d.RemovedNames) }

	result, err := nested_slice.Apply(a, d)
	if err != nil { t.Fatalf("Apply: %v", err) }
	if !reflect.DeepEqual(sortedStrings(result.Names), []string{"x", "y"}) {
		t.Errorf("Apply: Names: got %v want [x y]", result.Names)
	}
}

// TestSlice_RemoveElements: Diff records removed elements in RemovedNames; Apply removes them (R-DG-016, R-DG-028 req 2).
func TestSlice_RemoveElements(t *testing.T) {
	a := nested_slice.NestedSliceSnapshot{Header: hdr(1), Key: "k", Names: []string{"x", "y"}}
	b := nested_slice.NestedSliceSnapshot{Header: hdr(2), Key: "k", Names: []string{"x"}}

	d, err := nested_slice.Diff(a, b)
	if err != nil { t.Fatalf("Diff: %v", err) }
	if len(d.RemovedNames) != 1 || d.RemovedNames[0] != "y" {
		t.Errorf("RemovedNames: got %v want [y]", d.RemovedNames)
	}
	if len(d.AddedNames) != 0 { t.Errorf("AddedNames must be empty for remove-only delta; got %v", d.AddedNames) }

	result, err := nested_slice.Apply(a, d)
	if err != nil { t.Fatalf("Apply: %v", err) }
	if !reflect.DeepEqual(result.Names, []string{"x"}) {
		t.Errorf("Apply: Names: got %v want [x]", result.Names)
	}
}

// TestSlice_AddAndRemove: simultaneous add and remove populates both delta fields (R-DG-016, R-DG-028 req 3).
func TestSlice_AddAndRemove(t *testing.T) {
	a := nested_slice.NestedSliceSnapshot{Header: hdr(1), Key: "k", Names: []string{"keep", "drop"}}
	b := nested_slice.NestedSliceSnapshot{Header: hdr(2), Key: "k", Names: []string{"keep", "new"}}

	d, err := nested_slice.Diff(a, b)
	if err != nil { t.Fatalf("Diff: %v", err) }
	if len(d.AddedNames) != 1 || d.AddedNames[0] != "new" {
		t.Errorf("AddedNames: got %v want [new]", d.AddedNames)
	}
	if len(d.RemovedNames) != 1 || d.RemovedNames[0] != "drop" {
		t.Errorf("RemovedNames: got %v want [drop]", d.RemovedNames)
	}
}

// TestSlice_RoundTrip: Apply(a, Diff(a,b))==b for simultaneous add/remove on both
// Names (string) and Tags (comparable struct) (R-DG-016, R-DG-028 req 4).
func TestSlice_RoundTrip(t *testing.T) {
	a := nested_slice.NestedSliceSnapshot{
		Header: hdr(1), Key: "k",
		Names: []string{"keep", "drop"},
		Tags:  []nested_slice.Tag{{Key: "env", Val: "prod"}, {Key: "region", Val: "eu"}},
		Count: 5,
	}
	b := nested_slice.NestedSliceSnapshot{
		Header: hdr(2), Key: "k",
		Names: []string{"keep", "new"},
		Tags:  []nested_slice.Tag{{Key: "env", Val: "prod"}, {Key: "tier", Val: "hot"}},
		Count: 5,
	}

	d, err := nested_slice.Diff(a, b)
	if err != nil { t.Fatalf("Diff: %v", err) }

	result, err := nested_slice.Apply(a, d)
	if err != nil { t.Fatalf("Apply: %v", err) }

	// Names round-trip (order: survivors in source order, additions appended).
	wantNames := []string{"keep", "new"}
	if !reflect.DeepEqual(result.Names, wantNames) {
		t.Errorf("Names round-trip failed: got %v want %v", result.Names, wantNames)
	}
	// Tags round-trip.
	wantTags := []nested_slice.Tag{{Key: "env", Val: "prod"}, {Key: "tier", Val: "hot"}}
	if !reflect.DeepEqual(result.Tags, wantTags) {
		t.Errorf("Tags round-trip failed: got %v want %v", result.Tags, wantTags)
	}
}

// TestSlice_AtomicCoexistence: Count-only change yields non-nil SetCount with nil slice deltas (R-DG-016, R-DG-028 req 5).
func TestSlice_AtomicCoexistence(t *testing.T) {
	names := []string{"x", "y"}
	a := nested_slice.NestedSliceSnapshot{Header: hdr(1), Key: "k", Names: names, Count: 1}
	b := nested_slice.NestedSliceSnapshot{Header: hdr(2), Key: "k", Names: names, Count: 2}

	d, err := nested_slice.Diff(a, b)
	if err != nil { t.Fatalf("Diff: %v", err) }
	if d.SetCount == nil { t.Error("SetCount must be non-nil when Count changed") }
	if *d.SetCount != 2 { t.Errorf("SetCount: got %d want 2", *d.SetCount) }
	if len(d.AddedNames) != 0 { t.Errorf("AddedNames must be nil when Names unchanged; got %v", d.AddedNames) }
	if len(d.RemovedNames) != 0 { t.Errorf("RemovedNames must be empty when Names unchanged; got %v", d.RemovedNames) }
}
`
	runEmittedInModule(t, runOpts{
		pkgName:      "nested_slice",
		snapshotSrc:  srcCode,
		generatedSrc: generatedSrc,
		extraFiles:   map[string]string{"nested_slice_test.go": testCode},
		runArgs:      []string{"./..."},
	})
}

// compileCheckEmitNestedSliceReflect verifies that the generated nested_slice_reflect
// delta source compiles and satisfies three runtime contracts for the non-comparable
// element path (R-DG-016, R-DG-028, §5.2 reflect.DeepEqual fallback):
//
//  1. Add blob: Diff records new []byte in AddedBlobs; Apply adds it.
//  2. Remove blob: Diff records removed []byte in RemovedBlobs; Apply removes it.
//  3. Round-trip: Apply(a, Diff(a,b)) payload-equals b for simultaneous add/remove.
//
// Covers: R-DG-016, R-DG-028, §5.2 (non-comparable O(n²) path)
func compileCheckEmitNestedSliceReflect(t *testing.T, generatedSrc []byte) {
	t.Helper()

	srcCode := `package nested_slice_reflect

import eddt "go.resystems.io/eddt/runtime"

var _ eddt.Header

// NestedSliceReflectSnapshot carries a delta.nested [][]byte field.
// []byte is not comparable, so generated code uses reflect.DeepEqual (§5.2).
type NestedSliceReflectSnapshot struct {
	eddt.Header
	Key   string   ` + "`eddt:\"entity.key\"`" + `
	Blobs [][]byte ` + "`eddt:\"delta.nested\"`" + `
}
`

	testCode := `package nested_slice_reflect_test

import (
	"reflect"
	"testing"
	"time"

	"nested_slice_reflect"
	eddt "go.resystems.io/eddt/runtime"
)

func blobHdr(seq uint64) eddt.Header {
	return eddt.Header{EntityID: eddt.EntityID{1}, ChainID: "c", Sequence: seq, EffectiveAt: time.Now()}
}

// TestReflect_AddBlob: Diff records new []byte in AddedBlobs; Apply adds it (R-DG-016, R-DG-028 §5.2 req 1).
func TestReflect_AddBlob(t *testing.T) {
	b1 := []byte{1, 2, 3}
	b2 := []byte{4, 5, 6}
	a := nested_slice_reflect.NestedSliceReflectSnapshot{Header: blobHdr(1), Key: "k", Blobs: [][]byte{b1}}
	b := nested_slice_reflect.NestedSliceReflectSnapshot{Header: blobHdr(2), Key: "k", Blobs: [][]byte{b1, b2}}

	d, err := nested_slice_reflect.Diff(a, b)
	if err != nil { t.Fatalf("Diff: %v", err) }
	if len(d.AddedBlobs) != 1 || !reflect.DeepEqual(d.AddedBlobs[0], b2) {
		t.Errorf("AddedBlobs: got %v want [%v]", d.AddedBlobs, b2)
	}
	if len(d.RemovedBlobs) != 0 { t.Errorf("RemovedBlobs must be empty; got %v", d.RemovedBlobs) }

	result, err := nested_slice_reflect.Apply(a, d)
	if err != nil { t.Fatalf("Apply: %v", err) }
	if !reflect.DeepEqual(result.Blobs, b.Blobs) {
		t.Errorf("Apply result mismatch: got %v want %v", result.Blobs, b.Blobs)
	}
}

// TestReflect_RemoveBlob: Diff records removed []byte in RemovedBlobs; Apply removes it (R-DG-016, R-DG-028 §5.2 req 2).
func TestReflect_RemoveBlob(t *testing.T) {
	b1 := []byte{1, 2, 3}
	b2 := []byte{4, 5, 6}
	a := nested_slice_reflect.NestedSliceReflectSnapshot{Header: blobHdr(1), Key: "k", Blobs: [][]byte{b1, b2}}
	b := nested_slice_reflect.NestedSliceReflectSnapshot{Header: blobHdr(2), Key: "k", Blobs: [][]byte{b1}}

	d, err := nested_slice_reflect.Diff(a, b)
	if err != nil { t.Fatalf("Diff: %v", err) }
	if len(d.RemovedBlobs) != 1 || !reflect.DeepEqual(d.RemovedBlobs[0], b2) {
		t.Errorf("RemovedBlobs: got %v want [%v]", d.RemovedBlobs, b2)
	}
	if len(d.AddedBlobs) != 0 { t.Errorf("AddedBlobs must be empty; got %v", d.AddedBlobs) }

	result, err := nested_slice_reflect.Apply(a, d)
	if err != nil { t.Fatalf("Apply: %v", err) }
	if !reflect.DeepEqual(result.Blobs, b.Blobs) {
		t.Errorf("Apply result mismatch: got %v want %v", result.Blobs, b.Blobs)
	}
}

// TestReflect_RoundTrip: Apply(a, Diff(a,b))==b for simultaneous add/remove (R-DG-016, R-DG-028 §5.2 req 3).
func TestReflect_RoundTrip(t *testing.T) {
	keep := []byte{1}
	drop := []byte{2}
	add  := []byte{3}
	a := nested_slice_reflect.NestedSliceReflectSnapshot{Header: blobHdr(1), Key: "k", Blobs: [][]byte{keep, drop}}
	b := nested_slice_reflect.NestedSliceReflectSnapshot{Header: blobHdr(2), Key: "k", Blobs: [][]byte{keep, add}}

	d, err := nested_slice_reflect.Diff(a, b)
	if err != nil { t.Fatalf("Diff: %v", err) }

	result, err := nested_slice_reflect.Apply(a, d)
	if err != nil { t.Fatalf("Apply: %v", err) }

	// Survivor order: keep is first (source order), add is appended (R-DG-028).
	want := [][]byte{keep, add}
	if !reflect.DeepEqual(result.Blobs, want) {
		t.Errorf("RoundTrip: got %v want %v", result.Blobs, want)
	}
}
`
	runEmittedInModule(t, runOpts{
		pkgName:      "nested_slice_reflect",
		snapshotSrc:  srcCode,
		generatedSrc: generatedSrc,
		extraFiles:   map[string]string{"nested_slice_reflect_test.go": testCode},
		runArgs:      []string{"./..."},
	})
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
	pkgs, err := loadPackages([]string{"./testdata/emit/" + fixtureName}, slog.Default())
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
// Covers: R-DG-037
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

// ---------------------------------------------------------------------------
// R-DG-016..07 clearable-envelope template tests
// ---------------------------------------------------------------------------

func TestEmitTemplate_Clearable_Struct_SamePkg(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "clearable_struct_delta.go")

	cfg := Config{
		InputPkgs:     []string{"./testdata/emit/clearable_struct"},
		TargetStructs: []string{"ClearableStructSnapshot"},
		OutPath:       outPath,
	}
	if err := New(cfg).Run(); err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	assertGofmtClean(t, outPath)

	src, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("reading output: %v", err)
	}
	srcStr := string(src)

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, outPath, src, 0)
	if err != nil {
		t.Fatalf("generated file is not valid Go: %v\n--- source ---\n%s", err, src)
	}

	// ClearableStructSnapshotDelta must carry Location as runtime.FieldDelta[AddressDelta].
	if !strings.Contains(srcStr, "runtime.FieldDelta[AddressDelta]") {
		t.Error("expected runtime.FieldDelta[AddressDelta] in generated output")
	}

	// AddressDelta companion must be emitted (R-DG-016 reuse path).
	if findStructDecl(f, "AddressDelta") == nil {
		t.Error("AddressDelta companion struct must be emitted")
	}

	// No map or slice wrapper: Location is struct-typed, not a container.
	if findStructDecl(f, "LocationMapDelta") != nil {
		t.Error("LocationMapDelta must not be emitted for a struct-typed clearable field")
	}
	if findStructDecl(f, "LocationSliceDelta") != nil {
		t.Error("LocationSliceDelta must not be emitted for a struct-typed clearable field")
	}

	// ApplyAddress / DiffAddress must be emitted.
	if findFuncDecl(f, "ApplyAddress") == nil {
		t.Error("ApplyAddress function must be emitted")
	}
	if findFuncDecl(f, "DiffAddress") == nil {
		t.Error("DiffAddress function must be emitted")
	}

	// Apply body must contain the Op-switch for Location.
	for _, frag := range []string{"OpRetract", "OpAssert", "ApplyAddress"} {
		if !strings.Contains(srcStr, frag) {
			t.Errorf("Apply body missing %q fragment", frag)
		}
	}

	// Diff body must contain the three-branch predicate.
	for _, frag := range []string{"DiffAddress", "OpRetract"} {
		if !strings.Contains(srcStr, frag) {
			t.Errorf("Diff body missing %q reference", frag)
		}
	}

	// No reflect import: Address is comparable (all-scalar fields).
	if strings.Contains(srcStr, `"reflect"`) {
		t.Error(`unexpected "reflect" import: Address is comparable`)
	}

	// Method wrappers must be present in same-package mode.
	assertHasMethods(t, f, "ClearableStructSnapshot", []string{"Apply", "Diff"})

	t.Run("CompileCheck", func(t *testing.T) {
		compileCheckEmitClearableStruct(t, src)
	})
}

func TestEmitTemplate_Clearable_Map_SamePkg(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "clearable_map_delta.go")

	cfg := Config{
		InputPkgs:     []string{"./testdata/emit/clearable_map"},
		TargetStructs: []string{"ClearableMapSnapshot"},
		OutPath:       outPath,
	}
	if err := New(cfg).Run(); err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	assertGofmtClean(t, outPath)

	src, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("reading output: %v", err)
	}
	srcStr := string(src)

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, outPath, src, 0)
	if err != nil {
		t.Fatalf("generated file is not valid Go: %v\n--- source ---\n%s", err, src)
	}

	// ClearableMapSnapshotDelta must carry Tags as runtime.FieldDelta[TagsMapDelta].
	if !strings.Contains(srcStr, "runtime.FieldDelta[TagsMapDelta]") {
		t.Error("expected runtime.FieldDelta[TagsMapDelta] in generated output")
	}

	// TagsMapDelta wrapper struct must be emitted with the correct fields.
	assertDeltaShape(t, f, "TagsMapDelta", []string{"UpdatedTags", "RemovedTags"})

	// IsEmpty method and Apply/Diff helpers must be emitted.
	if findMethodDecl(f, "TagsMapDelta", "IsEmpty") == nil {
		t.Error("TagsMapDelta.IsEmpty method must be emitted")
	}
	if findFuncDecl(f, "ApplyTagsMapDelta") == nil {
		t.Error("ApplyTagsMapDelta function must be emitted")
	}
	if findFuncDecl(f, "DiffTagsMapDelta") == nil {
		t.Error("DiffTagsMapDelta function must be emitted")
	}

	// Apply body must contain the Op-switch for Tags.
	for _, frag := range []string{"OpRetract", "OpAssert", "ApplyTagsMapDelta"} {
		if !strings.Contains(srcStr, frag) {
			t.Errorf("Apply body missing %q fragment", frag)
		}
	}

	// Diff body must contain the three-branch predicate.
	for _, frag := range []string{"IsEmpty", "DiffTagsMapDelta", "OpRetract"} {
		if !strings.Contains(srcStr, frag) {
			t.Errorf("Diff body missing %q reference", frag)
		}
	}

	// No reflect import: map[string]string value type is comparable.
	if strings.Contains(srcStr, `"reflect"`) {
		t.Error(`unexpected "reflect" import: string value type is comparable`)
	}

	// Method wrappers must be present in same-package mode.
	assertHasMethods(t, f, "ClearableMapSnapshot", []string{"Apply", "Diff"})

	t.Run("CompileCheck", func(t *testing.T) {
		compileCheckEmitClearableMap(t, src)
	})
}

func TestEmitTemplate_Clearable_Slice_SamePkg(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "clearable_slice_delta.go")

	cfg := Config{
		InputPkgs:     []string{"./testdata/emit/clearable_slice"},
		TargetStructs: []string{"ClearableSliceSnapshot"},
		OutPath:       outPath,
	}
	if err := New(cfg).Run(); err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	assertGofmtClean(t, outPath)

	src, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("reading output: %v", err)
	}
	srcStr := string(src)

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, outPath, src, 0)
	if err != nil {
		t.Fatalf("generated file is not valid Go: %v\n--- source ---\n%s", err, src)
	}

	// ClearableSliceSnapshotDelta must carry Groups as runtime.FieldDelta[GroupsSliceDelta].
	if !strings.Contains(srcStr, "runtime.FieldDelta[GroupsSliceDelta]") {
		t.Error("expected runtime.FieldDelta[GroupsSliceDelta] in generated output")
	}

	// GroupsSliceDelta wrapper struct must be emitted with the correct fields.
	assertDeltaShape(t, f, "GroupsSliceDelta", []string{"AddedGroups", "RemovedGroups"})

	// IsEmpty method and Apply/Diff helpers must be emitted.
	if findMethodDecl(f, "GroupsSliceDelta", "IsEmpty") == nil {
		t.Error("GroupsSliceDelta.IsEmpty method must be emitted")
	}
	if findFuncDecl(f, "ApplyGroupsSliceDelta") == nil {
		t.Error("ApplyGroupsSliceDelta function must be emitted")
	}
	if findFuncDecl(f, "DiffGroupsSliceDelta") == nil {
		t.Error("DiffGroupsSliceDelta function must be emitted")
	}

	// Apply body must contain the Op-switch for Groups.
	for _, frag := range []string{"OpRetract", "OpAssert", "ApplyGroupsSliceDelta"} {
		if !strings.Contains(srcStr, frag) {
			t.Errorf("Apply body missing %q fragment", frag)
		}
	}

	// Diff body must contain the three-branch predicate.
	for _, frag := range []string{"IsEmpty", "DiffGroupsSliceDelta", "OpRetract"} {
		if !strings.Contains(srcStr, frag) {
			t.Errorf("Diff body missing %q reference", frag)
		}
	}

	// No reflect import: string element type is comparable.
	if strings.Contains(srcStr, `"reflect"`) {
		t.Error(`unexpected "reflect" import: string element type is comparable`)
	}

	// Method wrappers must be present in same-package mode.
	assertHasMethods(t, f, "ClearableSliceSnapshot", []string{"Apply", "Diff"})

	t.Run("CompileCheck", func(t *testing.T) {
		compileCheckEmitClearableSlice(t, src)
	})
}

func TestEmitTemplate_Clearable_Map_Reflect_SamePkg(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "clearable_map_reflect_delta.go")

	cfg := Config{
		InputPkgs:     []string{"./testdata/emit/clearable_map_reflect"},
		TargetStructs: []string{"ClearableMapReflectSnapshot"},
		OutPath:       outPath,
	}
	if err := New(cfg).Run(); err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	assertGofmtClean(t, outPath)

	src, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("reading output: %v", err)
	}
	srcStr := string(src)

	// reflect import must be present: Bag contains a slice → non-comparable value type.
	if !strings.Contains(srcStr, `"reflect"`) {
		t.Error(`"reflect" import must be present: Bag is non-comparable (contains a slice)`)
	}
	if !strings.Contains(srcStr, "reflect.DeepEqual") {
		t.Error("reflect.DeepEqual must appear in DiffTagsMapDelta for non-comparable value type")
	}
}

func TestEmitTemplate_Clearable_Slice_Reflect_SamePkg(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "clearable_slice_reflect_delta.go")

	cfg := Config{
		InputPkgs:     []string{"./testdata/emit/clearable_slice_reflect"},
		TargetStructs: []string{"ClearableSliceReflectSnapshot"},
		OutPath:       outPath,
	}
	if err := New(cfg).Run(); err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	assertGofmtClean(t, outPath)

	src, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("reading output: %v", err)
	}
	srcStr := string(src)

	// reflect import must be present: []byte element is non-comparable.
	if !strings.Contains(srcStr, `"reflect"`) {
		t.Error(`"reflect" import must be present: []byte is non-comparable`)
	}
	if !strings.Contains(srcStr, "reflect.DeepEqual") {
		t.Error("reflect.DeepEqual must appear in DiffBlobsSliceDelta for non-comparable element type")
	}
}

func TestEmitTemplate_NestedOnly_NoFieldDelta(t *testing.T) {
	cases := []struct {
		name   string
		pkg    string
		target string
	}{
		{"nested_map", "./testdata/emit/nested_map", "NestedMapSnapshot"},
		{"nested_slice", "./testdata/emit/nested_slice", "NestedSliceSnapshot"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			outPath := filepath.Join(t.TempDir(), "delta.go")
			cfg := Config{
				InputPkgs:     []string{tc.pkg},
				TargetStructs: []string{tc.target},
				OutPath:       outPath,
			}
			if err := New(cfg).Run(); err != nil {
				t.Fatalf("Run() failed: %v", err)
			}
			src, err := os.ReadFile(outPath)
			if err != nil {
				t.Fatalf("reading output: %v", err)
			}
			srcStr := string(src)
			for _, tok := range []string{"runtime.FieldDelta", "IsEmpty"} {
				if strings.Contains(srcStr, tok) {
					t.Errorf("nested-only output must not contain %q (byte-identical regression)", tok)
				}
			}
		})
	}
}

func compileCheckEmitClearableStruct(t *testing.T, generatedSrc []byte) {
	t.Helper()

	srcCode := `package clearable_struct

import eddt "go.resystems.io/eddt/runtime"

var _ eddt.Header

type Address struct {
	Street string
	City   string
}

type ClearableStructSnapshot struct {
	eddt.Header
	Key      string  ` + "`eddt:\"entity.key\"`" + `
	Location Address ` + "`eddt:\"delta.nested,delta.clearable\"`" + `
	Count    int32
}
`

	testCode := `package clearable_struct_test

import (
	"testing"
	"time"

	"clearable_struct"
	eddt "go.resystems.io/eddt/runtime"
)

func hdrCS(seq uint64) eddt.Header {
	return eddt.Header{EntityID: eddt.EntityID{1}, ChainID: "c", Sequence: seq, EffectiveAt: time.Now()}
}

var (
	addrA = clearable_struct.Address{Street: "1 Main St", City: "Springfield"}
	addrB = clearable_struct.Address{Street: "2 Oak Ave", City: "Shelbyville"}
)

// TestClearableStruct_OpIgnore: equal Location → Diff produces OpIgnore → Apply propagates it.
func TestClearableStruct_OpIgnore(t *testing.T) {
	a := clearable_struct.ClearableStructSnapshot{Header: hdrCS(1), Key: "k", Location: addrA, Count: 3}
	b := clearable_struct.ClearableStructSnapshot{Header: hdrCS(2), Key: "k", Location: addrA, Count: 3}
	d, err := clearable_struct.Diff(a, b)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if d.Location.Op != eddt.OpIgnore {
		t.Errorf("equal Location must yield OpIgnore; got %v", d.Location.Op)
	}
	result, err := clearable_struct.Apply(a, d)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if result.Location != addrA {
		t.Errorf("OpIgnore: Location must propagate unchanged; got %v", result.Location)
	}
}

// TestClearableStruct_OpRetract: non-zero→zero Location → Diff produces OpRetract → Apply resets to Address{}.
func TestClearableStruct_OpRetract(t *testing.T) {
	a := clearable_struct.ClearableStructSnapshot{Header: hdrCS(1), Key: "k", Location: addrA}
	b := clearable_struct.ClearableStructSnapshot{Header: hdrCS(2), Key: "k", Location: clearable_struct.Address{}}
	d, err := clearable_struct.Diff(a, b)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if d.Location.Op != eddt.OpRetract {
		t.Errorf("non-zero to zero Location must yield OpRetract; got %v", d.Location.Op)
	}
	result, err := clearable_struct.Apply(a, d)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if result.Location != (clearable_struct.Address{}) {
		t.Errorf("OpRetract: Location must be zero Address{}; got %v", result.Location)
	}
}

// TestClearableStruct_OpAssert: different non-zero locations → Diff produces OpAssert → Apply sets to b.Location.
func TestClearableStruct_OpAssert(t *testing.T) {
	a := clearable_struct.ClearableStructSnapshot{Header: hdrCS(1), Key: "k", Location: addrA}
	b := clearable_struct.ClearableStructSnapshot{Header: hdrCS(2), Key: "k", Location: addrB}
	d, err := clearable_struct.Diff(a, b)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if d.Location.Op != eddt.OpAssert {
		t.Errorf("changed Location must yield OpAssert; got %v", d.Location.Op)
	}
	result, err := clearable_struct.Apply(a, d)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if result.Location != addrB {
		t.Errorf("OpAssert: Location must equal addrB; got %v", result.Location)
	}
}

// TestClearableStruct_RoundTrip: Apply(a, Diff(a,b)).Location == b.Location for all three Op cases.
func TestClearableStruct_RoundTrip(t *testing.T) {
	base := clearable_struct.ClearableStructSnapshot{Header: hdrCS(1), Key: "k", Location: addrA, Count: 5}
	targets := []clearable_struct.ClearableStructSnapshot{
		{Header: hdrCS(2), Key: "k", Location: addrA, Count: 5},
		{Header: hdrCS(2), Key: "k", Location: clearable_struct.Address{}, Count: 5},
		{Header: hdrCS(2), Key: "k", Location: addrB, Count: 7},
	}
	for _, target := range targets {
		d, err := clearable_struct.Diff(base, target)
		if err != nil {
			t.Fatalf("Diff: %v", err)
		}
		result, err := clearable_struct.Apply(base, d)
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if result.Location != target.Location {
			t.Errorf("round-trip Location: got %v want %v", result.Location, target.Location)
		}
		if result.Count != target.Count {
			t.Errorf("round-trip Count: got %d want %d", result.Count, target.Count)
		}
	}
}
`
	runEmittedInModule(t, runOpts{
		pkgName:      "clearable_struct",
		snapshotSrc:  srcCode,
		generatedSrc: generatedSrc,
		extraFiles:   map[string]string{"clearable_struct_test.go": testCode},
		runArgs:      []string{"./..."},
	})
}

func compileCheckEmitClearableMap(t *testing.T, generatedSrc []byte) {
	t.Helper()

	srcCode := `package clearable_map

import eddt "go.resystems.io/eddt/runtime"

var _ eddt.Header

type ClearableMapSnapshot struct {
	eddt.Header
	Key   string            ` + "`eddt:\"entity.key\"`" + `
	Tags  map[string]string ` + "`eddt:\"delta.nested,delta.clearable\"`" + `
	Count int32
}
`

	testCode := `package clearable_map_test

import (
	"reflect"
	"testing"
	"time"

	"clearable_map"
	eddt "go.resystems.io/eddt/runtime"
)

func hdrCM(seq uint64) eddt.Header {
	return eddt.Header{EntityID: eddt.EntityID{1}, ChainID: "c", Sequence: seq, EffectiveAt: time.Now()}
}

// TestClearableMap_OpIgnore: equal Tags → Diff produces OpIgnore → Apply propagates Tags.
func TestClearableMap_OpIgnore(t *testing.T) {
	tags := map[string]string{"x": "1"}
	a := clearable_map.ClearableMapSnapshot{Header: hdrCM(1), Key: "k", Tags: tags}
	b := clearable_map.ClearableMapSnapshot{Header: hdrCM(2), Key: "k", Tags: map[string]string{"x": "1"}}
	d, err := clearable_map.Diff(a, b)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if d.Tags.Op != eddt.OpIgnore {
		t.Errorf("equal Tags must yield OpIgnore; got %v", d.Tags.Op)
	}
	result, err := clearable_map.Apply(a, d)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !reflect.DeepEqual(result.Tags, tags) {
		t.Errorf("OpIgnore: Tags must propagate unchanged; got %v", result.Tags)
	}
}

// TestClearableMap_OpRetract: non-empty → nil Tags → Diff produces OpRetract → Apply sets to nil.
func TestClearableMap_OpRetract(t *testing.T) {
	a := clearable_map.ClearableMapSnapshot{Header: hdrCM(1), Key: "k", Tags: map[string]string{"x": "1"}}
	b := clearable_map.ClearableMapSnapshot{Header: hdrCM(2), Key: "k", Tags: nil}
	d, err := clearable_map.Diff(a, b)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if d.Tags.Op != eddt.OpRetract {
		t.Errorf("non-empty to nil Tags must yield OpRetract; got %v", d.Tags.Op)
	}
	result, err := clearable_map.Apply(a, d)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if result.Tags != nil {
		t.Errorf("OpRetract: Tags must be nil; got %v", result.Tags)
	}
}

// TestClearableMap_OpAssert: different non-empty Tags → Diff produces OpAssert → Apply applies inner delta.
func TestClearableMap_OpAssert(t *testing.T) {
	a := clearable_map.ClearableMapSnapshot{Header: hdrCM(1), Key: "k", Tags: map[string]string{"x": "1", "y": "2"}}
	b := clearable_map.ClearableMapSnapshot{Header: hdrCM(2), Key: "k", Tags: map[string]string{"x": "1", "z": "3"}}
	d, err := clearable_map.Diff(a, b)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if d.Tags.Op != eddt.OpAssert {
		t.Errorf("changed Tags must yield OpAssert; got %v", d.Tags.Op)
	}
	result, err := clearable_map.Apply(a, d)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !reflect.DeepEqual(result.Tags, b.Tags) {
		t.Errorf("OpAssert: Tags must match b; got %v want %v", result.Tags, b.Tags)
	}
}

// TestClearableMap_RoundTrip: Apply(a, Diff(a,b)).Tags == b.Tags for all three Op cases.
func TestClearableMap_RoundTrip(t *testing.T) {
	base := clearable_map.ClearableMapSnapshot{
		Header: hdrCM(1), Key: "k",
		Tags:   map[string]string{"keep": "v", "drop": "gone"},
		Count:  2,
	}
	targets := []clearable_map.ClearableMapSnapshot{
		{Header: hdrCM(2), Key: "k", Tags: map[string]string{"keep": "v", "drop": "gone"}, Count: 2},
		{Header: hdrCM(2), Key: "k", Tags: nil, Count: 2},
		{Header: hdrCM(2), Key: "k", Tags: map[string]string{"keep": "v", "added": "new"}, Count: 5},
	}
	for _, target := range targets {
		d, err := clearable_map.Diff(base, target)
		if err != nil {
			t.Fatalf("Diff: %v", err)
		}
		result, err := clearable_map.Apply(base, d)
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if !reflect.DeepEqual(result.Tags, target.Tags) {
			t.Errorf("round-trip Tags: got %v want %v", result.Tags, target.Tags)
		}
		if result.Count != target.Count {
			t.Errorf("round-trip Count: got %d want %d", result.Count, target.Count)
		}
	}
}

// TestClearableMap_AtomicCoexistence: Count-only change yields SetCount with no Tags change.
func TestClearableMap_AtomicCoexistence(t *testing.T) {
	tags := map[string]string{"x": "1"}
	a := clearable_map.ClearableMapSnapshot{Header: hdrCM(1), Key: "k", Tags: tags, Count: 1}
	b := clearable_map.ClearableMapSnapshot{Header: hdrCM(2), Key: "k", Tags: map[string]string{"x": "1"}, Count: 2}
	d, err := clearable_map.Diff(a, b)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if d.SetCount == nil || *d.SetCount != 2 {
		t.Errorf("SetCount must be 2; got %v", d.SetCount)
	}
	if d.Tags.Op != eddt.OpIgnore {
		t.Errorf("Tags must be OpIgnore when unchanged; got %v", d.Tags.Op)
	}
}
`
	runEmittedInModule(t, runOpts{
		pkgName:      "clearable_map",
		snapshotSrc:  srcCode,
		generatedSrc: generatedSrc,
		extraFiles:   map[string]string{"clearable_map_test.go": testCode},
		runArgs:      []string{"./..."},
	})
}

func compileCheckEmitClearableSlice(t *testing.T, generatedSrc []byte) {
	t.Helper()

	srcCode := `package clearable_slice

import eddt "go.resystems.io/eddt/runtime"

var _ eddt.Header

type ClearableSliceSnapshot struct {
	eddt.Header
	Key    string   ` + "`eddt:\"entity.key\"`" + `
	Groups []string ` + "`eddt:\"delta.nested,delta.clearable\"`" + `
	Count  int32
}
`
	testCode := `package clearable_slice_test

import (
	"reflect"
	"testing"
	"time"

	"clearable_slice"
	eddt "go.resystems.io/eddt/runtime"
)

func hdrCSl(seq uint64) eddt.Header {
	return eddt.Header{EntityID: eddt.EntityID{1}, ChainID: "c", Sequence: seq, EffectiveAt: time.Now()}
}

// TestClearableSlice_OpIgnore: equal Groups → Diff produces OpIgnore → Apply propagates Groups.
func TestClearableSlice_OpIgnore(t *testing.T) {
	groups := []string{"a", "b"}
	a := clearable_slice.ClearableSliceSnapshot{Header: hdrCSl(1), Key: "k", Groups: groups}
	b := clearable_slice.ClearableSliceSnapshot{Header: hdrCSl(2), Key: "k", Groups: []string{"a", "b"}}
	d, err := clearable_slice.Diff(a, b)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if d.Groups.Op != eddt.OpIgnore {
		t.Errorf("equal Groups must yield OpIgnore; got %v", d.Groups.Op)
	}
	result, err := clearable_slice.Apply(a, d)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !reflect.DeepEqual(result.Groups, groups) {
		t.Errorf("OpIgnore: Groups must propagate unchanged; got %v", result.Groups)
	}
}

// TestClearableSlice_OpRetract: non-empty → nil Groups → Diff produces OpRetract → Apply sets to nil.
func TestClearableSlice_OpRetract(t *testing.T) {
	a := clearable_slice.ClearableSliceSnapshot{Header: hdrCSl(1), Key: "k", Groups: []string{"a", "b"}}
	b := clearable_slice.ClearableSliceSnapshot{Header: hdrCSl(2), Key: "k", Groups: nil}
	d, err := clearable_slice.Diff(a, b)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if d.Groups.Op != eddt.OpRetract {
		t.Errorf("non-empty to nil Groups must yield OpRetract; got %v", d.Groups.Op)
	}
	result, err := clearable_slice.Apply(a, d)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if result.Groups != nil {
		t.Errorf("OpRetract: Groups must be nil; got %v", result.Groups)
	}
}

// TestClearableSlice_OpAssert: different non-empty Groups → Diff produces OpAssert → Apply applies inner delta.
func TestClearableSlice_OpAssert(t *testing.T) {
	a := clearable_slice.ClearableSliceSnapshot{Header: hdrCSl(1), Key: "k", Groups: []string{"a", "b"}}
	b := clearable_slice.ClearableSliceSnapshot{Header: hdrCSl(2), Key: "k", Groups: []string{"a", "c"}}
	d, err := clearable_slice.Diff(a, b)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if d.Groups.Op != eddt.OpAssert {
		t.Errorf("changed Groups must yield OpAssert; got %v", d.Groups.Op)
	}
	result, err := clearable_slice.Apply(a, d)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	found := make(map[string]bool, len(result.Groups))
	for _, g := range result.Groups {
		found[g] = true
	}
	if !found["a"] {
		t.Error("OpAssert: Groups must contain a")
	}
	if !found["c"] {
		t.Error("OpAssert: Groups must contain c")
	}
	if found["b"] {
		t.Error("OpAssert: Groups must not contain b (removed)")
	}
}

// TestClearableSlice_RoundTrip_EmptyToNonEmpty: nil → non-empty asserts correctly.
func TestClearableSlice_RoundTrip_EmptyToNonEmpty(t *testing.T) {
	a := clearable_slice.ClearableSliceSnapshot{Header: hdrCSl(1), Key: "k", Groups: nil}
	b := clearable_slice.ClearableSliceSnapshot{Header: hdrCSl(2), Key: "k", Groups: []string{"new"}}
	d, err := clearable_slice.Diff(a, b)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if d.Groups.Op != eddt.OpAssert {
		t.Errorf("nil to non-empty must yield OpAssert; got %v", d.Groups.Op)
	}
	result, err := clearable_slice.Apply(a, d)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !reflect.DeepEqual(result.Groups, b.Groups) {
		t.Errorf("RoundTrip: got %v want %v", result.Groups, b.Groups)
	}
}

// TestClearableSlice_AtomicCoexistence: Count-only change yields SetCount with no Groups change.
func TestClearableSlice_AtomicCoexistence(t *testing.T) {
	groups := []string{"x", "y"}
	a := clearable_slice.ClearableSliceSnapshot{Header: hdrCSl(1), Key: "k", Groups: groups, Count: 1}
	b := clearable_slice.ClearableSliceSnapshot{Header: hdrCSl(2), Key: "k", Groups: []string{"x", "y"}, Count: 2}
	d, err := clearable_slice.Diff(a, b)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if d.SetCount == nil || *d.SetCount != 2 {
		t.Errorf("SetCount must be 2; got %v", d.SetCount)
	}
	if d.Groups.Op != eddt.OpIgnore {
		t.Errorf("Groups must be OpIgnore when unchanged; got %v", d.Groups.Op)
	}
}
`
	runEmittedInModule(t, runOpts{
		pkgName:      "clearable_slice",
		snapshotSrc:  srcCode,
		generatedSrc: generatedSrc,
		extraFiles:   map[string]string{"clearable_slice_test.go": testCode},
		runArgs:      []string{"./..."},
	})
}

// TestEmitTemplate_Clearable_Struct_Reflect_SamePkg verifies R-DG-016..07 generation
// for a clearable struct field whose inner type is non-comparable (contains a slice).
// ClearableStructEqReflect=true must trigger reflect.DeepEqual in the emitted Diff.
// Covers: R-DG-016, R-DG-026, R-DG-026.
func TestEmitTemplate_Clearable_Struct_Reflect_SamePkg(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "clearable_struct_reflect_delta.go")
	cfg := Config{
		InputPkgs:     []string{"./testdata/emit/clearable_struct_reflect"},
		TargetStructs: []string{"ClearableStructReflectSnapshot"},
		OutPath:       outPath,
	}
	if err := New(cfg).Run(); err != nil {
		t.Fatalf("Run() failed: %v", err)
	}
	assertGofmtClean(t, outPath)
	src, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("reading output: %v", err)
	}
	srcStr := string(src)
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, outPath, src, 0)
	if err != nil {
		t.Fatalf("generated file is not valid Go: %v\n--- source ---\n%s", err, src)
	}

	// reflect import required: LogEntry is non-comparable.
	if !strings.Contains(srcStr, `"reflect"`) {
		t.Error(`expected "reflect" import: LogEntry contains a slice and is non-comparable`)
	}
	// reflect.DeepEqual must appear in the diff for the clearable comparison.
	if !strings.Contains(srcStr, "reflect.DeepEqual") {
		t.Error("expected reflect.DeepEqual in generated Diff for non-comparable clearable struct")
	}
	// LogEntryDelta companion must be emitted (R-DG-016 path).
	if findStructDecl(f, "LogEntryDelta") == nil {
		t.Error("LogEntryDelta companion struct must be emitted")
	}
	// Delta must carry Latest as FieldDelta[LogEntryDelta].
	if !strings.Contains(srcStr, "runtime.FieldDelta[LogEntryDelta]") {
		t.Error("expected runtime.FieldDelta[LogEntryDelta] in generated output")
	}
	// Method wrappers present in same-package mode.
	assertHasMethods(t, f, "ClearableStructReflectSnapshot", []string{"Apply", "Diff"})

	t.Run("CompileAndRun", func(t *testing.T) {
		runEmittedInModule(t, runOpts{
			pkgName:      "clearable_struct_reflect",
			fixtureDir:   "testdata/emit/clearable_struct_reflect",
			generatedSrc: src,
			extraFiles:   map[string]string{"clearable_struct_reflect_test.go": clearableStructReflectRunTest},
			runArgs:      []string{"./..."},
		})
	})
}

const clearableStructReflectRunTest = `package clearable_struct_reflect_test

import (
	"reflect"
	"testing"
	"time"

	"clearable_struct_reflect"
	eddt "go.resystems.io/eddt/runtime"
)

func hdrCSR(seq uint64) eddt.Header {
	return eddt.Header{EntityID: eddt.EntityID{1}, ChainID: "c", Sequence: seq, EffectiveAt: time.Now()}
}

var (
	logA = clearable_struct_reflect.LogEntry{Message: "hello", Tags: []string{"a", "b"}}
	logB = clearable_struct_reflect.LogEntry{Message: "world", Tags: []string{"c"}}
)

func TestClearableStructReflect_OpIgnore(t *testing.T) {
	a := clearable_struct_reflect.ClearableStructReflectSnapshot{Header: hdrCSR(1), Key: "k", Latest: logA}
	b := clearable_struct_reflect.ClearableStructReflectSnapshot{Header: hdrCSR(2), Key: "k", Latest: logA}
	d, err := clearable_struct_reflect.Diff(a, b)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if d.Latest.Op != eddt.OpIgnore {
		t.Errorf("equal Latest must yield OpIgnore; got %v", d.Latest.Op)
	}
	result, err := clearable_struct_reflect.Apply(a, d)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !reflect.DeepEqual(result.Latest, logA) {
		t.Errorf("OpIgnore: Latest must propagate unchanged; got %v", result.Latest)
	}
}

func TestClearableStructReflect_OpRetract(t *testing.T) {
	a := clearable_struct_reflect.ClearableStructReflectSnapshot{Header: hdrCSR(1), Key: "k", Latest: logA}
	b := clearable_struct_reflect.ClearableStructReflectSnapshot{Header: hdrCSR(2), Key: "k"}
	d, err := clearable_struct_reflect.Diff(a, b)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if d.Latest.Op != eddt.OpRetract {
		t.Errorf("non-zero→zero Latest must yield OpRetract; got %v", d.Latest.Op)
	}
	result, err := clearable_struct_reflect.Apply(a, d)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !reflect.DeepEqual(result.Latest, clearable_struct_reflect.LogEntry{}) {
		t.Errorf("OpRetract: Latest must be zero LogEntry{}; got %v", result.Latest)
	}
}

func TestClearableStructReflect_OpAssert(t *testing.T) {
	a := clearable_struct_reflect.ClearableStructReflectSnapshot{Header: hdrCSR(1), Key: "k", Latest: logA}
	b := clearable_struct_reflect.ClearableStructReflectSnapshot{Header: hdrCSR(2), Key: "k", Latest: logB}
	d, err := clearable_struct_reflect.Diff(a, b)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if d.Latest.Op != eddt.OpAssert {
		t.Errorf("changed Latest must yield OpAssert; got %v", d.Latest.Op)
	}
	result, err := clearable_struct_reflect.Apply(a, d)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !reflect.DeepEqual(result.Latest, logB) {
		t.Errorf("OpAssert: Latest must equal logB; got %v", result.Latest)
	}
}
`

// TestEmitTemplate_Clearable_Pointer_SamePkg verifies R-DG-016..07 generation for
// a clearable struct field whose inner struct contains pointer sub-fields.
// ContactInfo is comparable (pointer equality), so ClearableStructEqReflect=false
// and no reflect import is needed.  The ContactInfoDelta companion must carry
// SetPhone **string for the *string sub-field.
// Covers: R-DG-016, R-DG-026, R-DG-026.
func TestEmitTemplate_Clearable_Pointer_SamePkg(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "clearable_pointer_delta.go")
	cfg := Config{
		InputPkgs:     []string{"./testdata/emit/clearable_pointer"},
		TargetStructs: []string{"ClearablePointerSnapshot"},
		OutPath:       outPath,
	}
	if err := New(cfg).Run(); err != nil {
		t.Fatalf("Run() failed: %v", err)
	}
	assertGofmtClean(t, outPath)
	src, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("reading output: %v", err)
	}
	srcStr := string(src)
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, outPath, src, 0)
	if err != nil {
		t.Fatalf("generated file is not valid Go: %v\n--- source ---\n%s", err, src)
	}

	// ContactInfoDelta companion must be emitted.
	if findStructDecl(f, "ContactInfoDelta") == nil {
		t.Error("ContactInfoDelta companion struct must be emitted")
	}
	// SetPhone field must be **string (pointer-to-pointer for *string source field).
	if !strings.Contains(srcStr, "SetPhone **string") {
		t.Error("expected SetPhone **string delta field for *string sub-field in ContactInfoDelta")
	}
	// No reflect import: ContactInfo is comparable (pointer comparison).
	if strings.Contains(srcStr, `"reflect"`) {
		t.Error(`unexpected "reflect" import: ContactInfo is comparable`)
	}
	// Delta carries Contact as FieldDelta[ContactInfoDelta].
	if !strings.Contains(srcStr, "runtime.FieldDelta[ContactInfoDelta]") {
		t.Error("expected runtime.FieldDelta[ContactInfoDelta] in generated output")
	}
	// Method wrappers present in same-package mode.
	assertHasMethods(t, f, "ClearablePointerSnapshot", []string{"Apply", "Diff"})

	t.Run("CompileAndRun", func(t *testing.T) {
		runEmittedInModule(t, runOpts{
			pkgName:      "clearable_pointer",
			fixtureDir:   "testdata/emit/clearable_pointer",
			generatedSrc: src,
			extraFiles:   map[string]string{"clearable_pointer_test.go": clearablePointerRunTest},
			runArgs:      []string{"./..."},
		})
	})
}

const clearablePointerRunTest = `package clearable_pointer_test

import (
	"testing"
	"time"

	"clearable_pointer"
	eddt "go.resystems.io/eddt/runtime"
)

func hdrCP(seq uint64) eddt.Header {
	return eddt.Header{EntityID: eddt.EntityID{1}, ChainID: "c", Sequence: seq, EffectiveAt: time.Now()}
}

func TestClearablePointer_OpIgnore(t *testing.T) {
	phone := "555-1234"
	info := clearable_pointer.ContactInfo{Name: "Alice", Phone: &phone}
	a := clearable_pointer.ClearablePointerSnapshot{Header: hdrCP(1), Key: "k", Contact: info}
	b := clearable_pointer.ClearablePointerSnapshot{Header: hdrCP(2), Key: "k", Contact: info}
	d, err := clearable_pointer.Diff(a, b)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if d.Contact.Op != eddt.OpIgnore {
		t.Errorf("equal Contact must yield OpIgnore; got %v", d.Contact.Op)
	}
	result, err := clearable_pointer.Apply(a, d)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if result.Contact != info {
		t.Errorf("OpIgnore: Contact must propagate unchanged; got %v", result.Contact)
	}
}

func TestClearablePointer_OpRetract(t *testing.T) {
	phone := "555-1234"
	info := clearable_pointer.ContactInfo{Name: "Alice", Phone: &phone}
	a := clearable_pointer.ClearablePointerSnapshot{Header: hdrCP(1), Key: "k", Contact: info}
	b := clearable_pointer.ClearablePointerSnapshot{Header: hdrCP(2), Key: "k"}
	d, err := clearable_pointer.Diff(a, b)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if d.Contact.Op != eddt.OpRetract {
		t.Errorf("non-zero→zero Contact must yield OpRetract; got %v", d.Contact.Op)
	}
	result, err := clearable_pointer.Apply(a, d)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if result.Contact != (clearable_pointer.ContactInfo{}) {
		t.Errorf("OpRetract: Contact must be zero ContactInfo{}; got %v", result.Contact)
	}
}

func TestClearablePointer_OpAssert(t *testing.T) {
	phone1 := "555-1234"
	phone2 := "555-5678"
	infoA := clearable_pointer.ContactInfo{Name: "Alice", Phone: &phone1}
	infoB := clearable_pointer.ContactInfo{Name: "Bob", Phone: &phone2}
	a := clearable_pointer.ClearablePointerSnapshot{Header: hdrCP(1), Key: "k", Contact: infoA}
	b := clearable_pointer.ClearablePointerSnapshot{Header: hdrCP(2), Key: "k", Contact: infoB}
	d, err := clearable_pointer.Diff(a, b)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if d.Contact.Op != eddt.OpAssert {
		t.Errorf("changed Contact must yield OpAssert; got %v", d.Contact.Op)
	}
	result, err := clearable_pointer.Apply(a, d)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if result.Contact.Name != infoB.Name {
		t.Errorf("OpAssert: Contact.Name must be %q; got %q", infoB.Name, result.Contact.Name)
	}
	if result.Contact.Phone == nil || *result.Contact.Phone != *infoB.Phone {
		t.Errorf("OpAssert: Contact.Phone must equal infoB.Phone")
	}
}
`

// TestEmitTemplate_Clearable_CrossPkg verifies clearable struct+map+slice emission
// in cross-package mode (OutPkgNameOverride="deltas").
// Wrapper types (TagDelta, AttrsMapDelta, GroupsSliceDelta) live in the output
// package; the zero-composite for the clearable struct field must be model.Tag{}.
// No method wrappers are emitted in cross-package mode (R-DG-012, R-DG-013, R-DG-019).
// Covers: R-DG-016, R-DG-026, R-DG-012, R-DG-013, R-DG-019, R-DG-026.
func TestEmitTemplate_Clearable_CrossPkg(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "clearable_crosspkg_delta.go")
	cfg := Config{
		InputPkgs:          []string{"./testdata/emit/clearable_crosspkg/model"},
		TargetStructs:      []string{"ClearableCrossPkgSnapshot"},
		OutPath:            outPath,
		OutPkgNameOverride: "deltas",
	}
	if err := New(cfg).Run(); err != nil {
		t.Fatalf("Run() failed: %v", err)
	}
	assertGofmtClean(t, outPath)
	src, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("reading output: %v", err)
	}
	srcStr := string(src)
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, outPath, src, 0)
	if err != nil {
		t.Fatalf("generated file is not valid Go: %v\n--- source ---\n%s", err, src)
	}

	// Output package must be the override.
	if !strings.Contains(srcStr, "package deltas") {
		t.Errorf("expected 'package deltas', got:\n%s", srcStr)
	}
	// Source-package types must be qualified in Apply/Diff/Coalesce signatures.
	if !strings.Contains(srcStr, "model.ClearableCrossPkgSnapshot") {
		t.Error("expected 'model.ClearableCrossPkgSnapshot' in function signatures")
	}
	// Zero-composite for the clearable struct field must be model.Tag{}.
	if !strings.Contains(srcStr, "model.Tag{}") {
		t.Error("expected 'model.Tag{}' as zero-composite for clearable struct field")
	}
	// All three clearable wrapper types must be emitted in the output package.
	for _, want := range []string{"TagDelta", "AttrsMapDelta", "GroupsSliceDelta"} {
		if findStructDecl(f, want) == nil {
			t.Errorf("struct %s must be emitted in cross-package output", want)
		}
	}
	// No method wrappers in cross-package mode (R-DG-012, R-DG-013, R-DG-019).
	if findMethodDecl(f, "ClearableCrossPkgSnapshot", "Apply") != nil {
		t.Error("Apply method wrapper must not be emitted in cross-package mode (R-DG-012, R-DG-013, R-DG-019)")
	}
	// Source-package import must be present.
	if !strings.Contains(srcStr, "clearable_crosspkg/model") {
		t.Error("expected source-package import in cross-package output")
	}
}
