package gencommon

import (
	"os"
	"path/filepath"
	"testing"
)

// fixtureGenericCode is the source for a self-contained module that defines a
// FieldDelta[T] generic type (matching the shape of runtime.FieldDelta[T]) and
// a parent Snapshot struct with two generic-instantiation fields: one with a
// scalar type argument and one with a pointer-to-struct type argument.
//
// Using a local definition keeps the fixture independent of the runtime package,
// which does not exist yet (Phase 1).
const fixtureGenericCode = `package model

type FieldDeltaOp int8

type FieldDelta[T any] struct {
	Op    FieldDeltaOp
	Value T
}

type Inner struct {
	Z string
}

type Snapshot struct {
	Seq       int64
	Scalar    FieldDelta[int32]  // scalar type argument — Op + Value int32
	PtrStruct FieldDelta[*Inner] // pointer-to-struct type argument — Op + Value *Inner
}
`

// writeGenericFixture creates a temp module with the generic fixture and
// returns the module directory. The module is rooted at tmpDir.
func writeGenericFixture(t *testing.T) string {
	t.Helper()
	tmpDir := t.TempDir()

	modDir := filepath.Join(tmpDir, "model")
	if err := os.MkdirAll(modDir, 0755); err != nil {
		t.Fatalf("mkdir model: %v", err)
	}
	if err := os.WriteFile(filepath.Join(modDir, "model.go"), []byte(fixtureGenericCode), 0644); err != nil {
		t.Fatalf("write model.go: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module example.com/genericsfix\n\ngo 1.25.0\n"), 0644); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}
	return modDir
}

// TestParse_GenericField_BasicPresence verifies that after adding *ast.IndexExpr
// support, all three Snapshot fields are present: the plain Seq field and both
// generic-instantiation fields (Scalar, PtrStruct).
//
// Covers: R1 (parse generic instantiation fields), R2 (derived name assignment)
func TestParse_GenericField_BasicPresence(t *testing.T) {
	modDir := writeGenericFixture(t)

	_, _, structs, err := Parse([]string{modDir}, []string{"Snapshot"}, false)
	if err != nil {
		t.Fatalf("Parse() returned error: %v", err)
	}
	if len(structs) == 0 {
		t.Fatal("Parse() returned no structs; expected at least Snapshot")
	}

	// Find Snapshot in the results.
	var snapshot *StructInfo
	for i := range structs {
		if structs[i].Name == "Snapshot" {
			snapshot = &structs[i]
			break
		}
	}
	if snapshot == nil {
		t.Fatal("Snapshot not found in Parse() results")
	}

	want := []string{"Seq", "Scalar", "PtrStruct"}
	present := map[string]bool{}
	for _, f := range snapshot.Fields {
		present[f.Name] = true
	}
	for _, name := range want {
		if !present[name] {
			t.Errorf("expected field %q in Snapshot.Fields; not found (fields: %v)", name, snapshot.Fields)
		}
	}
}

// TestParse_GenericField_ScalarArg verifies that Parse() returns a FieldInfo for
// Snapshot.Scalar that is struct-shaped (IsStruct=true) with the derived name
// "FieldDelta_Int32", and that the FieldDelta_Int32 StructInfo is in the results
// with Op and Value sub-fields.
//
// Covers: R1 (parse scalar type-arg instantiation), R2 (derived name), R3 (sub-fields)
func TestParse_GenericField_ScalarArg(t *testing.T) {
	modDir := writeGenericFixture(t)

	_, _, structs, err := Parse([]string{modDir}, []string{"Snapshot"}, false)
	if err != nil {
		t.Fatalf("Parse() returned error: %v", err)
	}

	var snapshot *StructInfo
	for i := range structs {
		if structs[i].Name == "Snapshot" {
			snapshot = &structs[i]
			break
		}
	}
	if snapshot == nil {
		t.Fatal("Snapshot not found in Parse() results")
	}

	var scalar *FieldInfo
	for i := range snapshot.Fields {
		if snapshot.Fields[i].Name == "Scalar" {
			scalar = &snapshot.Fields[i]
			break
		}
	}
	if scalar == nil {
		t.Fatal("Scalar field not found in Snapshot.Fields")
	}
	if !scalar.IsStruct {
		t.Errorf("Scalar.IsStruct = false, want true (FieldDelta[int32] should resolve as a struct)")
	}
	if scalar.StructName != "FieldDelta_Int32" {
		t.Errorf("Scalar.StructName = %q, want %q", scalar.StructName, "FieldDelta_Int32")
	}

	// FieldDelta_Int32 must appear as a StructInfo in the results.
	var fd *StructInfo
	for i := range structs {
		if structs[i].Name == "FieldDelta_Int32" {
			fd = &structs[i]
			break
		}
	}
	if fd == nil {
		t.Fatal("FieldDelta_Int32 not found in Parse() results")
	}
	// Must have Op and Value sub-fields.
	subFields := map[string]bool{}
	for _, f := range fd.Fields {
		subFields[f.Name] = true
	}
	for _, want := range []string{"Op", "Value"} {
		if !subFields[want] {
			t.Errorf("FieldDelta_Int32 missing sub-field %q", want)
		}
	}
}

// TestParse_GenericField_PtrStructArg verifies that Parse() returns a FieldInfo for
// Snapshot.PtrStruct with derived name "FieldDelta_PtrInner", and that Inner is
// recursively queued and also appears in the results.
//
// Covers: R1 (parse pointer type-arg instantiation), R2 (derived name), R4 (recursive struct queue)
func TestParse_GenericField_PtrStructArg(t *testing.T) {
	modDir := writeGenericFixture(t)

	_, _, structs, err := Parse([]string{modDir}, []string{"Snapshot"}, false)
	if err != nil {
		t.Fatalf("Parse() returned error: %v", err)
	}

	var snapshot *StructInfo
	for i := range structs {
		if structs[i].Name == "Snapshot" {
			snapshot = &structs[i]
			break
		}
	}
	if snapshot == nil {
		t.Fatal("Snapshot not found in Parse() results")
	}

	var ptrStruct *FieldInfo
	for i := range snapshot.Fields {
		if snapshot.Fields[i].Name == "PtrStruct" {
			ptrStruct = &snapshot.Fields[i]
			break
		}
	}
	if ptrStruct == nil {
		t.Fatal("PtrStruct field not found in Snapshot.Fields")
	}
	if !ptrStruct.IsStruct {
		t.Errorf("PtrStruct.IsStruct = false, want true (FieldDelta[*Inner] should resolve as a struct)")
	}
	if ptrStruct.StructName != "FieldDelta_PtrInner" {
		t.Errorf("PtrStruct.StructName = %q, want %q", ptrStruct.StructName, "FieldDelta_PtrInner")
	}

	// Inner must have been queued and processed (via Value *Inner in FieldDelta_PtrInner).
	innerFound := false
	for _, si := range structs {
		if si.Name == "Inner" {
			innerFound = true
			break
		}
	}
	if !innerFound {
		t.Error("Inner struct not found in Parse() results; expected it to be queued via FieldDelta_PtrInner.Value")
	}
}

// TestDerivedInstanceName verifies that Parse() produces the expected derived
// identifier names for single-arg and pointer-arg generic instantiations.
//
// Covers: R2 (derivedInstanceName), R5 (pointer type arg naming)
func TestDerivedInstanceName(t *testing.T) {
	modDir := writeGenericFixture(t)

	_, _, structs, err := Parse([]string{modDir}, []string{"Snapshot"}, false)
	if err != nil {
		t.Fatalf("Parse() returned error: %v", err)
	}

	want := []string{"FieldDelta_Int32", "FieldDelta_PtrInner"}
	found := map[string]bool{}
	for _, si := range structs {
		found[si.Name] = true
	}
	for _, name := range want {
		if !found[name] {
			t.Errorf("expected StructInfo with Name=%q in Parse() results; not found (all names: %v)", name, structNames(structs))
		}
	}
}

// structNames returns the Name fields of all StructInfo entries for test diagnostics.
func structNames(structs []StructInfo) []string {
	names := make([]string, len(structs))
	for i, si := range structs {
		names[i] = si.Name
	}
	return names
}
