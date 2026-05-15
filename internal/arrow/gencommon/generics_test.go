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

// TestParse_GenericField_CurrentBehavior proves the current gap: fields whose
// types are generic instantiations (e.g. FieldDelta[int32]) are not represented
// in fieldInfoFromExpr, so they are silently skipped and absent from the
// returned FieldInfo slice.
//
// This test passes with the current gencommon implementation and acts as a
// regression guard — if the behaviour changes (either the fix lands or a new
// code path introduces a panic) this test will catch it.
func TestParse_GenericField_CurrentBehavior(t *testing.T) {
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

	// Verify the non-generic field (Seq) is present.
	seqFound := false
	for _, f := range snapshot.Fields {
		if f.Name == "Seq" {
			seqFound = true
		}
	}
	if !seqFound {
		t.Error("expected Seq field in Snapshot.Fields; not found")
	}

	// Generic-instantiation fields must be absent (skipped via warning).
	for _, f := range snapshot.Fields {
		if f.Name == "Scalar" || f.Name == "PtrStruct" {
			t.Errorf("field %q should not be present in Snapshot.Fields under current gencommon (no *ast.IndexExpr support); got %+v", f.Name, f)
		}
	}
}

// TestParse_GenericField_ScalarArg is the acceptance criterion for gencommon
// generic-instantiation support with a scalar type argument.
//
// After the fix: Parse() must return a FieldInfo for Snapshot.Scalar that is
// a struct-shaped field (IsStruct=true) with two inlined sub-fields — Op
// (int8-compatible) and Value (int32).
//
// Skipped until gencommon adds *ast.IndexExpr / *ast.IndexListExpr handling
// and fieldInfoFromType checks named.TypeArgs().
func TestParse_GenericField_ScalarArg(t *testing.T) {
	t.Skip("pending: gencommon does not yet handle *ast.IndexExpr (generic instantiations)")

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
		t.Fatal("Scalar field not found in Snapshot.Fields after fix")
	}
	if !scalar.IsStruct {
		t.Errorf("Scalar.IsStruct = false, want true (FieldDelta[int32] should resolve as a struct)")
	}
}

// TestParse_GenericField_PtrStructArg is the acceptance criterion for
// gencommon generic-instantiation support with a pointer-to-struct type
// argument.
//
// After the fix: Snapshot.PtrStruct must be a struct-shaped FieldInfo whose
// Value sub-field is itself a struct (IsStruct=true, StructName="Inner"), and
// Inner must also appear in the returned structs slice (queued for recursive
// processing).
//
// Skipped until gencommon adds *ast.IndexExpr / *ast.IndexListExpr handling.
func TestParse_GenericField_PtrStructArg(t *testing.T) {
	t.Skip("pending: gencommon does not yet handle *ast.IndexExpr (generic instantiations)")

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
		t.Fatal("PtrStruct field not found in Snapshot.Fields after fix")
	}
	if !ptrStruct.IsStruct {
		t.Errorf("PtrStruct.IsStruct = false, want true (FieldDelta[*Inner] should resolve as a struct)")
	}

	// Inner must have been queued and processed.
	innerFound := false
	for _, si := range structs {
		if si.Name == "Inner" {
			innerFound = true
			break
		}
	}
	if !innerFound {
		t.Error("Inner struct not found in Parse() results; expected it to be queued via PtrStruct's Value field")
	}
}
