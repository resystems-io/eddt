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

// fixtureNestedGenericCode instantiates generics with other instantiations,
// with a slice, with a type from another package, and through a named slice,
// the shapes that reach the generator through the type checker rather than
// the syntax tree.
const fixtureNestedGenericCode = `package model

import "net/netip"

type Pair[K, V any] struct {
	Key   K
	Value V
}

type List[T any] struct {
	Items []T
}

type Item struct {
	N int64
}

type PairList []Pair[string, int64]

type Record struct {
	Nested Pair[string, List[Item]]
	A      Pair[string, Pair[int32, int32]]
	B      Pair[string, Pair[string, string]]
	S      Pair[[]int32, int32]
	Named  PairList
	Ext    Pair[netip.Addr, int32]
}
`

// writeGenericFixture creates a temp module with the generic fixture and
// returns the module directory. The module is rooted at tmpDir.
func writeGenericFixture(t *testing.T) string {
	t.Helper()
	return writeModelFixture(t, fixtureGenericCode)
}

// writeModelFixture creates a temp module holding code as package model and
// returns the package directory.
func writeModelFixture(t *testing.T, code string) string {
	t.Helper()
	tmpDir := t.TempDir()

	modDir := filepath.Join(tmpDir, "model")
	if err := os.MkdirAll(modDir, 0755); err != nil {
		t.Fatalf("mkdir model: %v", err)
	}
	if err := os.WriteFile(filepath.Join(modDir, "model.go"), []byte(code), 0644); err != nil {
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

// TestParse_GenericField_NestedAndCompositeArgs verifies the instantiations
// that reach the generator through the type checker: a generic nested inside
// a generic keeps the inner fields, two instantiations that differ only in
// their inner arguments get distinct names, a slice type argument yields a
// valid identifier, a named slice of an instantiation resolves its element,
// and a type argument from another package gets that package imported.
func TestParse_GenericField_NestedAndCompositeArgs(t *testing.T) {
	modDir := writeModelFixture(t, fixtureNestedGenericCode)

	pkgName, _, structs, err := Parse([]string{modDir}, []string{"Record"}, false)
	if err != nil {
		t.Fatalf("Parse() returned error: %v", err)
	}
	byName := map[string]*StructInfo{}
	for i := range structs {
		byName[structs[i].Name] = &structs[i]
	}
	record := byName["Record"]
	if record == nil {
		t.Fatal("Record not found in Parse() results")
	}
	fields := map[string]FieldInfo{}
	for _, f := range record.Fields {
		fields[f.Name] = f
	}

	wantStruct := map[string]string{
		"Nested": "Pair_String_List_Item",
		"A":      "Pair_String_Pair_Int32_Int32",
		"B":      "Pair_String_Pair_String_String",
		"S":      "Pair_SliceOfInt32_Int32",
		"Ext":    "Pair_Addr_Int32",
	}
	for field, want := range wantStruct {
		f, ok := fields[field]
		if !ok {
			t.Errorf("Record.%s missing from the parsed fields", field)
			continue
		}
		if !f.IsStruct || f.StructName != want {
			t.Errorf("Record.%s: StructName = %q (IsStruct %v), want %q", field, f.StructName, f.IsStruct, want)
		}
		if byName[want] == nil {
			t.Errorf("no StructInfo named %q in the results", want)
		}
	}

	if named, ok := fields["Named"]; !ok || !named.IsList || named.EltInfo == nil || !named.EltInfo.IsStruct || named.EltInfo.StructName != "Pair_String_Int64" {
		t.Errorf("Record.Named = %+v, want a list of Pair_String_Int64", fields["Named"])
	}
	if byName["Pair_String_Int64"] == nil {
		t.Error("no StructInfo named Pair_String_Int64 in the results")
	}

	if inner := byName["List_Item"]; inner == nil || len(inner.Fields) != 1 || !inner.Fields[0].IsList || inner.Fields[0].EltInfo.StructName != "Item" {
		t.Errorf("List_Item = %+v, want one field Items holding a list of Item", inner)
	}
	if inner := byName["Pair_String_List_Item"]; inner == nil || len(inner.Fields) != 2 || inner.Fields[1].StructName != "List_Item" {
		t.Errorf("Pair_String_List_Item = %+v, want Key and a Value of List_Item", inner)
	}
	if s := byName["Pair_SliceOfInt32_Int32"]; s == nil || len(s.Fields) != 2 || !s.Fields[0].IsList {
		t.Errorf("Pair_SliceOfInt32_Int32 = %+v, want a list Key and a Value", s)
	}
	if ext := byName["Pair_Addr_Int32"]; ext == nil || len(ext.Fields) != 2 || ext.Fields[0].MarshalMethod != "MarshalText" {
		t.Errorf("Pair_Addr_Int32 = %+v, want a Key resolved through MarshalText", ext)
	}

	_, imports, err := ResolveOutputContext(pkgName, structs, "", nil, map[string]bool{})
	if err != nil {
		t.Fatalf("ResolveOutputContext: %v", err)
	}
	var netipImported bool
	for _, imp := range imports {
		if imp.Path == "net/netip" {
			netipImported = true
		}
	}
	if !netipImported {
		t.Errorf("imports = %v, want net/netip for the type argument of Ext", imports)
	}
	if got := byName["Pair_Addr_Int32"].GoTypeExpr; got != "Pair[netip.Addr, int32]" {
		t.Errorf("Pair_Addr_Int32.GoTypeExpr = %q, want the qualified instantiation", got)
	}
	if got := byName["Pair_String_List_Item"].GoTypeExpr; got != "Pair[string, List[Item]]" {
		t.Errorf("Pair_String_List_Item.GoTypeExpr = %q", got)
	}
}
