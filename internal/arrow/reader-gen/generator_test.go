package readergen

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.resystems.io/eddt/internal/arrow/gencommon"
)

func TestGenerator_Parse(t *testing.T) {
	tmpDir := t.TempDir()
	testCode := `package testpkg

type SimpleStruct struct {
	ID         int32
	Name       string
	Valid      bool
	Value      float64
	Tags       []string
	Scores     map[string]float64
	SingleByte byte
	ByteSlice  []byte
}
`
	if err := os.WriteFile(filepath.Join(tmpDir, "test_structs.go"), []byte(testCode), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module testpkg\n\ngo 1.25.0\n"), 0644); err != nil {
		t.Fatalf("Failed to write go.mod: %v", err)
	}

	g := NewGenerator([]string{tmpDir}, []string{"SimpleStruct"}, "out.go", false, nil)

	pkgName, pkgPath, structs, err := g.Parse()
	if err != nil {
		t.Fatalf("Parse() failed: %v", err)
	}

	if pkgName != "testpkg" {
		t.Errorf("Expected parsed package testpkg, got %s", pkgName)
	}
	if pkgPath != "testpkg" {
		t.Errorf("Expected parsed package path testpkg, got %s", pkgPath)
	}

	if len(structs) != 1 {
		t.Fatalf("Expected 1 struct, got %d", len(structs))
	}

	expected := gencommon.StructInfo{
		Name:    "SimpleStruct",
		PkgPath: "testpkg",
		PkgName: "testpkg",
		Fields: []gencommon.FieldInfo{
			{Name: "ID", GoType: "int32", ArrowType: "arrow.PrimitiveTypes.Int32", ArrowBuilder: "*array.Int32Builder", CastType: "int32", ArrowArrayType: "*array.Int32", ValueMethod: "Value", ZeroExpr: "0"},
			{Name: "Name", GoType: "string", ArrowType: "arrow.BinaryTypes.String", ArrowBuilder: "*array.StringBuilder", CastType: "string", ArrowArrayType: "*array.String", ValueMethod: "Value", ZeroExpr: `""`},
			{Name: "Valid", GoType: "bool", ArrowType: "arrow.FixedWidthTypes.Boolean", ArrowBuilder: "*array.BooleanBuilder", CastType: "bool", ArrowArrayType: "*array.Boolean", ValueMethod: "Value", ZeroExpr: "false"},
			{Name: "Value", GoType: "float64", ArrowType: "arrow.PrimitiveTypes.Float64", ArrowBuilder: "*array.Float64Builder", CastType: "float64", ArrowArrayType: "*array.Float64", ValueMethod: "Value", ZeroExpr: "0"},
			{
				Name:           "Tags",
				GoType:         "[]string",
				ArrowType:      "arrow.ListOf(arrow.BinaryTypes.String)",
				ArrowBuilder:   "*array.ListBuilder",
				IsList:         true,
				ArrowArrayType: "*array.List",
				ZeroExpr:       "nil",
				EltInfo: &gencommon.FieldInfo{
					GoType:         "string",
					ArrowType:      "arrow.BinaryTypes.String",
					ArrowBuilder:   "*array.StringBuilder",
					CastType:       "string",
					ArrowArrayType: "*array.String",
					ValueMethod:    "Value",
					ZeroExpr:       `""`,
				},
			},
			{
				Name:           "Scores",
				GoType:         "map[string]float64",
				ArrowType:      "arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Float64)",
				ArrowBuilder:   "*array.MapBuilder",
				IsMap:          true,
				ArrowArrayType: "*array.Map",
				ZeroExpr:       "nil",
				KeyInfo: &gencommon.FieldInfo{
					GoType:         "string",
					ArrowType:      "arrow.BinaryTypes.String",
					ArrowBuilder:   "*array.StringBuilder",
					CastType:       "string",
					ArrowArrayType: "*array.String",
					ValueMethod:    "Value",
					ZeroExpr:       `""`,
				},
				EltInfo: &gencommon.FieldInfo{
					GoType:         "float64",
					ArrowType:      "arrow.PrimitiveTypes.Float64",
					ArrowBuilder:   "*array.Float64Builder",
					CastType:       "float64",
					ArrowArrayType: "*array.Float64",
					ValueMethod:    "Value",
					ZeroExpr:       "0",
				},
			},
			{Name: "SingleByte", GoType: "byte", ArrowType: "arrow.PrimitiveTypes.Uint8", ArrowBuilder: "*array.Uint8Builder", CastType: "uint8", ArrowArrayType: "*array.Uint8", ValueMethod: "Value", ZeroExpr: "0"},
			{Name: "ByteSlice", GoType: "[]byte", ArrowType: "arrow.BinaryTypes.Binary", ArrowBuilder: "*array.BinaryBuilder", CastType: "[]byte", ArrowArrayType: "*array.Binary", ValueMethod: "Value", ZeroExpr: "nil"},
		},
	}

	if diff := cmp.Diff([]gencommon.StructInfo{expected}, structs); diff != "" {
		t.Errorf("Parse() struct mismatch (-want +got):\n%s", diff)
	}
}

// TestGenerator_RunReservedNames verifies that reader-gen uses "arrow" and "array"
// (but not "memory") as reserved names, unlike writer-gen which also reserves "memory".
func TestGenerator_RunReservedNames(t *testing.T) {
	tmpDir := t.TempDir()
	testCode := `package mypkg

type Person struct {
	Name string
}
`
	if err := os.WriteFile(filepath.Join(tmpDir, "test.go"), []byte(testCode), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module mypkg\n\ngo 1.25.0\n"), 0644); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}

	outPath := filepath.Join(tmpDir, "out.go")

	// "arrow" and "array" should be reserved.
	for _, reserved := range []string{"arrow", "array"} {
		t.Run("reserved-"+reserved, func(t *testing.T) {
			g := NewGenerator([]string{tmpDir}, []string{"Person"}, outPath, false, nil)
			err := g.Run(reserved)
			if err == nil {
				t.Fatalf("Expected error for reserved package name %q, got nil", reserved)
			}
		})
	}

	// "memory" should NOT be reserved by reader-gen.
	t.Run("memory-not-reserved", func(t *testing.T) {
		g := NewGenerator([]string{tmpDir}, []string{"Person"}, outPath, false, nil)
		err := g.Run("memory")
		if err != nil {
			t.Fatalf("reader-gen should not reserve 'memory', got error: %v", err)
		}
	})
}
