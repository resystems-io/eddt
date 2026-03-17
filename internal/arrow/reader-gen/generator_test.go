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

func TestGenerator_ParsePointerPrimitives(t *testing.T) {
	tmpDir := t.TempDir()
	testCode := `package testpkg

type MyID int32

type PtrStruct struct {
	OptInt    *int32
	OptFloat  *float64
	OptBool   *bool
	OptName   *string
	OptID     *MyID
}
`
	if err := os.WriteFile(filepath.Join(tmpDir, "test_structs.go"), []byte(testCode), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module testpkg\n\ngo 1.25.0\n"), 0644); err != nil {
		t.Fatalf("Failed to write go.mod: %v", err)
	}

	g := NewGenerator([]string{tmpDir}, []string{"PtrStruct"}, "out.go", false, nil)

	_, _, structs, err := g.Parse()
	if err != nil {
		t.Fatalf("Parse() failed: %v", err)
	}

	if len(structs) != 1 {
		t.Fatalf("Expected 1 struct, got %d", len(structs))
	}

	expected := gencommon.StructInfo{
		Name:    "PtrStruct",
		PkgPath: "testpkg",
		PkgName: "testpkg",
		Fields: []gencommon.FieldInfo{
			{Name: "OptInt", GoType: "*int32", IsPointer: true, ArrowType: "arrow.PrimitiveTypes.Int32", ArrowBuilder: "*array.Int32Builder", CastType: "int32", ArrowArrayType: "*array.Int32", ValueMethod: "Value", ZeroExpr: "0"},
			{Name: "OptFloat", GoType: "*float64", IsPointer: true, ArrowType: "arrow.PrimitiveTypes.Float64", ArrowBuilder: "*array.Float64Builder", CastType: "float64", ArrowArrayType: "*array.Float64", ValueMethod: "Value", ZeroExpr: "0"},
			{Name: "OptBool", GoType: "*bool", IsPointer: true, ArrowType: "arrow.FixedWidthTypes.Boolean", ArrowBuilder: "*array.BooleanBuilder", CastType: "bool", ArrowArrayType: "*array.Boolean", ValueMethod: "Value", ZeroExpr: "false"},
			{Name: "OptName", GoType: "*string", IsPointer: true, ArrowType: "arrow.BinaryTypes.String", ArrowBuilder: "*array.StringBuilder", CastType: "string", ArrowArrayType: "*array.String", ValueMethod: "Value", ZeroExpr: `""`},
			{Name: "OptID", GoType: "*MyID", IsPointer: true, ArrowType: "arrow.PrimitiveTypes.Int32", ArrowBuilder: "*array.Int32Builder", CastType: "int32", ArrowArrayType: "*array.Int32", ValueMethod: "Value", ZeroExpr: "0", TypePkgPath: "testpkg"},
		},
	}

	if diff := cmp.Diff([]gencommon.StructInfo{expected}, structs); diff != "" {
		t.Errorf("Parse() struct mismatch (-want +got):\n%s", diff)
	}
}

func TestGenerator_ParseListFields(t *testing.T) {
	tmpDir := t.TempDir()
	testCode := `package testpkg

type ListStruct struct {
	Tags  []string
	Grid  [][]int32
	Bytes [][]byte
}
`
	if err := os.WriteFile(filepath.Join(tmpDir, "test_structs.go"), []byte(testCode), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module testpkg\n\ngo 1.25.0\n"), 0644); err != nil {
		t.Fatalf("Failed to write go.mod: %v", err)
	}

	g := NewGenerator([]string{tmpDir}, []string{"ListStruct"}, "out.go", false, nil)

	_, _, structs, err := g.Parse()
	if err != nil {
		t.Fatalf("Parse() failed: %v", err)
	}

	if len(structs) != 1 {
		t.Fatalf("Expected 1 struct, got %d", len(structs))
	}

	expected := gencommon.StructInfo{
		Name:    "ListStruct",
		PkgPath: "testpkg",
		PkgName: "testpkg",
		Fields: []gencommon.FieldInfo{
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
				Name:           "Grid",
				GoType:         "[][]int32",
				ArrowType:      "arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int32))",
				ArrowBuilder:   "*array.ListBuilder",
				IsList:         true,
				ArrowArrayType: "*array.List",
				ZeroExpr:       "nil",
				EltInfo: &gencommon.FieldInfo{
					GoType:         "[]int32",
					ArrowType:      "arrow.ListOf(arrow.PrimitiveTypes.Int32)",
					ArrowBuilder:   "*array.ListBuilder",
					IsList:         true,
					ArrowArrayType: "*array.List",
					ZeroExpr:       "nil",
					EltInfo: &gencommon.FieldInfo{
						GoType:         "int32",
						ArrowType:      "arrow.PrimitiveTypes.Int32",
						ArrowBuilder:   "*array.Int32Builder",
						CastType:       "int32",
						ArrowArrayType: "*array.Int32",
						ValueMethod:    "Value",
						ZeroExpr:       "0",
					},
				},
			},
			{
				Name:           "Bytes",
				GoType:         "[][]byte",
				ArrowType:      "arrow.ListOf(arrow.BinaryTypes.Binary)",
				ArrowBuilder:   "*array.ListBuilder",
				IsList:         true,
				ArrowArrayType: "*array.List",
				ZeroExpr:       "nil",
				EltInfo: &gencommon.FieldInfo{
					GoType:         "[]byte",
					ArrowType:      "arrow.BinaryTypes.Binary",
					ArrowBuilder:   "*array.BinaryBuilder",
					CastType:       "[]byte",
					ArrowArrayType: "*array.Binary",
					ValueMethod:    "Value",
					ZeroExpr:       "nil",
				},
			},
		},
	}

	if diff := cmp.Diff([]gencommon.StructInfo{expected}, structs); diff != "" {
		t.Errorf("Parse() struct mismatch (-want +got):\n%s", diff)
	}
}

func TestGenerator_ParseFixedSizeListFields(t *testing.T) {
	tmpDir := t.TempDir()
	testCode := `package testpkg

type FixedStruct struct {
	Header [4]byte
	Scores [3]int32
	Matrix [3][2]int32
}
`
	if err := os.WriteFile(filepath.Join(tmpDir, "test_structs.go"), []byte(testCode), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module testpkg\n\ngo 1.25.0\n"), 0644); err != nil {
		t.Fatalf("Failed to write go.mod: %v", err)
	}

	g := NewGenerator([]string{tmpDir}, []string{"FixedStruct"}, "out.go", false, nil)

	_, _, structs, err := g.Parse()
	if err != nil {
		t.Fatalf("Parse() failed: %v", err)
	}

	if len(structs) != 1 {
		t.Fatalf("Expected 1 struct, got %d", len(structs))
	}

	expected := gencommon.StructInfo{
		Name:    "FixedStruct",
		PkgPath: "testpkg",
		PkgName: "testpkg",
		Fields: []gencommon.FieldInfo{
			{
				Name:            "Header",
				GoType:          "[4]byte",
				ArrowType:       "arrow.FixedSizeListOfNonNullable(4, arrow.PrimitiveTypes.Uint8)",
				ArrowBuilder:    "*array.FixedSizeListBuilder",
				IsFixedSizeList: true,
				FixedSizeLen:    "4",
				ArrowArrayType:  "*array.FixedSizeList",
				ZeroExpr:        "[4]byte{}",
				EltInfo: &gencommon.FieldInfo{
					GoType:         "byte",
					ArrowType:      "arrow.PrimitiveTypes.Uint8",
					ArrowBuilder:   "*array.Uint8Builder",
					CastType:       "uint8",
					ArrowArrayType: "*array.Uint8",
					ValueMethod:    "Value",
					ZeroExpr:       "0",
				},
			},
			{
				Name:            "Scores",
				GoType:          "[3]int32",
				ArrowType:       "arrow.FixedSizeListOfNonNullable(3, arrow.PrimitiveTypes.Int32)",
				ArrowBuilder:    "*array.FixedSizeListBuilder",
				IsFixedSizeList: true,
				FixedSizeLen:    "3",
				ArrowArrayType:  "*array.FixedSizeList",
				ZeroExpr:        "[3]int32{}",
				EltInfo: &gencommon.FieldInfo{
					GoType:         "int32",
					ArrowType:      "arrow.PrimitiveTypes.Int32",
					ArrowBuilder:   "*array.Int32Builder",
					CastType:       "int32",
					ArrowArrayType: "*array.Int32",
					ValueMethod:    "Value",
					ZeroExpr:       "0",
				},
			},
			{
				Name:            "Matrix",
				GoType:          "[3][2]int32",
				ArrowType:       "arrow.FixedSizeListOfNonNullable(3, arrow.FixedSizeListOfNonNullable(2, arrow.PrimitiveTypes.Int32))",
				ArrowBuilder:    "*array.FixedSizeListBuilder",
				IsFixedSizeList: true,
				FixedSizeLen:    "3",
				ArrowArrayType:  "*array.FixedSizeList",
				ZeroExpr:        "[3][2]int32{}",
				EltInfo: &gencommon.FieldInfo{
					GoType:          "[2]int32",
					ArrowType:       "arrow.FixedSizeListOfNonNullable(2, arrow.PrimitiveTypes.Int32)",
					ArrowBuilder:    "*array.FixedSizeListBuilder",
					IsFixedSizeList: true,
					FixedSizeLen:    "2",
					ArrowArrayType:  "*array.FixedSizeList",
					ZeroExpr:        "[2]int32{}",
					EltInfo: &gencommon.FieldInfo{
						GoType:         "int32",
						ArrowType:      "arrow.PrimitiveTypes.Int32",
						ArrowBuilder:   "*array.Int32Builder",
						CastType:       "int32",
						ArrowArrayType: "*array.Int32",
						ValueMethod:    "Value",
						ZeroExpr:       "0",
					},
				},
			},
		},
	}

	if diff := cmp.Diff([]gencommon.StructInfo{expected}, structs); diff != "" {
		t.Errorf("Parse() struct mismatch (-want +got):\n%s", diff)
	}
}

func TestGenerator_ParseMapFields(t *testing.T) {
	tmpDir := t.TempDir()
	testCode := `package testpkg

type MapStruct struct {
	Scores map[string]float64
	IntMap  map[int32]string
	Nested  map[string]map[string]int32
	ListVal map[string][]int32
}
`
	if err := os.WriteFile(filepath.Join(tmpDir, "test_structs.go"), []byte(testCode), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module testpkg\n\ngo 1.25.0\n"), 0644); err != nil {
		t.Fatalf("Failed to write go.mod: %v", err)
	}

	g := NewGenerator([]string{tmpDir}, []string{"MapStruct"}, "out.go", false, nil)

	_, _, structs, err := g.Parse()
	if err != nil {
		t.Fatalf("Parse() failed: %v", err)
	}

	if len(structs) != 1 {
		t.Fatalf("Expected 1 struct, got %d", len(structs))
	}

	expected := gencommon.StructInfo{
		Name:    "MapStruct",
		PkgPath: "testpkg",
		PkgName: "testpkg",
		Fields: []gencommon.FieldInfo{
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
			{
				Name:           "IntMap",
				GoType:         "map[int32]string",
				ArrowType:      "arrow.MapOf(arrow.PrimitiveTypes.Int32, arrow.BinaryTypes.String)",
				ArrowBuilder:   "*array.MapBuilder",
				IsMap:          true,
				ArrowArrayType: "*array.Map",
				ZeroExpr:       "nil",
				KeyInfo: &gencommon.FieldInfo{
					GoType:         "int32",
					ArrowType:      "arrow.PrimitiveTypes.Int32",
					ArrowBuilder:   "*array.Int32Builder",
					CastType:       "int32",
					ArrowArrayType: "*array.Int32",
					ValueMethod:    "Value",
					ZeroExpr:       "0",
				},
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
				Name:           "Nested",
				GoType:         "map[string]map[string]int32",
				ArrowType:      "arrow.MapOf(arrow.BinaryTypes.String, arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32))",
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
					GoType:         "map[string]int32",
					ArrowType:      "arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32)",
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
						GoType:         "int32",
						ArrowType:      "arrow.PrimitiveTypes.Int32",
						ArrowBuilder:   "*array.Int32Builder",
						CastType:       "int32",
						ArrowArrayType: "*array.Int32",
						ValueMethod:    "Value",
						ZeroExpr:       "0",
					},
				},
			},
			{
				Name:           "ListVal",
				GoType:         "map[string][]int32",
				ArrowType:      "arrow.MapOf(arrow.BinaryTypes.String, arrow.ListOf(arrow.PrimitiveTypes.Int32))",
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
					GoType:         "[]int32",
					ArrowType:      "arrow.ListOf(arrow.PrimitiveTypes.Int32)",
					ArrowBuilder:   "*array.ListBuilder",
					IsList:         true,
					ArrowArrayType: "*array.List",
					ZeroExpr:       "nil",
					EltInfo: &gencommon.FieldInfo{
						GoType:         "int32",
						ArrowType:      "arrow.PrimitiveTypes.Int32",
						ArrowBuilder:   "*array.Int32Builder",
						CastType:       "int32",
						ArrowArrayType: "*array.Int32",
						ValueMethod:    "Value",
						ZeroExpr:       "0",
					},
				},
			},
		},
	}

	if diff := cmp.Diff([]gencommon.StructInfo{expected}, structs); diff != "" {
		t.Errorf("Parse() struct mismatch (-want +got):\n%s", diff)
	}
}

func TestGenerator_ParseStructFields(t *testing.T) {
	tmpDir := t.TempDir()
	testCode := `package testpkg

type Inner struct {
	Value int32
	Label string
}

type Outer struct {
	ID     int32
	Child  Inner
	PChild *Inner
}
`
	if err := os.WriteFile(filepath.Join(tmpDir, "test_structs.go"), []byte(testCode), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module testpkg\n\ngo 1.25.0\n"), 0644); err != nil {
		t.Fatalf("Failed to write go.mod: %v", err)
	}

	g := NewGenerator([]string{tmpDir}, []string{"Outer"}, "out.go", false, nil)

	_, _, structs, err := g.Parse()
	if err != nil {
		t.Fatalf("Parse() failed: %v", err)
	}

	// Parse should discover both Outer and Inner (via queue)
	if len(structs) != 2 {
		t.Fatalf("Expected 2 structs, got %d", len(structs))
	}

	// Find Outer struct
	var outer gencommon.StructInfo
	var inner gencommon.StructInfo
	for _, s := range structs {
		switch s.Name {
		case "Outer":
			outer = s
		case "Inner":
			inner = s
		}
	}

	if outer.Name != "Outer" {
		t.Fatal("Outer struct not found")
	}
	if inner.Name != "Inner" {
		t.Fatal("Inner struct not found")
	}

	// Verify Outer fields
	if len(outer.Fields) != 3 {
		t.Fatalf("Expected 3 fields in Outer, got %d", len(outer.Fields))
	}

	// Field: ID int32
	if outer.Fields[0].Name != "ID" || outer.Fields[0].GoType != "int32" {
		t.Errorf("Field 0: got %s %s, want ID int32", outer.Fields[0].Name, outer.Fields[0].GoType)
	}

	// Field: Child Inner (value struct)
	child := outer.Fields[1]
	if child.Name != "Child" {
		t.Errorf("Field 1 Name: got %s, want Child", child.Name)
	}
	if !child.IsStruct {
		t.Error("Field 1: expected IsStruct=true")
	}
	if child.IsPointer {
		t.Error("Field 1: expected IsPointer=false")
	}
	if child.StructName != "Inner" {
		t.Errorf("Field 1 StructName: got %s, want Inner", child.StructName)
	}
	if child.ArrowArrayType != "*array.Struct" {
		t.Errorf("Field 1 ArrowArrayType: got %s, want *array.Struct", child.ArrowArrayType)
	}

	// Field: PChild *Inner (pointer-to-struct)
	pchild := outer.Fields[2]
	if pchild.Name != "PChild" {
		t.Errorf("Field 2 Name: got %s, want PChild", pchild.Name)
	}
	if !pchild.IsStruct {
		t.Error("Field 2: expected IsStruct=true")
	}
	if !pchild.IsPointer {
		t.Error("Field 2: expected IsPointer=true")
	}
	if pchild.StructName != "Inner" {
		t.Errorf("Field 2 StructName: got %s, want Inner", pchild.StructName)
	}
	if pchild.ArrowArrayType != "*array.Struct" {
		t.Errorf("Field 2 ArrowArrayType: got %s, want *array.Struct", pchild.ArrowArrayType)
	}

	// Verify Inner has expected fields
	if len(inner.Fields) != 2 {
		t.Fatalf("Expected 2 fields in Inner, got %d", len(inner.Fields))
	}
	if inner.Fields[0].Name != "Value" || inner.Fields[0].GoType != "int32" {
		t.Errorf("Inner Field 0: got %s %s, want Value int32", inner.Fields[0].Name, inner.Fields[0].GoType)
	}
	if inner.Fields[1].Name != "Label" || inner.Fields[1].GoType != "string" {
		t.Errorf("Inner Field 1: got %s %s, want Label string", inner.Fields[1].Name, inner.Fields[1].GoType)
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
