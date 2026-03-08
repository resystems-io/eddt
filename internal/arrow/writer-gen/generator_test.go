package writergen

import (
	"go/ast"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestGenerator_Parse(t *testing.T) {
	// Create a temporary directory with a test file
	tmpDir := t.TempDir()
	testFilePath := filepath.Join(tmpDir, "test_structs.go")
	testCode := `package testpkg

type SimpleStruct struct {
	ID    int32
	Name  string
	Valid bool
	Value  float64
	Tags   []string
	Scores map[string]float64
}

type IgnoredStruct struct {
	Data []byte
}
`
	if err := os.WriteFile(testFilePath, []byte(testCode), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	modContent := "module testpkg\n\ngo 1.25.0\n"
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte(modContent), 0644); err != nil {
		t.Fatalf("Failed to write go.mod: %v", err)
	}

	g := NewGenerator(tmpDir, []string{"SimpleStruct"}, "out.go", false)

	structs, err := g.Parse()
	if err != nil {
		t.Fatalf("Parse() failed: %v", err)
	}

	if len(structs) != 1 {
		t.Fatalf("Expected 1 struct, got %d", len(structs))
	}

	expected := StructInfo{
		Name: "SimpleStruct",
		Fields: []FieldInfo{
			{Name: "ID", GoType: "int32", ArrowType: "arrow.PrimitiveTypes.Int32", ArrowBuilder: "*array.Int32Builder", CastType: "int32"},
			{Name: "Name", GoType: "string", ArrowType: "arrow.BinaryTypes.String", ArrowBuilder: "*array.StringBuilder", CastType: "string"},
			{Name: "Valid", GoType: "bool", ArrowType: "arrow.FixedWidthTypes.Boolean", ArrowBuilder: "*array.BooleanBuilder", CastType: "bool"},
			{Name: "Value", GoType: "float64", ArrowType: "arrow.PrimitiveTypes.Float64", ArrowBuilder: "*array.Float64Builder", CastType: "float64"},
			{
				Name:            "Tags",
				GoType:          "[]string",
				ArrowType:       "arrow.ListOf(arrow.BinaryTypes.String)",
				ArrowBuilder:    "*array.ListBuilder",
				IsList:          true,
				ValArrowBuilder: "*array.StringBuilder",
				ValCastType:     "string",
			},
			{
				Name:            "Scores",
				GoType:          "map[string]float64",
				ArrowType:       "arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Float64)",
				ArrowBuilder:    "*array.MapBuilder",
				IsMap:           true,
				KeyArrowBuilder: "*array.StringBuilder",
				ValArrowBuilder: "*array.Float64Builder",
				KeyCastType:     "string",
				ValCastType:     "float64",
			},
		},
	}

	if diff := cmp.Diff(expected, structs[0]); diff != "" {
		t.Errorf("Parse() struct mismatch (-want +got):\n%s", diff)
	}
}

func TestMapToArrowType(t *testing.T) {
	tests := []struct {
		goType      string
		expectedGo  string
		expectedArr string
		expectedBld string
		expectErr   bool
	}{
		{"int32", "int32", "arrow.PrimitiveTypes.Int32", "*array.Int32Builder", false},
		{"string", "string", "arrow.BinaryTypes.String", "*array.StringBuilder", false},
		{"bool", "bool", "arrow.FixedWidthTypes.Boolean", "*array.BooleanBuilder", false},
		{"uint64", "uint64", "arrow.PrimitiveTypes.Uint64", "*array.Uint64Builder", false},
		{"float64", "float64", "arrow.PrimitiveTypes.Float64", "*array.Float64Builder", false},
		{"unknown", "", "", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.goType, func(t *testing.T) {
			ident := &ast.Ident{Name: tt.goType}
			gotGo, gotArr, gotBld, _, err := mapToArrowType(ident)
			if (err != nil) != tt.expectErr {
				t.Errorf("mapToArrowType(%s) error = %v, expectErr %v", tt.goType, err, tt.expectErr)
				return
			}
			if err == nil {
				if gotGo != tt.expectedGo {
					t.Errorf("mapToArrowType(%s) gotGo = %v, want %v", tt.goType, gotGo, tt.expectedGo)
				}
				if gotArr != tt.expectedArr {
					t.Errorf("mapToArrowType(%s) gotArr = %v, want %v", tt.goType, gotArr, tt.expectedArr)
				}
				if gotBld != tt.expectedBld {
					t.Errorf("mapToArrowType(%s) gotBld = %v, want %v", tt.goType, gotBld, tt.expectedBld)
				}
			}
		})
	}
}
