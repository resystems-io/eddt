package writergen

import (
	"go/ast"
	"os"
	"path/filepath"
	"strings"
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

	if diff := cmp.Diff([]StructInfo{expected}, structs); diff != "" {
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

func TestTemplateOutput(t *testing.T) {
	tmpDir := t.TempDir()
	testFilePath := filepath.Join(tmpDir, "test_structs.go")
	testCode := `package mypkg

type Person struct {
	Name string
}
`
	if err := os.WriteFile(testFilePath, []byte(testCode), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	modContent := "module mypkg\n\ngo 1.25.0\n"
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte(modContent), 0644); err != nil {
		t.Fatalf("Failed to write go.mod: %v", err)
	}

	outPath := filepath.Join(tmpDir, "out_writer.go")
	g := NewGenerator(tmpDir, []string{"Person"}, outPath, false)

	err := g.Run("")
	if err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	outBytes, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	outStr := string(outBytes)
	if !strings.Contains(outStr, "package mypkg") {
		t.Errorf("Expected output to contain 'package mypkg'")
	}
	if !strings.Contains(outStr, "func NewPersonSchema() *arrow.Schema") {
		t.Errorf("Expected output to contain 'func NewPersonSchema() *arrow.Schema'")
	}
	if !strings.Contains(outStr, "type PersonArrowWriter struct") {
		t.Errorf("Expected output to contain 'type PersonArrowWriter struct'")
	}
}

func TestTemplateOutputOverridePkg(t *testing.T) {
	tmpDir := t.TempDir()
	testFilePath := filepath.Join(tmpDir, "test_structs.go")
	testCode := `package mypkg

type Person struct {
	Name string
}
`
	if err := os.WriteFile(testFilePath, []byte(testCode), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	modContent := "module mypkg\n\ngo 1.25.0\n"
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte(modContent), 0644); err != nil {
		t.Fatalf("Failed to write go.mod: %v", err)
	}

	outPath := filepath.Join(tmpDir, "out_writer.go")
	g := NewGenerator(tmpDir, []string{"Person"}, outPath, false)

	err := g.Run("differentpkg")
	if err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	outBytes, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	outStr := string(outBytes)
	if !strings.Contains(outStr, "package differentpkg") {
		t.Errorf("Expected output to contain 'package differentpkg', got:\n%s", outStr)
	}
	if !strings.Contains(outStr, "\"mypkg\"") {
		t.Errorf("Expected output to import 'mypkg', got:\n%s", outStr)
	}
	if !strings.Contains(outStr, "func (w *PersonArrowWriter) Append(row mypkg.Person)") {
		t.Errorf("Expected output to use imported struct mypkg.Person, got:\n%s", outStr)
	}
	if strings.Contains(outStr, "row Person)") {
		t.Errorf("Expected no unqualified 'row Person)' in method signatures, got:\n%s", outStr)
	}
}

func TestTemplateOutputPkgNameCollision(t *testing.T) {
	tmpDir := t.TempDir()
	testFilePath := filepath.Join(tmpDir, "test_structs.go")
	testCode := `package mypkg

type Person struct {
	Name string
}
`
	if err := os.WriteFile(testFilePath, []byte(testCode), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	modContent := "module mypkg\n\ngo 1.25.0\n"
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte(modContent), 0644); err != nil {
		t.Fatalf("Failed to write go.mod: %v", err)
	}

	outPath := filepath.Join(tmpDir, "out_writer.go")
	g := NewGenerator(tmpDir, []string{"Person"}, outPath, false)

	for _, reserved := range []string{"arrow", "array", "memory"} {
		t.Run(reserved, func(t *testing.T) {
			err := g.Run(reserved)
			if err == nil {
				t.Fatalf("Expected error for reserved package name %q, got nil", reserved)
			}
			if !strings.Contains(err.Error(), "collides with an import") {
				t.Errorf("Expected collision error, got: %v", err)
			}
		})
	}
}
