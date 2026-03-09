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

	g := NewGenerator(tmpDir, []string{"SimpleStruct"}, "out.go", false, "")

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
	g := NewGenerator(tmpDir, []string{"Person"}, outPath, false, "")

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
	g := NewGenerator(tmpDir, []string{"Person"}, outPath, false, "")

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
	g := NewGenerator(tmpDir, []string{"Person"}, outPath, false, "")

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

func TestGenerator_PointerToPrimitive(t *testing.T) {
	tmpDir := t.TempDir()
	testFilePath := filepath.Join(tmpDir, "test_structs.go")
	testCode := `package mypkg

type Person struct {
	Age *int32
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
	g := NewGenerator(tmpDir, []string{"Person"}, outPath, false, "")

	err := g.Run("")
	if err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	outBytes, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	outStr := string(outBytes)
	if !strings.Contains(outStr, "{Name: \"Age\",") {
		t.Errorf("Expected output to contain field Age, got:\n%s", outStr)
	}
}

func TestGenerator_PointerToStruct(t *testing.T) {
	tmpDir := t.TempDir()
	testFilePath := filepath.Join(tmpDir, "test_structs.go")
	testCode := `package mypkg

type Nested struct {
	Value int32
}

type Person struct {
	Details *Nested
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
	g := NewGenerator(tmpDir, []string{"Person"}, outPath, false, "")

	err := g.Run("")
	if err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	outBytes, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	outStr := string(outBytes)
	if !strings.Contains(outStr, "func AppendNestedStruct(b *array.StructBuilder, row Nested)") {
		t.Errorf("Expected output to contain AppendNestedStruct, got:\n%s", outStr)
	}
	if !strings.Contains(outStr, "{Name: \"Details\",") {
		t.Errorf("Expected output to contain field Details, got:\n%s", outStr)
	}
}

func TestGenerator_SliceOfPointerToPrimitive(t *testing.T) {
	tmpDir := t.TempDir()
	testFilePath := filepath.Join(tmpDir, "test_structs.go")
	testCode := `package mypkg

type Person struct {
	Scores []*int32
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
	g := NewGenerator(tmpDir, []string{"Person"}, outPath, false, "")

	err := g.Run("")
	if err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	outBytes, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	outStr := string(outBytes)
	if !strings.Contains(outStr, "{Name: \"Scores\",") {
		t.Errorf("Expected output to contain field Scores, got:\n%s", outStr)
	}
}

func TestGenerator_SliceOfPointerToStruct(t *testing.T) {
	tmpDir := t.TempDir()
	testFilePath := filepath.Join(tmpDir, "test_structs.go")
	testCode := `package mypkg

type Nested struct {
	Value int32
}

type Person struct {
	DetailsList []*Nested
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
	g := NewGenerator(tmpDir, []string{"Person"}, outPath, false, "")

	err := g.Run("")
	if err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	outBytes, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	outStr := string(outBytes)
	if !strings.Contains(outStr, "{Name: \"DetailsList\",") {
		t.Errorf("Expected output to contain field DetailsList, got:\n%s", outStr)
	}
}

// TestGenerator_IPAddressStruct tests the generation of Arrow writers for a struct containing IPAddress
//
// In particular, this tests the case where the referenced types are in a different package.
// In this case, the AST returns `*ast.SelectorExpr` for the type, and we need to
// resolve it to the actual type. When it is local, we get `*ast.Ident`.
func TestGenerator_IPAddressStruct(t *testing.T) {
	tmpDir := t.TempDir()
	testFilePath := filepath.Join(tmpDir, "test_structs.go")
	testCode := `package mypkg

import "net/netip"

type IPAddress struct {
	IPv4 *netip.Addr
	IPv6 *netip.Addr
}

type Outer struct {
	SGW IPAddress
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
	g := NewGenerator(tmpDir, []string{"Outer"}, outPath, true, "")

	err := g.Run("")
	if err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	outBytes, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	outStr := string(outBytes)

	// The Outer struct should contain the SGW field
	if !strings.Contains(outStr, "{Name: \"SGW\",") {
		t.Errorf("Expected output to contain field SGW, got:\n%s", outStr)
	}

	// The IPAddress struct should contain both IPv4 and IPv6 fields
	if !strings.Contains(outStr, "{Name: \"IPv4\",") {
		t.Errorf("Expected output to contain field IPv4, got:\n%s", outStr)
	}
	if !strings.Contains(outStr, "{Name: \"IPv6\",") {
		t.Errorf("Expected output to contain field IPv6, got:\n%s", outStr)
	}

	// Verify MarshalText is used (netip.Addr implements encoding.TextMarshaler)
	if !strings.Contains(outStr, ".MarshalText()") {
		t.Errorf("Expected output to use MarshalText(), got:\n%s", outStr)
	}
}

func TestGenerator_ExternalTypeStringFallback(t *testing.T) {
	tmpDir := t.TempDir()
	testFilePath := filepath.Join(tmpDir, "test_structs.go")
	// url.URL has String() but NOT MarshalText, so String should be used
	testCode := `package mypkg

import "net/url"

type WebLink struct {
	Target *url.URL
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
	g := NewGenerator(tmpDir, []string{"WebLink"}, outPath, false, "")

	err := g.Run("")
	if err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	outBytes, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	outStr := string(outBytes)
	if !strings.Contains(outStr, "{Name: \"Target\",") {
		t.Errorf("Expected output to contain field Target, got:\n%s", outStr)
	}
	if !strings.Contains(outStr, ".String()") {
		t.Errorf("Expected output to use String() for url.URL, got:\n%s", outStr)
	}
	// Make sure MarshalText is NOT used (url.URL doesn't implement it)
	if strings.Contains(outStr, "MarshalText") {
		t.Errorf("Did not expect MarshalText for url.URL, got:\n%s", outStr)
	}
}

func TestGenerator_ExternalTypeUnsupported(t *testing.T) {
	tmpDir := t.TempDir()
	testFilePath := filepath.Join(tmpDir, "test_structs.go")
	// sync.Mutex does not implement TextMarshaler, Stringer, or BinaryMarshaler
	testCode := `package mypkg

import "sync"

type Container struct {
	ID   int32
	Lock *sync.Mutex
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
	g := NewGenerator(tmpDir, []string{"Container"}, outPath, false, "")

	err := g.Run("")
	if err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	outBytes, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	outStr := string(outBytes)

	// ID should still be present
	if !strings.Contains(outStr, "{Name: \"ID\",") {
		t.Errorf("Expected output to contain field ID, got:\n%s", outStr)
	}
	// Lock should be skipped (sync.Mutex has no marshal interface)
	if strings.Contains(outStr, "Lock") {
		t.Errorf("Expected Lock field to be skipped, but it was found in output:\n%s", outStr)
	}
}

func TestGenerator_NamedPrimitiveType(t *testing.T) {
	tmpDir := t.TempDir()
	testFilePath := filepath.Join(tmpDir, "test_structs.go")
	// MyStates is a named type over int — this should be mapped like int
	testCode := `package mypkg

type MyStates int

type Device struct {
	ID    int32
	State MyStates
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
	g := NewGenerator(tmpDir, []string{"Device"}, outPath, true, "")

	err := g.Run("")
	if err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	outBytes, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	outStr := string(outBytes)

	// ID should be present
	if !strings.Contains(outStr, `{Name: "ID",`) {
		t.Errorf("Expected output to contain field ID, got:\n%s", outStr)
	}
	// State should also be present — named type over int should resolve to int64
	if !strings.Contains(outStr, `{Name: "State",`) {
		t.Errorf("Expected output to contain field State (named type MyStates over int), got:\n%s", outStr)
	}
}
