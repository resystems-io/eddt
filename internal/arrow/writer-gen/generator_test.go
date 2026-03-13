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
	ID         int32
	Name       string
	Valid      bool
	Value      float64
	Tags       []string
	Scores     map[string]float64
	SingleByte byte
	ByteSlice  []byte
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

	expected := StructInfo{
		Name:    "SimpleStruct",
		PkgPath: "testpkg",
		PkgName: "testpkg",
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
			{Name: "SingleByte", GoType: "byte", ArrowType: "arrow.PrimitiveTypes.Uint8", ArrowBuilder: "*array.Uint8Builder", CastType: "uint8"},
			{Name: "ByteSlice", GoType: "[]byte", ArrowType: "arrow.BinaryTypes.Binary", ArrowBuilder: "*array.BinaryBuilder", CastType: "[]byte"},
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
		{"byte", "byte", "arrow.PrimitiveTypes.Uint8", "*array.Uint8Builder", false},
		{"rune", "rune", "arrow.PrimitiveTypes.Int32", "*array.Int32Builder", false},
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
	g := NewGenerator([]string{tmpDir}, []string{"Person"}, outPath, false, nil)

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
	g := NewGenerator([]string{tmpDir}, []string{"Person"}, outPath, false, nil)

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
	if !strings.Contains(outStr, "func (w *PersonArrowWriter) Append(row *mypkg.Person)") {
		t.Errorf("Expected output to use imported struct *mypkg.Person, got:\n%s", outStr)
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
	g := NewGenerator([]string{tmpDir}, []string{"Person"}, outPath, false, nil)

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
	g := NewGenerator([]string{tmpDir}, []string{"Person"}, outPath, false, nil)

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

// TestGenerator_OuterStructWithMutexInner tests that generation succeeds when an inner struct
// contains a field (Lock *sync.Mutex) that has no supported marshal interface.
// The Lock field must be silently skipped. The test verifies that AppendInnerStruct
// receives the inner struct by pointer (*Inner), avoiding any copy of a struct that
// contains mutex-adjacent fields and eliminating the associated linter warnings.
func TestGenerator_OuterStructWithMutexInner(t *testing.T) {
	tmpDir := t.TempDir()
	testFilePath := filepath.Join(tmpDir, "test_structs.go")
	testCode := `package mypkg

import "sync"

type Inner struct {
	Value int32
	Lock  *sync.Mutex
}

type Outer struct {
	ID    int32
	Child Inner
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
	g := NewGenerator([]string{tmpDir}, []string{"Outer"}, outPath, false, nil)

	err := g.Run("")
	if err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	outBytes, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	outStr := string(outBytes)

	// Outer and Inner should both appear
	if !strings.Contains(outStr, `{Name: "ID",`) {
		t.Errorf("Expected output to contain field ID, got:\n%s", outStr)
	}
	if !strings.Contains(outStr, `{Name: "Child",`) {
		t.Errorf("Expected output to contain field Child, got:\n%s", outStr)
	}

	// Value field on Inner should be present
	if !strings.Contains(outStr, `{Name: "Value",`) {
		t.Errorf("Expected output to contain field Value on Inner, got:\n%s", outStr)
	}

	// Lock must be skipped — sync.Mutex implements no supported marshal interface
	if strings.Contains(outStr, "Lock") {
		t.Errorf("Expected Lock field to be skipped (no marshal interface), but found it in output:\n%s", outStr)
	}

	// AppendInnerStruct must exist — Inner is referenced as a nested struct
	if !strings.Contains(outStr, "func AppendInnerStruct(") {
		t.Errorf("Expected output to contain AppendInnerStruct helper, got:\n%s", outStr)
	}

	// AppendInnerStruct must receive Inner by pointer to avoid copying mutex-adjacent fields.
	if !strings.Contains(outStr, "func AppendInnerStruct(b *array.StructBuilder, row *Inner)") {
		t.Errorf("Expected AppendInnerStruct to take *Inner by pointer, got:\n%s", outStr)
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
	g := NewGenerator([]string{tmpDir}, []string{"Person"}, outPath, false, nil)

	err := g.Run("")
	if err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	outBytes, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	outStr := string(outBytes)
	if !strings.Contains(outStr, "func AppendNestedStruct(b *array.StructBuilder, row *Nested)") {
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
	g := NewGenerator([]string{tmpDir}, []string{"Person"}, outPath, false, nil)

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
	g := NewGenerator([]string{tmpDir}, []string{"Person"}, outPath, false, nil)

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
	g := NewGenerator([]string{tmpDir}, []string{"Outer"}, outPath, true, nil)

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
	g := NewGenerator([]string{tmpDir}, []string{"WebLink"}, outPath, false, nil)

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
	g := NewGenerator([]string{tmpDir}, []string{"Container"}, outPath, false, nil)

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
	g := NewGenerator([]string{tmpDir}, []string{"Device"}, outPath, true, nil)

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

// TestGenerator_IPAddressStructSlice tests generation for a struct containing a slice of pointers
// to an external package type (e.g. []*netip.Addr). The elements must be serialized via their
// marshal interface (MarshalText for netip.Addr), and nil pointer elements must append null.
func TestGenerator_IPAddressStructSlice(t *testing.T) {
	tmpDir := t.TempDir()
	testFilePath := filepath.Join(tmpDir, "test_structs.go")
	testCode := `package mypkg

import "net/netip"

type IPAddresses struct {
	IPv4s []*netip.Addr
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
	g := NewGenerator([]string{tmpDir}, []string{"IPAddresses"}, outPath, true, nil)

	err := g.Run("")
	if err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	outBytes, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	outStr := string(outBytes)

	// The IPAddresses struct should contain the IPv4s list field
	if !strings.Contains(outStr, `{Name: "IPv4s",`) {
		t.Errorf("Expected output to contain field IPv4s, got:\n%s", outStr)
	}

	// The list type should be arrow.ListOf(arrow.BinaryTypes.String) since netip.Addr uses MarshalText
	if !strings.Contains(outStr, "arrow.ListOf(arrow.BinaryTypes.String)") {
		t.Errorf("Expected output to use arrow.ListOf(arrow.BinaryTypes.String) for []*netip.Addr, got:\n%s", outStr)
	}

	// Elements should be serialized via MarshalText
	if !strings.Contains(outStr, ".MarshalText()") {
		t.Errorf("Expected output to use MarshalText() for *netip.Addr elements, got:\n%s", outStr)
	}

	// Nil pointer elements must append null, not panic
	if !strings.Contains(outStr, "AppendNull") {
		t.Errorf("Expected output to handle nil pointer elements with AppendNull, got:\n%s", outStr)
	}
}

func TestGenerator_PointerToNamedPrimitiveType(t *testing.T) {
	tmpDir := t.TempDir()
	testFilePath := filepath.Join(tmpDir, "test_structs.go")
	// MyStates is a named type over int — this should be mapped like int
	testCode := `package mypkg

type MyStates int

type Device struct {
	ID    *int32
	State *MyStates
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
	g := NewGenerator([]string{tmpDir}, []string{"Device"}, outPath, true, nil)

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

// TestGenerator_MultiPackageResolution tests that when two packages are provided, structs from
// the second package are resolved natively as Arrow struct types (not as marshal fallbacks).
// pkg1.Outer has a field of type pkg2.Inner; Inner must appear as a StructBuilder in the output.
func TestGenerator_MultiPackageResolution(t *testing.T) {
	tmpDir := t.TempDir()

	// pkg1: contains Outer which references Inner from pkg2
	pkg1Dir := filepath.Join(tmpDir, "pkg1")
	pkg2Dir := filepath.Join(tmpDir, "pkg2")
	if err := os.MkdirAll(pkg1Dir, 0755); err != nil {
		t.Fatalf("mkdir pkg1: %v", err)
	}
	if err := os.MkdirAll(pkg2Dir, 0755); err != nil {
		t.Fatalf("mkdir pkg2: %v", err)
	}

	pkg2Code := `package pkg2

type Inner struct {
	Value int32
}
`
	if err := os.WriteFile(filepath.Join(pkg2Dir, "inner.go"), []byte(pkg2Code), 0644); err != nil {
		t.Fatalf("write pkg2: %v", err)
	}
	if err := os.WriteFile(filepath.Join(pkg2Dir, "go.mod"), []byte("module pkg2\n\ngo 1.25.0\n"), 0644); err != nil {
		t.Fatalf("write pkg2 go.mod: %v", err)
	}

	pkg1Code := `package pkg1

import "pkg2"

type Outer struct {
	ID    int32
	Child pkg2.Inner
}
`
	if err := os.WriteFile(filepath.Join(pkg1Dir, "outer.go"), []byte(pkg1Code), 0644); err != nil {
		t.Fatalf("write pkg1: %v", err)
	}
	pkg1Mod := "module pkg1\n\ngo 1.25.0\n\nrequire pkg2 v0.0.0\n\nreplace pkg2 => " + pkg2Dir + "\n"
	if err := os.WriteFile(filepath.Join(pkg1Dir, "go.mod"), []byte(pkg1Mod), 0644); err != nil {
		t.Fatalf("write pkg1 go.mod: %v", err)
	}

	outPath := filepath.Join(tmpDir, "out_writer.go")
	g := NewGenerator([]string{pkg1Dir, pkg2Dir}, []string{"Outer"}, outPath, false, nil)

	_, _, structs, err := g.Parse()
	if err != nil {
		t.Fatalf("Parse() failed: %v", err)
	}

	// Both Outer and Inner should be in the results
	names := map[string]bool{}
	for _, s := range structs {
		names[s.Name] = true
	}
	if !names["Outer"] {
		t.Errorf("Expected Outer struct in results, got: %v", structs)
	}
	if !names["Inner"] {
		t.Errorf("Expected Inner struct in results (resolved natively from pkg2), got: %v", structs)
	}

	// Outer should have PkgPath/PkgName from pkg1
	for _, s := range structs {
		if s.Name == "Outer" {
			if s.PkgName != "pkg1" {
				t.Errorf("Outer.PkgName: want pkg1, got %s", s.PkgName)
			}
		}
		if s.Name == "Inner" {
			if s.PkgName != "pkg2" {
				t.Errorf("Inner.PkgName: want pkg2, got %s", s.PkgName)
			}
		}
	}

	// Child field on Outer should be IsStruct=true (native Arrow struct, not marshal fallback)
	for _, s := range structs {
		if s.Name == "Outer" {
			for _, f := range s.Fields {
				if f.Name == "Child" {
					if !f.IsStruct {
						t.Errorf("Child field should be IsStruct=true (native Arrow struct resolution), got IsStruct=false. MarshalMethod=%q", f.MarshalMethod)
					}
					if f.StructName != "Inner" {
						t.Errorf("Child StructName: want Inner, got %s", f.StructName)
					}
				}
			}
		}
	}
}

// TestGenerator_AliasMapping tests parsing of --pkg-alias entries and error handling.
func TestGenerator_AliasMapping(t *testing.T) {
	tmpDir := t.TempDir()
	testFilePath := filepath.Join(tmpDir, "model.go")
	testCode := `package mypkg

type Person struct {
	Name string
}
`
	if err := os.WriteFile(testFilePath, []byte(testCode), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module mypkg\n\ngo 1.25.0\n"), 0644); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}

	outPath := filepath.Join(tmpDir, "out.go")

	t.Run("valid-alias-applied", func(t *testing.T) {
		g := NewGenerator([]string{tmpDir}, []string{"Person"}, outPath, false, []string{"mypkg=mypkgalias"})
		err := g.Run("writers")
		if err != nil {
			t.Fatalf("Run() failed: %v", err)
		}
		out, _ := os.ReadFile(outPath)
		outStr := string(out)
		if !strings.Contains(outStr, `mypkgalias "mypkg"`) {
			t.Errorf("Expected aliased import mypkgalias \"mypkg\", got:\n%s", outStr)
		}
		if !strings.Contains(outStr, "row *mypkgalias.Person") {
			t.Errorf("Expected row *mypkgalias.Person, got:\n%s", outStr)
		}
	})

	t.Run("missing-equals-sign", func(t *testing.T) {
		g := NewGenerator([]string{tmpDir}, []string{"Person"}, outPath, false, []string{"noequalssign"})
		err := g.Run("")
		if err == nil {
			t.Fatalf("Expected error for alias without '=', got nil")
		}
		if !strings.Contains(err.Error(), "invalid --pkg-alias") {
			t.Errorf("Expected 'invalid --pkg-alias' error, got: %v", err)
		}
	})

	t.Run("empty-original", func(t *testing.T) {
		g := NewGenerator([]string{tmpDir}, []string{"Person"}, outPath, false, []string{"=alias"})
		err := g.Run("")
		if err == nil {
			t.Fatalf("Expected error for empty original in alias, got nil")
		}
		if !strings.Contains(err.Error(), "invalid --pkg-alias") {
			t.Errorf("Expected 'invalid --pkg-alias' error, got: %v", err)
		}
	})
}
