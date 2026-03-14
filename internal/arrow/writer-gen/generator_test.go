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
				Name:         "Tags",
				GoType:       "[]string",
				ArrowType:    "arrow.ListOf(arrow.BinaryTypes.String)",
				ArrowBuilder: "*array.ListBuilder",
				IsList:       true,
				EltInfo: &FieldInfo{
					GoType:       "string",
					ArrowType:    "arrow.BinaryTypes.String",
					ArrowBuilder: "*array.StringBuilder",
					CastType:     "string",
				},
			},
			{
				Name:         "Scores",
				GoType:       "map[string]float64",
				ArrowType:    "arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Float64)",
				ArrowBuilder: "*array.MapBuilder",
				IsMap:        true,
				KeyInfo: &FieldInfo{
					GoType:       "string",
					ArrowType:    "arrow.BinaryTypes.String",
					ArrowBuilder: "*array.StringBuilder",
					CastType:     "string",
				},
				EltInfo: &FieldInfo{
					GoType:       "float64",
					ArrowType:    "arrow.PrimitiveTypes.Float64",
					ArrowBuilder: "*array.Float64Builder",
					CastType:     "float64",
				},
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

// TestGenerator_RunOutput is a table-driven test that covers single-package code generation
// scenarios. Each case writes a Go source file, runs the generator, and checks the output
// for expected/unexpected strings.
func TestGenerator_RunOutput(t *testing.T) {
	tests := []struct {
		name           string
		goCode         string
		targetStruct   string
		pkgOverride    string // if non-empty, passed to Run() instead of ""
		mustContain    []string
		mustNotContain []string
	}{
		{
			name: "template-output",
			goCode: `package mypkg

type Person struct {
	Name string
}
`,
			targetStruct: "Person",
			mustContain: []string{
				"package mypkg",
				"func NewPersonSchema() *arrow.Schema",
				"type PersonArrowWriter struct",
			},
		},
		{
			name: "override-pkg",
			goCode: `package mypkg

type Person struct {
	Name string
}
`,
			targetStruct: "Person",
			pkgOverride:  "differentpkg",
			mustContain: []string{
				"package differentpkg",
				`"mypkg"`,
				"func (w *PersonArrowWriter) Append(row *mypkg.Person)",
			},
			mustNotContain: []string{
				"row Person)",
			},
		},
		{
			name: "pointer-to-primitive",
			goCode: `package mypkg

type Person struct {
	Age *int32
}
`,
			targetStruct: "Person",
			mustContain:  []string{`{Name: "Age",`},
		},
		{
			name: "outer-struct-with-mutex-inner",
			goCode: `package mypkg

import "sync"

type Inner struct {
	Value int32
	Lock  *sync.Mutex
}

type Outer struct {
	ID    int32
	Child Inner
}
`,
			targetStruct: "Outer",
			mustContain: []string{
				`{Name: "ID",`,
				`{Name: "Child",`,
				`{Name: "Value",`,
				"func AppendInnerStruct(",
				"func AppendInnerStruct(b *array.StructBuilder, row *Inner)",
			},
			mustNotContain: []string{"Lock"},
		},
		{
			name: "pointer-to-struct",
			goCode: `package mypkg

type Nested struct {
	Value int32
}

type Person struct {
	Details *Nested
}
`,
			targetStruct: "Person",
			mustContain: []string{
				"func AppendNestedStruct(b *array.StructBuilder, row *Nested)",
				`{Name: "Details",`,
			},
		},
		{
			name: "slice-of-pointer-to-primitive",
			goCode: `package mypkg

type Person struct {
	Scores []*int32
}
`,
			targetStruct: "Person",
			mustContain:  []string{`{Name: "Scores",`},
		},
		{
			name: "slice-of-pointer-to-struct",
			goCode: `package mypkg

type Nested struct {
	Value int32
}

type Person struct {
	DetailsList []*Nested
}
`,
			targetStruct: "Person",
			mustContain:  []string{`{Name: "DetailsList",`},
		},
		{
			name: "external-type-marshal-text",
			goCode: `package mypkg

import "net/netip"

type IPAddress struct {
	IPv4 *netip.Addr
	IPv6 *netip.Addr
}

type Outer struct {
	SGW IPAddress
}
`,
			targetStruct: "Outer",
			mustContain: []string{
				`{Name: "SGW",`,
				`{Name: "IPv4",`,
				`{Name: "IPv6",`,
				".MarshalText()",
			},
		},
		{
			name: "external-type-string-fallback",
			goCode: `package mypkg

import "net/url"

type WebLink struct {
	Target *url.URL
}
`,
			targetStruct:   "WebLink",
			mustContain:    []string{`{Name: "Target",`, ".String()"},
			mustNotContain: []string{"MarshalText"},
		},
		{
			name: "external-type-unsupported-skipped",
			goCode: `package mypkg

import "sync"

type Container struct {
	ID   int32
	Lock *sync.Mutex
}
`,
			targetStruct:   "Container",
			mustContain:    []string{`{Name: "ID",`},
			mustNotContain: []string{"Lock"},
		},
		{
			name: "named-primitive-type",
			goCode: `package mypkg

type MyStates int

type Device struct {
	ID    int32
	State MyStates
}
`,
			targetStruct: "Device",
			mustContain:  []string{`{Name: "ID",`, `{Name: "State",`},
		},
		{
			name: "slice-of-external-type-pointer",
			goCode: `package mypkg

import "net/netip"

type IPAddresses struct {
	IPv4s []*netip.Addr
}
`,
			targetStruct: "IPAddresses",
			mustContain: []string{
				`{Name: "IPv4s",`,
				"arrow.ListOf(arrow.BinaryTypes.String)",
				".MarshalText()",
				"AppendNull",
			},
		},
		{
			name: "blank-identifier-field-skipped",
			goCode: `package mypkg

type Padded struct {
	ID   int32
	_    int32
	Name string
}
`,
			targetStruct:   "Padded",
			mustContain:    []string{"row.ID", "row.Name", "NewPaddedArrowWriter"},
			mustNotContain: []string{"row._"},
		},
		{
			name: "triple-nested-slice",
			goCode: `package mypkg

type Deep struct {
	ID    int32
	Cube  [][][]int32
}
`,
			targetStruct: "Deep",
			mustContain: []string{
				"row.ID",
				"row.Cube",
				"v0Bldr",
				"v1Bldr",
				"v2Bldr",
				"for _, v0 := range",
				"for _, v1 := range v0",
				"for _, v2 := range v1",
			},
		},
		{
			name: "nested-slice",
			goCode: `package mypkg

type Matrix struct {
	ID   int32
	Grid [][]int32
	Tags [][]string
}
`,
			targetStruct: "Matrix",
			mustContain: []string{
				"arrow.ListOf(arrow.ListOf(",
				"NewMatrixArrowWriter",
				"v0Bldr",
				"v1Bldr",
				"for _, v0 := range",
				"for _, v1 := range v0",
			},
			mustNotContain: []string{},
		},
		{
			name: "map-with-slice-value",
			goCode: `package mypkg

type Grouped struct {
	ID   int32
	Data map[string][]int32
}
`,
			targetStruct: "Grouped",
			mustContain: []string{
				"row.Data",
				"arrow.MapOf(arrow.BinaryTypes.String, arrow.ListOf(arrow.PrimitiveTypes.Int32))",
				"v0KeyBldr",
				"v0ValBldr",
				"v1Bldr",
				"for _, v1 := range v0V",
			},
		},
		{
			name: "nested-map",
			goCode: `package mypkg

type Config struct {
	ID       int32
	Settings map[string]map[string]int32
}
`,
			targetStruct: "Config",
			mustContain: []string{
				"row.Settings",
				"arrow.MapOf(arrow.BinaryTypes.String, arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32))",
				"v0KeyBldr",
				"v0ValBldr",
				"v1KeyBldr",
				"v1ValBldr",
			},
		},
		{
			name: "list-of-maps",
			goCode: `package mypkg

type Timeline struct {
	ID     int32
	Events []map[string]int32
}
`,
			targetStruct: "Timeline",
			mustContain: []string{
				"row.Events",
				"arrow.ListOf(arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32))",
				"v0Bldr",
				"v1KeyBldr",
				"v1ValBldr",
			},
		},
		{
			name: "pointer-to-named-primitive-type",
			goCode: `package mypkg

type MyStates int

type Device struct {
	ID    *int32
	State *MyStates
}
`,
			targetStruct: "Device",
			mustContain:  []string{`{Name: "ID",`, `{Name: "State",`},
		},
		{
			name: "time-duration-as-int64",
			goCode: `package mypkg

import "time"

type Event struct {
	ID       int32
	Duration time.Duration
}
`,
			targetStruct: "Event",
			mustContain: []string{
				`{Name: "Duration",`,
				"Int64Builder",
				"int64(",
			},
			mustNotContain: []string{
				"String()",
				"MarshalText",
				"StringBuilder",
			},
		},
		{
			name: "pointer-to-time-duration",
			goCode: `package mypkg

import "time"

type Event struct {
	ID      int32
	Timeout *time.Duration
}
`,
			targetStruct: "Event",
			mustContain: []string{
				`{Name: "Timeout",`,
				"Int64Builder",
				"int64(",
				"AppendNull",
			},
			mustNotContain: []string{
				"String()",
				"MarshalText",
			},
		},
		{
			name: "time-time-as-timestamp",
			goCode: `package mypkg

import "time"

type Event struct {
	ID        int32
	CreatedAt time.Time
}
`,
			targetStruct: "Event",
			mustContain: []string{
				`{Name: "CreatedAt",`,
				"TimestampBuilder",
				"Timestamp_ns",
				".UnixNano()",
				"arrow.Timestamp(",
			},
			mustNotContain: []string{
				"MarshalText",
				"StringBuilder",
				"String()",
			},
		},
		{
			name: "pointer-to-time-time",
			goCode: `package mypkg

import "time"

type Event struct {
	ID        int32
	DeletedAt *time.Time
}
`,
			targetStruct: "Event",
			mustContain: []string{
				`{Name: "DeletedAt",`,
				"TimestampBuilder",
				".UnixNano()",
				"AppendNull",
			},
			mustNotContain: []string{
				"MarshalText",
				"StringBuilder",
			},
		},
		{
			name: "embedded-struct-flattened",
			goCode: `package mypkg

type Base struct {
	ID        int32
	CreatedAt string
}

type Device struct {
	Base
	Name string
}
`,
			targetStruct: "Device",
			mustContain: []string{
				"row.ID",
				"row.CreatedAt",
				"row.Name",
				`{Name: "ID",`,
				`{Name: "CreatedAt",`,
				`{Name: "Name",`,
				"NewDeviceArrowWriter",
			},
			mustNotContain: []string{
				"row.Base",
				"NewBaseSchema",
				"AppendBaseStruct",
			},
		},
		{
			name: "embedded-struct-shadowed-field",
			goCode: `package mypkg

type Base struct {
	ID   int32
	Name string
}

type Device struct {
	Base
	Name  string
	Label string
}
`,
			targetStruct: "Device",
			mustContain: []string{
				"row.ID",
				"row.Name",
				"row.Label",
				`{Name: "ID",`,
				`{Name: "Name",`,
				`{Name: "Label",`,
			},
			mustNotContain: []string{
				"AppendBaseStruct",
			},
		},
		{
			name: "embedded-non-struct-skipped",
			goCode: `package mypkg

type MyString string

type Container struct {
	MyString
	Value int32
}
`,
			targetStruct:   "Container",
			mustContain:    []string{"row.Value"},
			mustNotContain: []string{"MyString"},
		},
		{
			name: "pointer-embedded-struct-skipped",
			goCode: `package mypkg

type Base struct {
	ID int32
}

type Device struct {
	*Base
	Name string
}
`,
			targetStruct:   "Device",
			mustContain:    []string{"row.Name"},
			mustNotContain: []string{"row.ID", "AppendBaseStruct"},
		},
		{
			name: "embedded-struct-field-ordering",
			goCode: `package mypkg

type Meta struct {
	Version int32
}

type Device struct {
	Name string
	Meta
	Label string
}
`,
			targetStruct: "Device",
			mustContain: []string{
				`{Name: "Name",`,
				`{Name: "Version",`,
				`{Name: "Label",`,
			},
		},
		{
			name: "embedded-cross-ambiguity-skipped",
			goCode: `package mypkg

type Base1 struct {
	ID    int32
	Alpha string
}

type Base2 struct {
	ID   int32
	Beta string
}

type Device struct {
	Base1
	Base2
	Name string
}
`,
			targetStruct: "Device",
			mustContain: []string{
				"row.Alpha",
				"row.Beta",
				"row.Name",
			},
			mustNotContain: []string{
				"row.ID", // ambiguous — promoted by both Base1 and Base2
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			if err := os.WriteFile(filepath.Join(tmpDir, "test_structs.go"), []byte(tt.goCode), 0644); err != nil {
				t.Fatalf("Failed to write test file: %v", err)
			}
			if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module mypkg\n\ngo 1.25.0\n"), 0644); err != nil {
				t.Fatalf("Failed to write go.mod: %v", err)
			}

			outPath := filepath.Join(tmpDir, "out_writer.go")
			g := NewGenerator([]string{tmpDir}, []string{tt.targetStruct}, outPath, false, nil)

			pkgOverride := tt.pkgOverride
			if err := g.Run(pkgOverride); err != nil {
				t.Fatalf("Run() failed: %v", err)
			}

			outBytes, err := os.ReadFile(outPath)
			if err != nil {
				t.Fatalf("Failed to read output file: %v", err)
			}
			outStr := string(outBytes)

			for _, want := range tt.mustContain {
				if !strings.Contains(outStr, want) {
					t.Errorf("Expected output to contain %q, got:\n%s", want, outStr)
				}
			}
			for _, unwanted := range tt.mustNotContain {
				if strings.Contains(outStr, unwanted) {
					t.Errorf("Expected output NOT to contain %q, got:\n%s", unwanted, outStr)
				}
			}
		})
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

// TestGenerator_FixedSizeArray tests that fixed-size arrays ([N]T) are correctly mapped
// to Arrow FixedSizeList types and that the generated code compiles.
func TestGenerator_FixedSizeArray(t *testing.T) {
	tmpDir := t.TempDir()
	testFilePath := filepath.Join(tmpDir, "test_structs.go")
	testCode := `package mypkg

type Packet struct {
	Header [4]byte
	Scores [3]int32
	Label  string
}
`
	if err := os.WriteFile(testFilePath, []byte(testCode), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	modContent := "module mypkg\n\ngo 1.25.0\n"
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte(modContent), 0644); err != nil {
		t.Fatalf("Failed to write go.mod: %v", err)
	}

	g := NewGenerator([]string{tmpDir}, []string{"Packet"}, filepath.Join(tmpDir, "out.go"), false, nil)

	// Test Parse()-level FieldInfo
	_, _, structs, err := g.Parse()
	if err != nil {
		t.Fatalf("Parse() failed: %v", err)
	}
	if len(structs) != 1 {
		t.Fatalf("Expected 1 struct, got %d", len(structs))
	}

	fields := structs[0].Fields
	if len(fields) != 3 {
		t.Fatalf("Expected 3 fields, got %d", len(fields))
	}

	// Header: [4]byte → FixedSizeList of Uint8
	header := fields[0]
	if header.Name != "Header" {
		t.Errorf("Expected first field Header, got %s", header.Name)
	}
	if !header.IsFixedSizeList {
		t.Errorf("Header: expected IsFixedSizeList=true")
	}
	if header.FixedSizeLen != "4" {
		t.Errorf("Header: expected FixedSizeLen=4, got %s", header.FixedSizeLen)
	}
	if header.ArrowBuilder != "*array.FixedSizeListBuilder" {
		t.Errorf("Header: expected ArrowBuilder=*array.FixedSizeListBuilder, got %s", header.ArrowBuilder)
	}
	if !strings.Contains(header.ArrowType, "FixedSizeListOfNonNullable") {
		t.Errorf("Header: expected ArrowType to contain FixedSizeListOfNonNullable, got %s", header.ArrowType)
	}

	// Scores: [3]int32 → FixedSizeList of Int32
	scores := fields[1]
	if !scores.IsFixedSizeList {
		t.Errorf("Scores: expected IsFixedSizeList=true")
	}
	if scores.FixedSizeLen != "3" {
		t.Errorf("Scores: expected FixedSizeLen=3, got %s", scores.FixedSizeLen)
	}
	if scores.EltInfo == nil || scores.EltInfo.CastType != "int32" {
		castType := ""
		if scores.EltInfo != nil {
			castType = scores.EltInfo.CastType
		}
		t.Errorf("Scores: expected EltInfo.CastType=int32, got %s", castType)
	}

	// Test Run() — verify the generated code is valid Go (gofmt succeeds)
	outPath := filepath.Join(tmpDir, "out.go")
	g2 := NewGenerator([]string{tmpDir}, []string{"Packet"}, outPath, false, nil)
	if err := g2.Run(""); err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	outBytes, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("Failed to read output: %v", err)
	}
	outStr := string(outBytes)

	if !strings.Contains(outStr, "FixedSizeListBuilder") {
		t.Errorf("Expected output to contain FixedSizeListBuilder, got:\n%s", outStr)
	}
	if !strings.Contains(outStr, "FixedSizeListOfNonNullable") {
		t.Errorf("Expected output to contain FixedSizeListOfNonNullable, got:\n%s", outStr)
	}
	// Fixed-size arrays are value types — no nil check should be generated
	if strings.Contains(outStr, "row.Header == nil") || strings.Contains(outStr, "row.Scores == nil") {
		t.Errorf("Expected no nil check for fixed-size array fields, got:\n%s", outStr)
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
