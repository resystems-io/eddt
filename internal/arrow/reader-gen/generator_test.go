package readergen

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"go.resystems.io/eddt/internal/arrow/gencommon"
)

// TestGenerator_Parse consolidates the reader-gen Parse test cases into a
// table-driven test. Each case writes a Go source file and go.mod, runs
// Parse(), and verifies the result via a per-case check function.
//
// Cases 1–5 (simple, pointers, lists, fixed-size-lists, maps) use cmp.Diff
// against a full expected StructInfo. Case 6 (struct fields) uses targeted
// field-by-field assertions because it discovers multiple structs and requires
// checking cross-struct relationships.
func TestGenerator_Parse(t *testing.T) {
	diffStructs := func(t *testing.T, want []gencommon.StructInfo, got []gencommon.StructInfo) {
		t.Helper()
		if diff := cmp.Diff(want, got, cmpopts.IgnoreUnexported(gencommon.StructInfo{})); diff != "" {
			t.Errorf("Parse() struct mismatch (-want +got):\n%s", diff)
		}
	}

	tests := []struct {
		name    string
		goCode  string
		targets []string
		check   func(t *testing.T, pkgName, pkgPath string, structs []gencommon.StructInfo)
	}{
		{
			name: "simple-struct",
			goCode: `package testpkg

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
`,
			targets: []string{"SimpleStruct"},
			check: func(t *testing.T, pkgName, pkgPath string, structs []gencommon.StructInfo) {
				t.Helper()
				if pkgName != "testpkg" {
					t.Errorf("pkgName = %q, want %q", pkgName, "testpkg")
				}
				if pkgPath != "testpkg" {
					t.Errorf("pkgPath = %q, want %q", pkgPath, "testpkg")
				}
				diffStructs(t, []gencommon.StructInfo{{
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
				}}, structs)
			},
		},
		{
			name: "pointer-primitives",
			goCode: `package testpkg

type MyID int32

type PtrStruct struct {
	OptInt    *int32
	OptFloat  *float64
	OptBool   *bool
	OptName   *string
	OptID     *MyID
}
`,
			targets: []string{"PtrStruct"},
			check: func(t *testing.T, _, _ string, structs []gencommon.StructInfo) {
				t.Helper()
				diffStructs(t, []gencommon.StructInfo{{
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
				}}, structs)
			},
		},
		{
			name: "list-fields",
			goCode: `package testpkg

type ListStruct struct {
	Tags  []string
	Grid  [][]int32
	Bytes [][]byte
}
`,
			targets: []string{"ListStruct"},
			check: func(t *testing.T, _, _ string, structs []gencommon.StructInfo) {
				t.Helper()
				diffStructs(t, []gencommon.StructInfo{{
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
							EltInfo:        &gencommon.FieldInfo{GoType: "string", ArrowType: "arrow.BinaryTypes.String", ArrowBuilder: "*array.StringBuilder", CastType: "string", ArrowArrayType: "*array.String", ValueMethod: "Value", ZeroExpr: `""`},
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
								EltInfo:        &gencommon.FieldInfo{GoType: "int32", ArrowType: "arrow.PrimitiveTypes.Int32", ArrowBuilder: "*array.Int32Builder", CastType: "int32", ArrowArrayType: "*array.Int32", ValueMethod: "Value", ZeroExpr: "0"},
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
							EltInfo:        &gencommon.FieldInfo{GoType: "[]byte", ArrowType: "arrow.BinaryTypes.Binary", ArrowBuilder: "*array.BinaryBuilder", CastType: "[]byte", ArrowArrayType: "*array.Binary", ValueMethod: "Value", ZeroExpr: "nil"},
						},
					},
				}}, structs)
			},
		},
		{
			name: "fixed-size-list-fields",
			goCode: `package testpkg

type FixedStruct struct {
	Header [4]byte
	Scores [3]int32
	Matrix [3][2]int32
}
`,
			targets: []string{"FixedStruct"},
			check: func(t *testing.T, _, _ string, structs []gencommon.StructInfo) {
				t.Helper()
				diffStructs(t, []gencommon.StructInfo{{
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
							EltInfo:         &gencommon.FieldInfo{GoType: "byte", ArrowType: "arrow.PrimitiveTypes.Uint8", ArrowBuilder: "*array.Uint8Builder", CastType: "uint8", ArrowArrayType: "*array.Uint8", ValueMethod: "Value", ZeroExpr: "0"},
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
							EltInfo:         &gencommon.FieldInfo{GoType: "int32", ArrowType: "arrow.PrimitiveTypes.Int32", ArrowBuilder: "*array.Int32Builder", CastType: "int32", ArrowArrayType: "*array.Int32", ValueMethod: "Value", ZeroExpr: "0"},
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
								EltInfo:         &gencommon.FieldInfo{GoType: "int32", ArrowType: "arrow.PrimitiveTypes.Int32", ArrowBuilder: "*array.Int32Builder", CastType: "int32", ArrowArrayType: "*array.Int32", ValueMethod: "Value", ZeroExpr: "0"},
							},
						},
					},
				}}, structs)
			},
		},
		{
			name: "map-fields",
			goCode: `package testpkg

type MapStruct struct {
	Scores  map[string]float64
	IntMap  map[int32]string
	Nested  map[string]map[string]int32
	ListVal map[string][]int32
}
`,
			targets: []string{"MapStruct"},
			check: func(t *testing.T, _, _ string, structs []gencommon.StructInfo) {
				t.Helper()
				strKey := &gencommon.FieldInfo{GoType: "string", ArrowType: "arrow.BinaryTypes.String", ArrowBuilder: "*array.StringBuilder", CastType: "string", ArrowArrayType: "*array.String", ValueMethod: "Value", ZeroExpr: `""`}
				diffStructs(t, []gencommon.StructInfo{{
					Name:    "MapStruct",
					PkgPath: "testpkg",
					PkgName: "testpkg",
					Fields: []gencommon.FieldInfo{
						{
							Name: "Scores", GoType: "map[string]float64",
							ArrowType: "arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Float64)", ArrowBuilder: "*array.MapBuilder",
							IsMap: true, ArrowArrayType: "*array.Map", ZeroExpr: "nil",
							KeyInfo: strKey,
							EltInfo: &gencommon.FieldInfo{GoType: "float64", ArrowType: "arrow.PrimitiveTypes.Float64", ArrowBuilder: "*array.Float64Builder", CastType: "float64", ArrowArrayType: "*array.Float64", ValueMethod: "Value", ZeroExpr: "0"},
						},
						{
							Name: "IntMap", GoType: "map[int32]string",
							ArrowType: "arrow.MapOf(arrow.PrimitiveTypes.Int32, arrow.BinaryTypes.String)", ArrowBuilder: "*array.MapBuilder",
							IsMap: true, ArrowArrayType: "*array.Map", ZeroExpr: "nil",
							KeyInfo: &gencommon.FieldInfo{GoType: "int32", ArrowType: "arrow.PrimitiveTypes.Int32", ArrowBuilder: "*array.Int32Builder", CastType: "int32", ArrowArrayType: "*array.Int32", ValueMethod: "Value", ZeroExpr: "0"},
							EltInfo: &gencommon.FieldInfo{GoType: "string", ArrowType: "arrow.BinaryTypes.String", ArrowBuilder: "*array.StringBuilder", CastType: "string", ArrowArrayType: "*array.String", ValueMethod: "Value", ZeroExpr: `""`},
						},
						{
							Name: "Nested", GoType: "map[string]map[string]int32",
							ArrowType: "arrow.MapOf(arrow.BinaryTypes.String, arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32))", ArrowBuilder: "*array.MapBuilder",
							IsMap: true, ArrowArrayType: "*array.Map", ZeroExpr: "nil",
							KeyInfo: strKey,
							EltInfo: &gencommon.FieldInfo{
								GoType: "map[string]int32", ArrowType: "arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32)", ArrowBuilder: "*array.MapBuilder",
								IsMap: true, ArrowArrayType: "*array.Map", ZeroExpr: "nil",
								KeyInfo: strKey,
								EltInfo: &gencommon.FieldInfo{GoType: "int32", ArrowType: "arrow.PrimitiveTypes.Int32", ArrowBuilder: "*array.Int32Builder", CastType: "int32", ArrowArrayType: "*array.Int32", ValueMethod: "Value", ZeroExpr: "0"},
							},
						},
						{
							Name: "ListVal", GoType: "map[string][]int32",
							ArrowType: "arrow.MapOf(arrow.BinaryTypes.String, arrow.ListOf(arrow.PrimitiveTypes.Int32))", ArrowBuilder: "*array.MapBuilder",
							IsMap: true, ArrowArrayType: "*array.Map", ZeroExpr: "nil",
							KeyInfo: strKey,
							EltInfo: &gencommon.FieldInfo{
								GoType: "[]int32", ArrowType: "arrow.ListOf(arrow.PrimitiveTypes.Int32)", ArrowBuilder: "*array.ListBuilder",
								IsList: true, ArrowArrayType: "*array.List", ZeroExpr: "nil",
								EltInfo: &gencommon.FieldInfo{GoType: "int32", ArrowType: "arrow.PrimitiveTypes.Int32", ArrowBuilder: "*array.Int32Builder", CastType: "int32", ArrowArrayType: "*array.Int32", ValueMethod: "Value", ZeroExpr: "0"},
							},
						},
					},
				}}, structs)
			},
		},
		{
			// struct-fields discovers multiple structs (Outer + Inner via queue)
			// and verifies cross-struct relationships using targeted assertions.
			name: "struct-fields",
			goCode: `package testpkg

type Inner struct {
	Value int32
	Label string
}

type Outer struct {
	ID     int32
	Child  Inner
	PChild *Inner
}
`,
			targets: []string{"Outer"},
			check: func(t *testing.T, _, _ string, structs []gencommon.StructInfo) {
				t.Helper()
				if len(structs) != 2 {
					t.Fatalf("expected 2 structs (Outer + Inner), got %d", len(structs))
				}
				var outer, inner gencommon.StructInfo
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
				if len(outer.Fields) != 3 {
					t.Fatalf("expected 3 fields in Outer, got %d", len(outer.Fields))
				}
				if outer.Fields[0].Name != "ID" || outer.Fields[0].GoType != "int32" {
					t.Errorf("Outer.Fields[0]: got %s %s, want ID int32", outer.Fields[0].Name, outer.Fields[0].GoType)
				}
				child := outer.Fields[1]
				if child.Name != "Child" || !child.IsStruct || child.IsPointer || child.StructName != "Inner" || child.ArrowArrayType != "*array.Struct" {
					t.Errorf("Outer.Fields[1] (Child): got name=%s isStruct=%v isPtr=%v structName=%s arrayType=%s",
						child.Name, child.IsStruct, child.IsPointer, child.StructName, child.ArrowArrayType)
				}
				pchild := outer.Fields[2]
				if pchild.Name != "PChild" || !pchild.IsStruct || !pchild.IsPointer || pchild.StructName != "Inner" || pchild.ArrowArrayType != "*array.Struct" {
					t.Errorf("Outer.Fields[2] (PChild): got name=%s isStruct=%v isPtr=%v structName=%s arrayType=%s",
						pchild.Name, pchild.IsStruct, pchild.IsPointer, pchild.StructName, pchild.ArrowArrayType)
				}
				if len(inner.Fields) != 2 {
					t.Fatalf("expected 2 fields in Inner, got %d", len(inner.Fields))
				}
				if inner.Fields[0].Name != "Value" || inner.Fields[0].GoType != "int32" {
					t.Errorf("Inner.Fields[0]: got %s %s, want Value int32", inner.Fields[0].Name, inner.Fields[0].GoType)
				}
				if inner.Fields[1].Name != "Label" || inner.Fields[1].GoType != "string" {
					t.Errorf("Inner.Fields[1]: got %s %s, want Label string", inner.Fields[1].Name, inner.Fields[1].GoType)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			if err := os.WriteFile(filepath.Join(tmpDir, "test_structs.go"), []byte(tt.goCode), 0644); err != nil {
				t.Fatalf("write test_structs.go: %v", err)
			}
			if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module testpkg\n\ngo 1.25.0\n"), 0644); err != nil {
				t.Fatalf("write go.mod: %v", err)
			}
			g := NewGenerator([]string{tmpDir}, tt.targets, "out.go", false, nil)
			pkgName, pkgPath, structs, err := g.Parse()
			if err != nil {
				t.Fatalf("Parse() failed: %v", err)
			}
			tt.check(t, pkgName, pkgPath, structs)
		})
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

// TestGenerator_RunOutput_DoublePointerExternalTypes verifies that **T fields where T is
// an external type implementing TextMarshaler or Stringer are handled correctly. The
// resolver fix (clearing MarshalMethod on the outer **T FieldInfo) routes these fields
// through the IsPointer+EltInfo template path, which dereferences before calling the
// unmarshal method on the inner *T.
func TestGenerator_RunOutput_DoublePointerExternalTypes(t *testing.T) {
	tests := []struct {
		name           string
		goCode         string
		targetStruct   string
		mustContain    []string
		mustNotContain []string
	}{
		{
			// **netip.Addr implements encoding.TextMarshaler (via *netip.Addr); the reader
			// must unmarshal via UnmarshalText after one level of pointer allocation.
			name: "double-pointer-text-marshaler",
			goCode: `package mypkg

import "net/netip"

type OptionalAddr struct {
	Addr **netip.Addr
}
`,
			targetStruct: "OptionalAddr",
			mustContain: []string{
				"colAddr",
				"UnmarshalText",
				"IsNull",
			},
			// Must not call any marshal method directly on the outer **netip.Addr.
			mustNotContain: []string{"r.colAddr.MarshalText()", "r.colAddr.UnmarshalText("},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			if err := os.WriteFile(filepath.Join(tmpDir, "test.go"), []byte(tt.goCode), 0644); err != nil {
				t.Fatalf("write test file: %v", err)
			}
			if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module mypkg\n\ngo 1.25.0\n"), 0644); err != nil {
				t.Fatalf("write go.mod: %v", err)
			}

			outPath := filepath.Join(tmpDir, "out_reader.go")
			g := NewGenerator([]string{tmpDir}, []string{tt.targetStruct}, outPath, false, nil)
			if err := g.Run(""); err != nil {
				t.Fatalf("Run() failed: %v", err)
			}

			outBytes, err := os.ReadFile(outPath)
			if err != nil {
				t.Fatalf("read output: %v", err)
			}
			outStr := string(outBytes)

			for _, want := range tt.mustContain {
				if !strings.Contains(outStr, want) {
					t.Errorf("expected output to contain %q\n--- output ---\n%s", want, outStr)
				}
			}
			for _, unwanted := range tt.mustNotContain {
				if strings.Contains(outStr, unwanted) {
					t.Errorf("expected output NOT to contain %q\n--- output ---\n%s", unwanted, outStr)
				}
			}
		})
	}
}

// TestGenerator_ElidesExistingReaders verifies that when a companion .go file in
// the output directory already declares a reader constructor (e.g. NewInnerArrowReader),
// the generator suppresses re-declaration and emits an elision comment block.
// The target struct (Outer) is still fully generated.
// TestGenerator_ReaderElision covers the three elision scenarios for reader-gen:
// companion-file elision, no-self-elide on re-generation, and empty-dir baseline.
func TestGenerator_ReaderElision(t *testing.T) {
	innerOuterSrc := `package mypkg

type Inner struct {
	X int32
}

type Outer struct {
	ID    int32
	Child Inner
}
`
	targetSrc := `package mypkg

type Target struct {
	ID int32
}
`

	tests := []struct {
		name           string
		sourceCode     string
		targets        []string
		companion      string // content of companion.go; empty = no companion
		priorOutput    string // content written to outPath before Run; empty = no prior
		wantContains   []string
		wantNotContain []string
	}{
		{
			name:       "elides-companion-reader",
			sourceCode: innerOuterSrc,
			targets:    []string{"Outer"},
			companion:  "package mypkg\n\nfunc NewInnerArrowReader() interface{} { return nil }\n",
			wantContains: []string{
				"func NewOuterArrowReader(",
				"Schema helpers elided",
				"companion.go",
			},
			wantNotContain: []string{
				"func NewInnerArrowReader(", // must be elided
			},
		},
		{
			name:        "no-self-elide-on-regenerate",
			sourceCode:  targetSrc,
			targets:     []string{"Target"},
			priorOutput: "package mypkg\n\nfunc NewTargetArrowReader() interface{} { return nil }\n",
			wantContains: []string{
				"func NewTargetArrowReader(", // must not be elided
			},
			wantNotContain: []string{
				"Schema helpers elided",
			},
		},
		{
			name:       "empty-dir-no-elision",
			sourceCode: innerOuterSrc,
			targets:    []string{"Outer"},
			wantContains: []string{
				"func NewOuterArrowReader(",
				"func NewInnerArrowReader(",
			},
			wantNotContain: []string{
				"Schema helpers elided",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			if err := os.WriteFile(filepath.Join(tmpDir, "structs.go"), []byte(tt.sourceCode), 0644); err != nil {
				t.Fatalf("write structs.go: %v", err)
			}
			if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module mypkg\n\ngo 1.25.0\n"), 0644); err != nil {
				t.Fatalf("write go.mod: %v", err)
			}
			if tt.companion != "" {
				if err := os.WriteFile(filepath.Join(tmpDir, "companion.go"), []byte(tt.companion), 0644); err != nil {
					t.Fatalf("write companion.go: %v", err)
				}
			}
			outPath := filepath.Join(tmpDir, "out_reader.go")
			if tt.priorOutput != "" {
				if err := os.WriteFile(outPath, []byte(tt.priorOutput), 0644); err != nil {
					t.Fatalf("write prior output: %v", err)
				}
			}
			g := NewGenerator([]string{tmpDir}, tt.targets, outPath, false, nil)
			if err := g.Run(""); err != nil {
				t.Fatalf("Run() failed: %v", err)
			}
			outBytes, err := os.ReadFile(outPath)
			if err != nil {
				t.Fatalf("read output: %v", err)
			}
			outStr := string(outBytes)
			for _, want := range tt.wantContains {
				if !strings.Contains(outStr, want) {
					t.Errorf("want %q in output, not found\n%s", want, outStr)
				}
			}
			for _, notWant := range tt.wantNotContain {
				if strings.Contains(outStr, notWant) {
					t.Errorf("want %q absent from output, but found it\n%s", notWant, outStr)
				}
			}
		})
	}
}
