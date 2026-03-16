package gencommon

import (
	"go/ast"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestPrimitiveArrowType(t *testing.T) {
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
			gotGo, gotArr, gotBld, _, err := primitiveArrowType(ident)
			if (err != nil) != tt.expectErr {
				t.Errorf("primitiveArrowType(%s) error = %v, expectErr %v", tt.goType, err, tt.expectErr)
				return
			}
			if err == nil {
				if gotGo != tt.expectedGo {
					t.Errorf("primitiveArrowType(%s) gotGo = %v, want %v", tt.goType, gotGo, tt.expectedGo)
				}
				if gotArr != tt.expectedArr {
					t.Errorf("primitiveArrowType(%s) gotArr = %v, want %v", tt.goType, gotArr, tt.expectedArr)
				}
				if gotBld != tt.expectedBld {
					t.Errorf("primitiveArrowType(%s) gotBld = %v, want %v", tt.goType, gotBld, tt.expectedBld)
				}
			}
		})
	}
}

func TestArrowArrayType(t *testing.T) {
	tests := []struct {
		builder  string
		expected string
	}{
		{"*array.Int32Builder", "*array.Int32"},
		{"*array.StringBuilder", "*array.String"},
		{"*array.BooleanBuilder", "*array.Boolean"},
		{"*array.ListBuilder", "*array.List"},
		{"*array.MapBuilder", "*array.Map"},
		{"*array.StructBuilder", "*array.Struct"},
		{"*array.BinaryBuilder", "*array.Binary"},
		{"*array.TimestampBuilder", "*array.Timestamp"},
		{"*array.FixedSizeListBuilder", "*array.FixedSizeList"},
	}
	for _, tt := range tests {
		t.Run(tt.builder, func(t *testing.T) {
			got := arrowArrayType(tt.builder)
			if got != tt.expected {
				t.Errorf("arrowArrayType(%q) = %q, want %q", tt.builder, got, tt.expected)
			}
		})
	}
}

func TestUnmarshalForMarshal(t *testing.T) {
	tests := []struct {
		marshal   string
		unmarshal string
	}{
		{"MarshalText", "UnmarshalText"},
		{"MarshalBinary", "UnmarshalBinary"},
		{"String", ""},
		{"", ""},
	}
	for _, tt := range tests {
		t.Run(tt.marshal, func(t *testing.T) {
			got := unmarshalForMarshal(tt.marshal)
			if got != tt.unmarshal {
				t.Errorf("unmarshalForMarshal(%q) = %q, want %q", tt.marshal, got, tt.unmarshal)
			}
		})
	}
}

func TestZeroExprForCast(t *testing.T) {
	tests := []struct {
		castType string
		expected string
	}{
		{"int32", "0"},
		{"int64", "0"},
		{"uint8", "0"},
		{"float64", "0"},
		{"string", `""`},
		{"bool", "false"},
		{"[]byte", "nil"},
		{"arrow.Timestamp", "0"},
	}
	for _, tt := range tests {
		t.Run(tt.castType, func(t *testing.T) {
			got := zeroExprForCast(tt.castType)
			if got != tt.expected {
				t.Errorf("zeroExprForCast(%q) = %q, want %q", tt.castType, got, tt.expected)
			}
		})
	}
}

// TestReaderFieldsPopulated verifies that the reader-specific FieldInfo fields
// (ArrowArrayType, ValueMethod, UnmarshalMethod, ConvertBackExpr, ZeroExpr)
// are correctly populated during parsing for various type categories.
func TestReaderFieldsPopulated(t *testing.T) {
	tmpDir := t.TempDir()
	testCode := `package testpkg

type Inner struct {
	X int32
}

type ReaderFieldsStruct struct {
	// Primitives
	ID    int32
	Name  string
	Valid bool

	// Pointer to primitive
	OptID *int32

	// Containers
	Tags   []string
	Scores map[string]float64
	Matrix [3]int32
	Data   []byte

	// Nested struct
	Inner Inner

	// Pointer to struct
	OptInner *Inner
}
`
	if err := os.WriteFile(filepath.Join(tmpDir, "types.go"), []byte(testCode), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module testpkg\n\ngo 1.25.0\n"), 0644); err != nil {
		t.Fatal(err)
	}

	_, _, structs, err := Parse([]string{tmpDir}, []string{"ReaderFieldsStruct"}, false)
	if err != nil {
		t.Fatalf("Parse() failed: %v", err)
	}

	// Find our target struct (may also contain Inner).
	var target *StructInfo
	for i := range structs {
		if structs[i].Name == "ReaderFieldsStruct" {
			target = &structs[i]
			break
		}
	}
	if target == nil {
		t.Fatal("ReaderFieldsStruct not found")
	}

	// Build a lookup by field name.
	byName := map[string]FieldInfo{}
	for _, f := range target.Fields {
		byName[f.Name] = f
	}

	tests := []struct {
		field           string
		arrowArrayType  string
		valueMethod     string
		unmarshalMethod string
		convertBackExpr string
		zeroExpr        string
	}{
		{"ID", "*array.Int32", "Value", "", "", "0"},
		{"Name", "*array.String", "Value", "", "", `""`},
		{"Valid", "*array.Boolean", "Value", "", "", "false"},
		{"OptID", "*array.Int32", "Value", "", "", "0"},
		{"Tags", "*array.List", "", "", "", "nil"},
		{"Scores", "*array.Map", "", "", "", "nil"},
		{"Matrix", "*array.FixedSizeList", "", "", "", "[3]int32{}"},
		{"Data", "*array.Binary", "Value", "", "", "nil"},
		{"Inner", "*array.Struct", "", "", "", "Inner{}"},
		{"OptInner", "*array.Struct", "", "", "", "nil"},
	}

	for _, tt := range tests {
		t.Run(tt.field, func(t *testing.T) {
			fi, ok := byName[tt.field]
			if !ok {
				t.Fatalf("field %q not found", tt.field)
			}
			if diff := cmp.Diff(tt.arrowArrayType, fi.ArrowArrayType); diff != "" {
				t.Errorf("ArrowArrayType mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tt.valueMethod, fi.ValueMethod); diff != "" {
				t.Errorf("ValueMethod mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tt.unmarshalMethod, fi.UnmarshalMethod); diff != "" {
				t.Errorf("UnmarshalMethod mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tt.convertBackExpr, fi.ConvertBackExpr); diff != "" {
				t.Errorf("ConvertBackExpr mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tt.zeroExpr, fi.ZeroExpr); diff != "" {
				t.Errorf("ZeroExpr mismatch (-want +got):\n%s", diff)
			}
		})
	}

	// Verify recursive reader fields on container elements.
	tagsElt := byName["Tags"].EltInfo
	if tagsElt == nil {
		t.Fatal("Tags.EltInfo is nil")
	}
	if tagsElt.ArrowArrayType != "*array.String" {
		t.Errorf("Tags.EltInfo.ArrowArrayType = %q, want %q", tagsElt.ArrowArrayType, "*array.String")
	}
	if tagsElt.ValueMethod != "Value" {
		t.Errorf("Tags.EltInfo.ValueMethod = %q, want %q", tagsElt.ValueMethod, "Value")
	}
}
