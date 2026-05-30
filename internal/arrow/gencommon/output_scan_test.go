package gencommon

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

// writeGoFile creates a .go file with the given content in dir. The content
// must be a valid Go source file (package clause required).
func writeGoFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("writeGoFile %s: %v", name, err)
	}
	return path
}

func TestScanOutputPackageSchemas(t *testing.T) {
	tests := []struct {
		name        string
		files       map[string]string // filename → Go source
		excludeFile string            // file to exclude from scan (basename)
		funcSuffix  string
		want        map[string]string // funcName → declaring file basename
	}{
		{
			name:        "empty-dir",
			files:       nil,
			excludeFile: "out.go",
			funcSuffix:  "Schema",
			want:        map[string]string{},
		},
		{
			name: "finds-schema-func",
			files: map[string]string{
				"companion.go": `package mypkg
func NewHeaderSchema() {}
func NewItemSchema() {}
`,
			},
			excludeFile: "out.go",
			funcSuffix:  "Schema",
			want: map[string]string{
				"NewHeaderSchema": "companion.go",
				"NewItemSchema":   "companion.go",
			},
		},
		{
			name: "suffix-discrimination",
			files: map[string]string{
				"companion.go": `package mypkg
func NewHeaderSchema() {}
func NewHeaderArrowReader() {}
`,
			},
			excludeFile: "out.go",
			funcSuffix:  "ArrowReader",
			want:        map[string]string{"NewHeaderArrowReader": "companion.go"},
		},
		{
			name: "excludes-output-file",
			files: map[string]string{
				"out.go": `package mypkg
func NewHeaderSchema() {}
`,
				"companion.go": `package mypkg
func NewItemSchema() {}
`,
			},
			excludeFile: "out.go",
			funcSuffix:  "Schema",
			want:        map[string]string{"NewItemSchema": "companion.go"},
		},
		{
			name: "skips-methods",
			files: map[string]string{
				"companion.go": `package mypkg
type T struct{}
func (T) NewHeaderSchema() {}
func NewItemSchema() {}
`,
			},
			excludeFile: "out.go",
			funcSuffix:  "Schema",
			want:        map[string]string{"NewItemSchema": "companion.go"},
		},
		{
			name: "skips-non-new-prefix",
			files: map[string]string{
				"companion.go": `package mypkg
func BuildHeaderSchema() {}
func NewItemSchema() {}
`,
			},
			excludeFile: "out.go",
			funcSuffix:  "Schema",
			want:        map[string]string{"NewItemSchema": "companion.go"},
		},
		{
			name: "multiple-files",
			files: map[string]string{
				"writer_a.go": `package mypkg
func NewAlphaSchema() {}
`,
				"writer_b.go": `package mypkg
func NewBetaSchema() {}
`,
			},
			excludeFile: "out.go",
			funcSuffix:  "Schema",
			want: map[string]string{
				"NewAlphaSchema": "writer_a.go",
				"NewBetaSchema":  "writer_b.go",
			},
		},
		{
			name: "syntax-error-file-skipped",
			files: map[string]string{
				"broken.go": `package mypkg
this is not valid Go {{{`,
				"good.go": `package mypkg
func NewGoodSchema() {}
`,
			},
			excludeFile: "out.go",
			funcSuffix:  "Schema",
			want:        map[string]string{"NewGoodSchema": "good.go"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			for name, src := range tt.files {
				writeGoFile(t, dir, name, src)
			}
			excludePath := filepath.Join(dir, tt.excludeFile)

			got, err := ScanOutputPackageSchemas(dir, excludePath, tt.funcSuffix)
			if err != nil {
				t.Fatalf("ScanOutputPackageSchemas: %v", err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("ScanOutputPackageSchemas() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPartitionByExistingSchemas(t *testing.T) {
	mkStruct := func(name string, fields ...FieldInfo) StructInfo {
		return StructInfo{Name: name, Fields: fields}
	}
	mkField := func(name, arrowType string) FieldInfo {
		return FieldInfo{Name: name, ArrowType: arrowType}
	}

	tests := []struct {
		name       string
		structs    []StructInfo
		existing   map[string]string
		funcSuffix string
		wantKept   []StructInfo
		wantElided []ElidedSchema
	}{
		{
			name:       "empty-structs",
			structs:    nil,
			existing:   map[string]string{"NewFooSchema": "companion.go"},
			funcSuffix: "Schema",
			wantKept:   nil,
			wantElided: nil,
		},
		{
			name:       "no-existing",
			structs:    []StructInfo{mkStruct("Foo"), mkStruct("Bar")},
			existing:   map[string]string{},
			funcSuffix: "Schema",
			wantKept:   []StructInfo{mkStruct("Foo"), mkStruct("Bar")},
			wantElided: nil,
		},
		{
			name:       "all-elided",
			structs:    []StructInfo{mkStruct("Foo"), mkStruct("Bar")},
			existing:   map[string]string{"NewFooSchema": "c.go", "NewBarSchema": "c.go"},
			funcSuffix: "Schema",
			wantKept:   nil,
			wantElided: []ElidedSchema{
				{FuncName: "NewFooSchema", DeclaredIn: "c.go", FieldSummary: ""},
				{FuncName: "NewBarSchema", DeclaredIn: "c.go", FieldSummary: ""},
			},
		},
		{
			name: "mixed",
			structs: []StructInfo{
				mkStruct("Kept"),
				mkStruct("Elided"),
			},
			existing:   map[string]string{"NewElidedSchema": "companion.go"},
			funcSuffix: "Schema",
			wantKept:   []StructInfo{mkStruct("Kept")},
			wantElided: []ElidedSchema{
				{FuncName: "NewElidedSchema", DeclaredIn: "companion.go", FieldSummary: ""},
			},
		},
		{
			name: "field-summary-in-elided",
			structs: []StructInfo{
				mkStruct("Header",
					mkField("ID", "arrow.BinaryTypes.Binary"),
					mkField("Seq", "arrow.PrimitiveTypes.Int64"),
				),
			},
			existing:   map[string]string{"NewHeaderSchema": "snap.go"},
			funcSuffix: "Schema",
			wantKept:   nil,
			wantElided: []ElidedSchema{
				{
					FuncName:     "NewHeaderSchema",
					DeclaredIn:   "snap.go",
					FieldSummary: "ID (Binary), Seq (Int64)",
				},
			},
		},
		{
			name: "arrow-reader-suffix",
			structs: []StructInfo{
				mkStruct("Snap"),
				mkStruct("Delta"),
			},
			existing:   map[string]string{"NewSnapArrowReader": "snap_reader.go"},
			funcSuffix: "ArrowReader",
			wantKept:   []StructInfo{mkStruct("Delta")},
			wantElided: []ElidedSchema{
				{FuncName: "NewSnapArrowReader", DeclaredIn: "snap_reader.go", FieldSummary: ""},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotKept, gotElided := PartitionByExistingSchemas(tt.structs, tt.existing, tt.funcSuffix)

			if diff := cmp.Diff(tt.wantKept, gotKept, cmpopts.EquateEmpty(), cmpopts.IgnoreUnexported(StructInfo{})); diff != "" {
				t.Errorf("kept mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tt.wantElided, gotElided, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("elided mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
