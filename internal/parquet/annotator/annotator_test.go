package annotator

import (
	"bytes"
	"go/format"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v18/parquet/schema"
)

func TestAnnotator_Annotate(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		expected      string
		targetStructs []string
	}{
		{
			name: "No Existing Tags",
			input: `package test

type Person struct {
	Name string
	Age  int
}
`,
			expected: `package test

type Person struct {
	Name	string	` + "`" + `parquet:"name=Name, type=BYTE_ARRAY, logicaltype=String"` + "`" + `
	Age	int	` + "`" + `parquet:"name=Age, type=INT64"` + "`" + `
}
`,
		},
		{
			name: "Existing JSON Tags",
			input: `package test

type Product struct {
	ID    string  ` + "`json:\"product_id\"`" + `
	Price float64 ` + "`json:\"price\"`" + `
}
`,
			expected: `package test

type Product struct {
	ID	string	` + "`json:\"product_id\" parquet:\"name=product_id, type=BYTE_ARRAY, logicaltype=String\"`" + `
	Price	float64	` + "`json:\"price\" parquet:\"name=price, type=DOUBLE\"`" + `
}
`,
		},
		{
			name: "Existing Parquet Tags Are Kept",
			input: `package test

type Order struct {
	ID string ` + "`parquet:\"name=order_id, type=BYTE_ARRAY\"`" + `
}
`,
			expected: `package test

type Order struct {
	ID	string	` + "`parquet:\"name=order_id, type=BYTE_ARRAY\"`" + `
}
`,
		},
		{
			name: "Anonymous Fields and Unexported Fields",
			input: `package test

type Embedded struct {
	ID string
}

type Complex struct {
	Embedded
	private string
	Public  bool
}
`,
			expected: `package test

type Embedded struct {
	ID	string	` + "`parquet:\"name=ID, type=BYTE_ARRAY, logicaltype=String\"`" + `
}

type Complex struct {
	Embedded
	private	string
	Public	bool	` + "`parquet:\"name=Public, type=BOOLEAN\"`" + `
}
`,
		},
		{
			name:          "With TargetStructs set",
			targetStructs: []string{"User"},
			input: `package test

type User struct {
	ID string
}

type Device struct {
	ID string
}
`,
			expected: `package test

type User struct {
	ID	string	` + "`parquet:\"name=ID, type=BYTE_ARRAY, logicaltype=String\"`" + `
}

type Device struct {
	ID string
}
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := NewParquetAnnotator()
			a.TargetStructs = tt.targetStructs
			in := strings.NewReader(tt.input)
			var out bytes.Buffer

			err := a.Annotate(in, &out)
			if err != nil {
				t.Fatalf("Annotate error: %v", err)
			}

			got := out.String()

			expectedBytes, err := format.Source([]byte(tt.expected))
			if err != nil {
				t.Fatalf("Failed to format expected string: %v", err)
			}
			expected := string(expectedBytes)

			if got != expected {
				t.Errorf("\nExpected:\n%s\nGot:\n%s", expected, got)
			}
		})
	}
}

func TestAnnotator_GeneratedSchemaValid(t *testing.T) {
	type Person struct {
		Name string `parquet:"name=Name, type=BYTE_ARRAY, logicaltype=String"`
		Age  int    `parquet:"name=Age, type=INT64"`
	}

	type Product struct {
		ID    string  `json:"product_id" parquet:"name=product_id, type=BYTE_ARRAY, logicaltype=String"`
		Price float64 `json:"price" parquet:"name=price, type=DOUBLE"`
	}

	type Order struct {
		ID string `parquet:"name=order_id, type=BYTE_ARRAY"`
	}

	type Embedded struct {
		ID string `parquet:"name=ID, type=BYTE_ARRAY, logicaltype=String"`
	}

	type Complex struct {
		Embedded
		private string
		Public  bool `parquet:"name=Public, type=BOOLEAN"`
	}

	tests := []struct {
		name string
		obj  interface{}
	}{
		{"Person", Person{}},
		{"Product", Product{}},
		{"Order", Order{}},
		{"Complex", Complex{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc, err := schema.NewSchemaFromStruct(tt.obj)
			if err != nil {
				t.Fatalf("Failed to generate schema for %s: %v", tt.name, err)
			}
			if sc == nil {
				t.Fatalf("Schema for %s is nil", tt.name)
			}
			if sc.Root() == nil {
				t.Fatalf("Schema Root for %s is nil", tt.name)
			}
		})
	}
}
