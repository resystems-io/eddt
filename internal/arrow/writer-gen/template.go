package writergen

import (
	_ "embed"
	"text/template"

	"go.resystems.io/eddt/internal/arrow/gencommon"
)

// writerTemplateFS is the Go text/template used to generate Arrow append writers,
// loaded from the sibling writer.go.tmpl file via //go:embed.
//
// Template structure:
//
//   - Main template ("writer"): Generates the public API per struct — schema constructor,
//     ArrowWriter struct, New/Release/Append/NewRecordBatch methods, and the AppendStruct helper.
//   - Sub-template ("appendFields"): Shared field-handling logic used by both the Append
//     method (which accesses builders via w.b.Field) and AppendStruct (via b.FieldBuilder).
//     The builder accessor is passed as the "Bldr" key in a dict.
//
// A "dict" FuncMap helper is registered to pass multiple named values to sub-templates,
// since Go's text/template only supports a single pipeline argument to {{template}}.
//
// Template data is provided via the templateData struct. When the output package differs
// from the input package(s), Imports is populated with the required import paths and aliases,
// causing the template to emit import statements and qualify struct type references with the
// appropriate package qualifier (stored on each StructInfo as Qualifier).
//
//go:embed writer.go.tmpl
var writerTemplateFS string
var writerTemplate = template.Must(template.New("writer").Funcs(gencommon.TemplateFuncs()).Parse(writerTemplateFS))
