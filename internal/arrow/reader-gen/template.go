package readergen

import (
	_ "embed"
	"strings"
	"text/template"

	"go.resystems.io/eddt/internal/arrow/gencommon"
)

// readerTemplateFS is the Go text/template used to generate Arrow readers,
// loaded from the sibling reader.go.tmpl file via //go:embed.
//
// Template structure:
//
//   - Main template ("reader"): Generates per-struct reader struct, New constructor
//     (validate-at-init with column downcast), and LoadRow method.
//   - Sub-template ("loadField"): Emits the per-field load logic inside LoadRow.
//     Leaf primitives and pointer-to-primitives are handled (ValueMethod != "",
//     no containers/structs/marshal/convert). Pointer fields use null→nil semantics
//     and dereference-assign for zero-allocation reuse.
//
// Design:
//   - Validate-at-init (R5): NewXxxArrowReader performs all type assertions. If a column
//     exists but has the wrong type, it returns an error. Once init succeeds, LoadRow is infallible.
//   - Missing columns (R11/E5): If a column isn't found in the schema, the field stays nil.
//     LoadRow wraps each read in `if col != nil`, so missing columns are silently skipped.
//   - Null handling (R10): For non-pointer value types, IsNull(i) writes the zero-value
//     expression to prevent dirty reads when reusing the output struct.
//   - Column lookup by name (R4): schema.FieldIndices supports arbitrary column ordering.
//   - Read cast uses GoType (not CastType): writer casts GoType->CastType, reader casts back.
//
//go:embed reader.go.tmpl
var readerTemplateFS string

func readerTemplateFuncs() template.FuncMap {
	m := gencommon.TemplateFuncs()
	m["stripPtr"] = func(s string) string {
		if len(s) > 0 && s[0] == '*' {
			return s[1:]
		}
		return s
	}
	m["isDictCandidate"] = func(arrowArrayType string) bool {
		return arrowArrayType == "*array.String" || arrowArrayType == "*array.Binary"
	}
	m["repeat"] = func(s string, n int) string { return strings.Repeat(s, n) }
	return m
}

var readerTemplate = template.Must(template.New("reader").Funcs(readerTemplateFuncs()).Parse(readerTemplateFS))

type templateData struct {
	PackageName        string
	Version            string
	Imports            []gencommon.ImportInfo
	Structs            []gencommon.StructInfo
	HasUnmarshalFields bool
	ElidedSchemas      []gencommon.ElidedSchema
}
