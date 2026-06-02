// Package-level conflict resolution for multi-invocation code generation.
//
// When arrow-writer-gen or arrow-reader-gen is invoked more than once for the
// same output Go package — targeting different structs in separate invocations
// — each invocation independently discovers all nested struct types reachable
// from its targets and would emit a New<Name>Schema helper for each. Because
// all output files land in the same Go package, any shared nested type produces
// a DuplicateDecl error.
//
// ScanOutputPackageSchemas and PartitionByExistingSchemas implement
// output-package-aware elision: before writing any schema helpers the generator
// scans other .go files in the output directory for already-declared
// New*Schema functions and suppresses re-declaration of any it finds.
//
// Semantics:
//
//   - The invocation that runs FIRST declares all schema helpers for shared
//     nested types.
//   - Subsequent invocations detect those declarations and elide their own
//     copies, leaving the ArrowType references intact (the compile-time
//     resolution is satisfied by the companion file).
//   - A comment block is emitted in every output file that elides schemas,
//     listing each elided function name, which companion file declares it,
//     and the expected Arrow field summary for manual compatibility verification.
//
// Limitations:
//
//   - Ordering dependency: the companion file must exist before the current
//     invocation runs. In sequential build systems (Makefile, go:generate, CI
//     pipelines) ordering is deterministic and this works reliably. Fully
//     parallel invocations writing to an empty directory simultaneously can
//     still conflict; this case is not handled.
//
//   - Signature compatibility: whether a companion file's schema body matches
//     what the current invocation would generate is NOT verified automatically.
//     The emitted field summary is the mechanism for manual verification. A
//     genuine mismatch surfaces at runtime as an Arrow schema type error.

package gencommon

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

// ElidedSchema records a struct whose schema declaration was suppressed because
// the schema function is already declared in another file in the output package.
type ElidedSchema struct {
	FuncName     string // e.g. "NewHeaderSchema"
	DeclaredIn   string // e.g. "mme_events-arrow-writer.go" (base name only)
	FieldSummary string // brief comma-separated field list for manual verification
}

// ScanOutputPackageSchemas returns a map from function name to the filename
// (base name only) that declares it, by parsing all .go files in dir except
// the file at excludePath using go/parser.ParseFile.
//
// Only top-level (non-method) functions whose names start with "New" and end
// with funcSuffix are collected. Use "Schema" for writer-gen (which generates
// NewXxxSchema helpers) or "ArrowReader" for reader-gen (which generates
// NewXxxArrowReader constructors).
//
// The parser is used rather than a text regex so that function names appearing
// inside comments or string literals are not matched, and method declarations
// (which have a receiver) are correctly excluded.
//
// Parse errors in individual files are silently skipped — a broken file is
// excluded from the scan but other files are still processed.
func ScanOutputPackageSchemas(dir, excludePath, funcSuffix string) (map[string]string, error) {
	excludeBase := filepath.Base(excludePath)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	result := map[string]string{}
	fset := token.NewFileSet()
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || e.Name() == excludeBase {
			continue
		}
		fullPath := filepath.Join(dir, e.Name())
		file, err := parser.ParseFile(fset, fullPath, nil, 0)
		if err != nil || file == nil {
			continue // skip files with syntax errors
		}
		for _, decl := range file.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Recv != nil {
				continue // skip non-functions and methods
			}
			name := fd.Name.Name
			if strings.HasPrefix(name, "New") && strings.HasSuffix(name, funcSuffix) {
				result[name] = e.Name()
			}
		}
	}
	return result, nil
}

// PartitionByExistingSchemas splits structs into those whose generated helper
// function must still be declared (kept) and those whose function is already
// present in a companion file (elided).
//
// The function name for each struct is "New" + Name + funcSuffix. Use "Schema"
// for writer-gen and "ArrowReader" for reader-gen to match each generator's
// output naming convention.
//
// Elided structs are represented as ElidedSchema records for inclusion in the
// comment block that the generator emits at the top of the output file.
func PartitionByExistingSchemas(structs []StructInfo, existing map[string]string, funcSuffix string) (kept []StructInfo, elided []ElidedSchema) {
	for _, s := range structs {
		funcName := "New" + s.Name + funcSuffix
		if file, found := existing[funcName]; found {
			elided = append(elided, ElidedSchema{
				FuncName:     funcName,
				DeclaredIn:   file,
				FieldSummary: buildFieldSummary(s.Fields),
			})
		} else {
			kept = append(kept, s)
		}
	}
	return kept, elided
}

// buildFieldSummary returns a short human-readable summary of the Arrow field
// types in a struct, e.g. "EntityID (Binary), ChainID (Utf8), Sequence (Int64)".
// It is included in ElidedSchema.FieldSummary for manual schema verification.
func buildFieldSummary(fields []FieldInfo) string {
	parts := make([]string, 0, len(fields))
	for _, f := range fields {
		arrowShort := f.ArrowType
		// Shorten common arrow type prefixes for readability.
		arrowShort = strings.TrimPrefix(arrowShort, "arrow.BinaryTypes.")
		arrowShort = strings.TrimPrefix(arrowShort, "arrow.PrimitiveTypes.")
		arrowShort = strings.TrimPrefix(arrowShort, "arrow.FixedWidthTypes.")
		parts = append(parts, f.Name+" ("+arrowShort+")")
	}
	return strings.Join(parts, ", ")
}
