package gencommon

import (
	"bytes"
	"fmt"
	"go/format"
	"os"
)

// WriteFormattedGo formats buf as a Go source file and writes it to outPath
// with mode 0644. If the source cannot be formatted (syntax error in generated
// code) the raw source is included in the error for diagnosis.
//
// This is the shared output tail used by both writer-gen and reader-gen to
// avoid duplicating the format→write pipeline.
func WriteFormattedGo(outPath string, buf *bytes.Buffer) error {
	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		return fmt.Errorf("failed to format generated source: %w\nSource:\n%s", err, buf.String())
	}
	if err := os.WriteFile(outPath, formatted, 0644); err != nil {
		return fmt.Errorf("failed to write output file: %w", err)
	}
	return nil
}

// MergeImports appends imports from add into dst, skipping any whose Path is
// already present in dst. Order within add is preserved; the first occurrence
// of a given Path wins. The returned slice is dst (possibly extended).
//
// Import order in the returned slice affects the generated import block, which
// in turn affects go/format's output. Callers that need deterministic output
// should call MergeImports in a stable sequence.
func MergeImports(dst, add []ImportInfo) []ImportInfo {
	if len(add) == 0 {
		return dst
	}
	seen := make(map[string]bool, len(dst))
	for _, imp := range dst {
		seen[imp.Path] = true
	}
	for _, imp := range add {
		if !seen[imp.Path] {
			dst = append(dst, imp)
			seen[imp.Path] = true
		}
	}
	return dst
}
