// write.go implements the shared format-and-write output helper used by
// both the primary and standalone emit paths.
package deltagen

import (
	"bytes"
	"fmt"
	"go/format"
	"os"
)

// writeFormattedGo formats buf as a Go source file and writes it to outPath
// with mode 0644. If the source cannot be formatted (e.g. a template bug
// produces invalid Go), the raw source is included in the error for diagnosis.
func writeFormattedGo(outPath string, buf *bytes.Buffer) error {
	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		return fmt.Errorf(
			"delta-gen: generated source is not valid Go: %w\n--- raw source ---\n%s",
			err, buf.String())
	}
	if err := os.WriteFile(outPath, formatted, 0644); err != nil {
		return fmt.Errorf("delta-gen: writing output file %q: %w", outPath, err)
	}
	return nil
}
