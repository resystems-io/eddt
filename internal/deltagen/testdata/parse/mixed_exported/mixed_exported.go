// Package mixed_exported provides a Snapshot fixture with a mix of exported
// and unexported payload fields, used to exercise cross-package unexported-field
// filtering in G-03 parse tests.
package mixed_exported

import eddt "go.resystems.io/eddt/runtime"

// MixedSnapshot has two exported and one unexported payload field. When parsed
// with crossPackage=true the unexported field must be absent from the result.
type MixedSnapshot struct {
	eddt.Header
	ExportedID    int32
	internalCode  string //nolint:unused
	ExportedLabel string
}
