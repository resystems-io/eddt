// Package mixed_exported provides a Snapshot fixture with a mix of exported
// and unexported payload fields, used to exercise cross-package unexported-field
// filtering in parse tests (R-DG-001–R-DG-003).
package mixed_exported

import eddt "go.resystems.io/eddt/runtime"

// MixedKey is the entity-key struct for MixedSnapshot. It is exported, so the
// key is visible to the parser (R-DG-010) in both same-package and cross-package modes.
type MixedKey struct{ Code string }

// MixedSnapshot has two exported and one unexported payload field plus an
// exported entity.key field. When parsed with crossPackage=true the unexported
// field must be absent from the result. R-DG-010 surfaces Key via KeyVar in both
// modes, leaving the same payload-field counts the parse tests assert:
// three fields same-package, two fields cross-package.
type MixedSnapshot struct {
	eddt.Header
	Key           MixedKey `eddt:"entity.key"`
	ExportedID    int32
	internalCode  string //nolint:unused
	ExportedLabel string
}
