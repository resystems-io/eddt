// Package scalar_only provides a Snapshot fixture with only scalar and
// suppressed fields — no pointer, struct, slice, or map shapes. It is used by
// TestEmitTemplate_NoReflectImport_AllScalar to verify that the "reflect"
// import is NOT injected when no reflect.DeepEqual comparisons are needed.
package scalar_only

import eddt "go.resystems.io/eddt/runtime"

// ScalarOnlySnapshot has one entity-key field, two scalar payload fields,
// one commutative scalar, and one suppressed field. No non-scalar shape is
// present, so the generated Diff should not import "reflect".
type ScalarOnlySnapshot struct {
	eddt.Header
	Key     string `eddt:"entity.key"`
	Count   int32
	Label   string
	Rate    float64 `eddt:"delta.commutative"`
	Omitted string  `eddt:"delta.omit"`
}
