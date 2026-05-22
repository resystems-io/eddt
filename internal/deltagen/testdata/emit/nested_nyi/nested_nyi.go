// Package nested_nyi provides a Snapshot fixture with delta.nested on a slice
// field, used by TestEmitTemplate_NestedNYI_SliceSentinel to verify that the
// N-04 sentinel is returned. The map shape (N-03) is now implemented and lives
// in testdata/emit/nested_map/.
package nested_nyi

import eddt "go.resystems.io/eddt/runtime"

// Sub is a helper type used as the element type for the slice nested field.
type Sub struct{ X, Y int32 }

// NestedSliceNYISnapshot carries a delta.nested slice field to trigger the
// N-04 sentinel (slice compositional emission is not yet implemented).
type NestedSliceNYISnapshot struct {
	eddt.Header
	Key  string `eddt:"entity.key"`
	Subs []Sub  `eddt:"delta.nested"`
}
