// Package nested_nyi provides Snapshot fixtures with delta.nested on slice and
// map fields, used by TestEmitTemplate_NestedNYI_SliceSentinel and
// TestEmitTemplate_NestedNYI_MapSentinel to verify that N-03/N-04 sentinels
// are still returned for those shapes.
package nested_nyi

import eddt "go.resystems.io/eddt/runtime"

// Sub is a helper type used as the element type for slice/map nested fields.
type Sub struct{ X, Y int32 }

// NestedSliceNYISnapshot carries a delta.nested slice field to trigger N-03.
type NestedSliceNYISnapshot struct {
	eddt.Header
	Key  string `eddt:"entity.key"`
	Subs []Sub  `eddt:"delta.nested"`
}

// NestedMapNYISnapshot carries a delta.nested map field to trigger N-03/N-04.
type NestedMapNYISnapshot struct {
	eddt.Header
	Key string         `eddt:"entity.key"`
	Map map[string]Sub `eddt:"delta.nested"`
}
