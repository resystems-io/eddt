// Package nested_ok provides Snapshot fixtures with delta.nested applied to
// each composite shape (struct value, slice, map). Each fixture is parsed by
// T-02 tests to verify the harmonised granularity-axis gate admits composite
// shapes.
package nested_ok

import eddt "go.resystems.io/eddt/runtime"

// Inner is the nested struct used by NestedStructSnap and as the element type
// for NestedSliceSnap and NestedMapSnap (so the same Inner type covers all
// three composite-shape cases).
type Inner struct{ A, B int }

// NestedStructSnap tags a struct-value payload field with delta.nested.
// Expected: T-02 admits; ParsedField.Tag.Kind = TagKindNested.
type NestedStructSnap struct {
	eddt.Header
	Key string `eddt:"entity.key"`
	Sub Inner  `eddt:"delta.nested"`
}

// NestedSliceSnap tags a slice payload field with delta.nested.
// Expected: T-02 admits; ParsedField.Tag.Kind = TagKindNested.
type NestedSliceSnap struct {
	eddt.Header
	Key   string  `eddt:"entity.key"`
	Items []Inner `eddt:"delta.nested"`
}

// NestedMapSnap tags a map payload field with delta.nested.
// Expected: T-02 admits; ParsedField.Tag.Kind = TagKindNested.
type NestedMapSnap struct {
	eddt.Header
	Key  string           `eddt:"entity.key"`
	Tags map[string]Inner `eddt:"delta.nested"`
}
