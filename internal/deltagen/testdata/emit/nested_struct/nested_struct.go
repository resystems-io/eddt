// Package nested_struct is the primary N-01 fixture: a Snapshot with a single
// delta.nested struct-value field and one sibling atomic field.
package nested_struct

import eddt "go.resystems.io/eddt/runtime"

// Inner is the nested payload type. It carries two scalar fields.
type Inner struct {
	X int32
	Y string
}

// NestedStructSnapshot is the root Snapshot used by TestEmitTemplate_Nested_SamePkg.
type NestedStructSnapshot struct {
	eddt.Header
	Key   string `eddt:"entity.key"`
	Sub   Inner  `eddt:"delta.nested"`
	Label string
}
