// Package nested_deep is the N-01 two-level nesting fixture. Level1 carries a
// delta.nested Level2 sub-field, and the root Snapshot carries a delta.nested
// Level1 field. Both Level2Delta and Level1Delta must be emitted.
package nested_deep

import eddt "go.resystems.io/eddt/runtime"

// Level2 is the innermost nested type.
type Level2 struct {
	Val int32
}

// Level1 nests Level2 via delta.nested.
type Level1 struct {
	Count int32
	Sub   Level2 `eddt:"delta.nested"`
}

// NestedDeepSnapshot is the root Snapshot used by TestEmitTemplate_Nested_Deep.
type NestedDeepSnapshot struct {
	eddt.Header
	Key   string `eddt:"entity.key"`
	Inner Level1 `eddt:"delta.nested"`
	Name  string
}
