// Package nested_triple is the N-02 three-level nesting fixture. Snapshot →
// Level1 → Level2 → Level3 exercises arbitrary-depth compositional emission.
// Used by TestEmitTemplate_Nested_Triple.
package nested_triple

import eddt "go.resystems.io/eddt/runtime"

// Level3 is the innermost nested type.
type Level3 struct {
	Score int32
}

// Level2 nests Level3 via delta.nested.
type Level2 struct {
	Rank  int32
	Stats Level3 `eddt:"delta.nested"`
}

// Level1 nests Level2 via delta.nested.
type Level1 struct {
	Count int32
	Meta  Level2 `eddt:"delta.nested"`
}

// NestedTripleSnapshot is the root Snapshot used by TestEmitTemplate_Nested_Triple.
type NestedTripleSnapshot struct {
	eddt.Header
	Key  string `eddt:"entity.key"`
	Root Level1 `eddt:"delta.nested"`
	Name string
}
