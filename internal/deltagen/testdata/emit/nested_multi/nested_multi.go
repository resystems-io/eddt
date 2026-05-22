// Package nested_multi is the N-01 deduplication fixture: two delta.nested
// fields of the same type (Address) and one of a different type (Meta).
// AddressDelta must be emitted exactly once even though two fields share it.
package nested_multi

import eddt "go.resystems.io/eddt/runtime"

// Address holds street and city for a location.
type Address struct {
	Street string
	City   string
}

// Meta holds version metadata.
type Meta struct {
	Version int32
}

// NestedMultiSnapshot is the root Snapshot used by TestEmitTemplate_Nested_Dedup.
type NestedMultiSnapshot struct {
	eddt.Header
	Key    string  `eddt:"entity.key"`
	Home   Address `eddt:"delta.nested"`
	Work   Address `eddt:"delta.nested"` // same type as Home → AddressDelta emitted once
	Info   Meta    `eddt:"delta.nested"`
	Status int32
}
