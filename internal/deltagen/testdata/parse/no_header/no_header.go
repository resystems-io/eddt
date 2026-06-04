// Package no_header provides a plain struct with no embedded runtime.Header,
// used to exercise the "missing Header" error path in parse tests (R-DG-001–R-DG-003).
package no_header

// PlainStruct has no embedded runtime.Header and therefore cannot be a
// conforming EDDT Snapshot type. The parser must reject it.
type PlainStruct struct {
	ID   int32
	Name string
}
