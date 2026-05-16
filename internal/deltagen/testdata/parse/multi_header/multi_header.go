// Package multi_header provides a struct with two embedded runtime.Header
// fields, used to exercise the "multiple Headers" error path in G-03 parse
// tests.
package multi_header

import eddt "go.resystems.io/eddt/runtime"

// DualHeaderSnapshot embeds runtime.Header twice — once anonymously and once
// as a named field. The parser must reject this as a malformed Snapshot.
type DualHeaderSnapshot struct {
	eddt.Header
	Second eddt.Header
	ID     int32
}
