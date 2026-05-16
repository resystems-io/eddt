// Package bad_chan provides a Snapshot fixture with a channel payload field,
// used to exercise the unsupported-shape rejection path in G-03 parse tests.
package bad_chan

import eddt "go.resystems.io/eddt/runtime"

// ChanSnapshot has a chan field. The parser must reject it because channel
// types are not in the delta-gen §3.2 payload shape catalogue.
type ChanSnapshot struct {
	eddt.Header
	Events chan int
}
