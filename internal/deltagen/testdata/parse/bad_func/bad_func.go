// Package bad_func provides a Snapshot fixture with a function-valued payload
// field, used to exercise the unsupported-shape rejection path in G-03 parse
// tests.
package bad_func

import eddt "go.resystems.io/eddt/runtime"

// FuncSnapshot has a func-valued field. The parser must reject it because
// function types are not in the delta-gen §3.2 payload shape catalogue.
type FuncSnapshot struct {
	eddt.Header
	Handler func() error
}
