// Package bad_iface provides a Snapshot fixture with an interface-typed payload
// field, used to exercise the unsupported-shape rejection path in G-03 parse
// tests.
package bad_iface

import eddt "go.resystems.io/eddt/runtime"

// IfaceSnapshot has an interface{} field. The parser must reject it because
// interface types are not in the delta-gen §3.2 payload shape catalogue.
type IfaceSnapshot struct {
	eddt.Header
	Anything interface{}
}
