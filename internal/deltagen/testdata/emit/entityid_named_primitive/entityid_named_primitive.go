// Package entityid_named_primitive provides a Snapshot fixture with a named-
// primitive entity key (Key IMSI where type IMSI string). Used by
// TestEmitTemplate_NamedPrimitive_KeyMethodEmitted to verify that the method
// wrapper is emitted for named-primitive keys and that the function body
// contains the string(k) conversion (EM-05).
package entityid_named_primitive

import eddt "go.resystems.io/eddt/runtime"

// IMSI is a named string type used as an entity key. Being a named type, it
// qualifies for the same-package EntityID method wrapper (EM-05, R-24).
type IMSI string

// EntityIDNamedPrimSnapshot is the fixture Snapshot.
type EntityIDNamedPrimSnapshot struct {
	eddt.Header
	Key    IMSI `eddt:"entity.key"`
	Status int32
}
