// Package entityid_struct_key provides a Snapshot fixture with a tagged struct
// entity key containing two sub-fields of different underlying types (string
// and uint64). Used by TestEmitTemplate_StructKey_SamePkg to verify that the
// EntityID function body emits two runtime.Write* calls in source order and
// that the same-package method wrapper is generated (R-DG-034).
package entityid_struct_key

import eddt "go.resystems.io/eddt/runtime"

// SomeKey is the entity-key struct with two comparable sub-fields of different
// underlying types, exercising both WriteString and WriteUint64 hash calls.
type SomeKey struct {
	IMSI  string
	SubID uint64
}

// EntityIDStructKeySnapshot is the fixture Snapshot.
type EntityIDStructKeySnapshot struct {
	eddt.Header
	Key    SomeKey `eddt:"entity.key"`
	Status int32
}
