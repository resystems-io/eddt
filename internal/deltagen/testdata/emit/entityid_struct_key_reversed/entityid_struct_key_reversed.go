// Package entityid_struct_key_reversed provides a Snapshot fixture whose
// entity-key struct declares sub-fields in reverse alphabetical order
// (SubID before IMSI). Used by TestEmitTemplate_StructKey_FieldOrderStability
// to prove that EntityID emission is stable to source declaration order:
// the emitter sorts sub-fields lexicographically by name, so the generated
// Write* calls are identical to those for entityid_struct_key despite the
// different source order (R-DG-034).
package entityid_struct_key_reversed

import eddt "go.resystems.io/eddt/runtime"

// ReversedKey is the entity-key struct with sub-fields declared in reverse
// alphabetical order (SubID first, then IMSI). The emitter must still hash
// IMSI before SubID (lexicographic order) to maintain stability.
type ReversedKey struct {
	SubID uint64
	IMSI  string
}

// EntityIDReversedKeySnapshot is the fixture Snapshot.
type EntityIDReversedKeySnapshot struct {
	eddt.Header
	Key    ReversedKey `eddt:"entity.key"`
	Status int32
}
