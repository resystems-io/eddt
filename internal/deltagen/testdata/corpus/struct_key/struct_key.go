// Package struct_key is the C-01 struct-valued entity key corpus fixture.
//
// It covers the multi-field EntityID hash path (EM-05) with a struct-valued
// entity.key.  Both SessionKey fields are scalar, making the struct comparable
// in Go.  The generator hashes the fields in lexicographic field-name order
// (EM-05 stability invariant) and emits an EntityID function and method wrapper.
//
// Coverage:
//   - Struct-valued entity.key (SessionKey{TenantID string, SessionN uint64})
//   - Multi-field EntityID hash with lexicographic field-name order (EM-05)
//   - Scalar atomic fields alongside the struct key (State string, Count int32)
package struct_key

import eddt "go.resystems.io/eddt/runtime"

// SessionKey is the struct-valued entity key.
//
// Both fields are scalar so SessionKey is comparable in Go.  The generator
// hashes them in lexicographic field-name order (EM-05):
// SessionN (uint64) < TenantID (string).
type SessionKey struct {
	// TenantID is the tenant namespace identifier.
	TenantID string
	// SessionN is the per-tenant session sequence number.
	SessionN uint64
}

// SessionSnapshot covers the struct-key EntityID hash path (EM-05).
//
// Delta encoding axes covered:
//   - Key:   struct-valued entity.key → EntityID hashes SessionKey fields (EM-05)
//   - State: ShapeScalar → SetState *string in TDelta
//   - Count: ShapeScalar → SetCount *int32 in TDelta
type SessionSnapshot struct {
	eddt.Header
	// Key is the struct-valued entity key; EntityID is computed from its fields.
	Key SessionKey `eddt:"entity.key"`
	// State is a scalar string field.
	State string
	// Count is a scalar int32 field.
	Count int32
}
