// Package struct_key_clearable is the HK-16 corpus fixture combining a
// struct-valued entity.key with clearable payload fields.
//
// It exercises the coexistence of EntityID hashing (EM-05) and tri-state
// clearable field encoding (CL-08) in a single Snapshot type:
//
//   - Key:    SkcKey{ID string, Shard int32} — struct entity.key, multi-field hash.
//   - Home:   SkcAddress clearable struct field → FieldDelta[HomeSkcAddressDelta].
//   - Labels: map[string]string clearable map field → FieldDelta[LabelsMapDelta].
//   - Tags:   []string clearable slice field → FieldDelta[TagsSliceDelta].
//   - Score:  float64 plain atomic coexistence field.
//
// The fixture is consumed by TestCorpus_All (C-01) and all four
// TestConformance_* tests (C-02 round-trip, C-03 identity-diff, C-04 coalesce).
package struct_key_clearable

import eddt "go.resystems.io/eddt/runtime"

// SkcKey is the struct-valued entity key.
// Fields are scalar so SkcKey is comparable; EntityID hashes them in
// lexicographic field-name order (EM-05): ID (string) < Shard (int32).
type SkcKey struct {
	ID    string
	Shard int32
}

// SkcAddress is the inner struct for the clearable struct field.
// Both fields are scalar so SkcAddress is comparable (no reflect.DeepEqual needed).
type SkcAddress struct {
	Street string
	City   string
}

// StructKeyClearableSnapshot combines a struct entity.key (EM-05) with
// all three clearable field shapes (CL-08) for coexistence validation.
type StructKeyClearableSnapshot struct {
	eddt.Header
	Key    SkcKey            `eddt:"entity.key"`
	Home   SkcAddress        `eddt:"delta.nested,delta.clearable"`
	Labels map[string]string `eddt:"delta.nested,delta.clearable"`
	Tags   []string          `eddt:"delta.nested,delta.clearable"`
	Score  float64
}
