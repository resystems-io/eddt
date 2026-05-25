// Package composite is the C-01 compositional conformance corpus fixture.
//
// It covers all three delta.nested shapes (struct/map/slice) in a single
// Snapshot definition, plus an atomic coexistence field.  This fixture
// exercises the compositional emission paths (N-01, N-03, N-04) together
// and is used by C-02..C-05 to test compositional semantics.
//
// Coverage:
//   - delta.nested struct value: Details ContactDetails → DetailsDelta (N-01)
//   - delta.nested map: Labels map[string]string → UpdatedLabels / RemovedLabels (N-03)
//   - delta.nested slice: Groups []string → AddedGroups / RemovedGroups (N-04)
//   - Atomic coexistence: Rank int32 alongside compositional fields
//   - Scalar string entity.key
package composite

import eddt "go.resystems.io/eddt/runtime"

// ContactDetails is a struct used as a delta.nested struct-value field (N-01).
// All fields are scalar so the struct is comparable in Go.
type ContactDetails struct {
	// Email is the contact email address.
	Email string
	// Phone is the contact phone number.
	Phone string
}

// CompositeSnapshot covers all three compositional shapes under delta.nested.
//
// Delta encoding axes covered (§1.6.3 + Phase 5):
//   - Details: ShapeStructValue + delta.nested → DetailsDelta (N-01)
//   - Labels:  ShapeMap         + delta.nested → UpdatedLabels / RemovedLabels (N-03)
//   - Groups:  ShapeSlice       + delta.nested → AddedGroups / RemovedGroups (N-04)
//   - Rank:    ShapeScalar (atomic coexistence) → SetRank *int32 in TDelta
type CompositeSnapshot struct {
	eddt.Header
	// Key is the scalar string entity key; used to compute EntityID (EM-05).
	Key string `eddt:"entity.key"`
	// Details is a struct-value field encoded with element-wise delta (N-01).
	Details ContactDetails `eddt:"delta.nested"`
	// Labels is a map field encoded with element-wise delta (N-03, E-16 upsert semantics).
	Labels map[string]string `eddt:"delta.nested"`
	// Groups is a slice field encoded with set-difference delta (N-04, E-15 set-diff semantics).
	Groups []string `eddt:"delta.nested"`
	// Rank is an atomic coexistence field; changes produce SetRank *int32 in the delta.
	Rank int32
}
