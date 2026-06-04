// Package composite is the R-DG-019 compositional conformance corpus fixture.
//
// It covers all three delta.nested shapes (struct/map/slice) in a single
// Snapshot definition, plus an atomic coexistence field.  This fixture
// exercises the compositional emission paths (R-DG-016, R-DG-016, R-DG-016, R-DG-028) together
// and is used by R-DG-019..R-DG-019, R-DG-039 to test compositional semantics.
//
// Coverage:
//   - delta.nested struct value: Details ContactDetails → DetailsDelta (R-DG-016)
//   - delta.nested map: Labels map[string]string → UpdatedLabels / RemovedLabels (R-DG-016)
//   - delta.nested slice: Groups []string → AddedGroups / RemovedGroups (R-DG-016, R-DG-028)
//   - Atomic coexistence: Rank int32 alongside compositional fields
//   - Scalar string entity.key
package composite

import eddt "go.resystems.io/eddt/runtime"

// ContactDetails is a struct used as a delta.nested struct-value field (R-DG-016).
// All fields are scalar so the struct is comparable in Go.
type ContactDetails struct {
	// Email is the contact email address.
	Email string
	// Phone is the contact phone number.
	Phone string
}

// CompositeSnapshot covers all three compositional shapes under delta.nested.
//
// Delta encoding axes covered (§1.6.3 ):
//   - Details: ShapeStructValue + delta.nested → DetailsDelta (R-DG-016)
//   - Labels:  ShapeMap         + delta.nested → UpdatedLabels / RemovedLabels (R-DG-016)
//   - Groups:  ShapeSlice       + delta.nested → AddedGroups / RemovedGroups (R-DG-016, R-DG-028)
//   - Rank:    ShapeScalar (atomic coexistence) → SetRank *int32 in TDelta
type CompositeSnapshot struct {
	eddt.Header
	// Key is the scalar string entity key; used to compute EntityID (R-DG-034).
	Key string `eddt:"entity.key"`
	// Details is a struct-value field encoded with element-wise delta (R-DG-016).
	Details ContactDetails `eddt:"delta.nested"`
	// Labels is a map field encoded with element-wise delta (R-DG-016, R-DG-006, R-DG-016 upsert semantics).
	Labels map[string]string `eddt:"delta.nested"`
	// Groups is a slice field encoded with set-difference delta (R-DG-016, R-DG-028, R-DG-006, R-DG-016 set-diff semantics).
	Groups []string `eddt:"delta.nested"`
	// Rank is an atomic coexistence field; changes produce SetRank *int32 in the delta.
	Rank int32
}
