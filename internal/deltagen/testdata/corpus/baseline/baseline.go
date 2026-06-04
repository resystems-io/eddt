// Package baseline is the R-DG-019 primary conformance corpus fixture.
//
// It covers every baseline field shape and every baseline presence tag in a
// single Snapshot definition, with a scalar string entity.key.  This fixture
// is the canonical input for R-DG-019 / R-DG-019 / R-DG-019 property-based tests.
//
// Coverage:
//   - All five atomic shapes: scalar (string, int32), pointer (*int32),
//     struct value (MetaInfo), slice ([]string), map (map[string]string).
//   - All three baseline presence tags: delta.omit, delta.retired, delta.commutative.
//   - Scalar string entity.key (single-field EntityID hash path, R-DG-034).
package baseline

import eddt "go.resystems.io/eddt/runtime"

// MetaInfo is an all-scalar struct used as a ShapeStructValue atomic field.
// All fields are scalar so the struct is comparable in Go.
type MetaInfo struct {
	// Region is the geographic region identifier.
	Region string
	// Version is the schema version counter.
	Version int32
}

// BaselineSnapshot is the primary conformance corpus fixture.
//
// Delta encoding axes covered (§1.6.3):
//   - Name:     ShapeScalar  → SetName *string in TDelta
//   - Priority: ShapePointer → SetPriority **int32 in TDelta
//   - Meta:     ShapeStructValue (atomic) → SetMeta *MetaInfo in TDelta
//   - Tags:     ShapeSlice (atomic per R-DG-006, R-DG-016) → SetTags *[]string in TDelta
//   - Attrs:    ShapeMap (atomic per R-DG-006, R-DG-016)   → SetAttrs *map[string]string in TDelta
//   - Hidden:   delta.omit — suppressed from TDelta (presence = Omit)
//   - Legacy:   delta.retired — suppressed from TDelta (presence = Retired)
//   - Score:    delta.commutative — SetScore *int32 in TDelta (presence = Commutative)
type BaselineSnapshot struct {
	eddt.Header
	// Key is the scalar string entity key; used to compute EntityID (R-DG-034).
	Key string `eddt:"entity.key"`
	// Name is a scalar string field (ShapeScalar).
	Name string
	// Priority is a pointer-to-int32 field (ShapePointer).
	Priority *int32
	// Meta is a struct-value atomic field (ShapeStructValue); all fields scalar.
	Meta MetaInfo
	// Tags is a string-slice atomic field (ShapeSlice, atomic per R-DG-006, R-DG-016).
	// Use delta.nested to opt into set-diff encoding (R-DG-016, R-DG-028).
	Tags []string
	// Attrs is a string-keyed string-valued map atomic field (ShapeMap, atomic per R-DG-006, R-DG-016).
	// Use delta.nested to opt into element-wise encoding (R-DG-016).
	Attrs map[string]string
	// Hidden is excluded from the TDelta type (delta.omit presence tag).
	Hidden string `eddt:"delta.omit"`
	// Legacy is excluded from the TDelta type (delta.retired presence tag).
	Legacy int32 `eddt:"delta.retired"`
	// Score is a commutative atomic field; last-writer-wins semantics apply.
	Score int32 `eddt:"delta.commutative"`
}
