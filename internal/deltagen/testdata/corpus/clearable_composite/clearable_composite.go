// Package clearable_composite is the R-DG-026 conformance corpus fixture for a
// Snapshot with all three delta.nested+delta.clearable inner shapes together.
//
// It exercises:
//   - struct clearable (Location Address): struct-valued R-DG-016 companion reuse,
//     comparable struct so no reflect.DeepEqual fallback in the wrapper.
//   - map clearable (Tags map[string]string): net-new TagsMapDelta wrapper with
//     IsEmpty() / ApplyTagsMapDelta / DiffTagsMapDelta helpers.
//   - slice clearable (Groups []string): net-new GroupsSliceDelta wrapper with
//     IsEmpty() / ApplyGroupsSliceDelta / DiffGroupsSliceDelta helpers.
//   - atomic coexistence (Count int32): plain atomic field beside all three
//     clearable fields, verifying no cross-field interference.
//
// The fixture is consumed by all four TestConformance_* tests (R-DG-019 round-trip,
// R-DG-019 identity-diff, R-DG-019 coalesce-as-fold, plus the R-DG-026 dedicated
// TestConformance_TruthTable for §5.4 rows).
package clearable_composite

import eddt "go.resystems.io/eddt/runtime"

// Address is a comparable struct used as the clearable struct inner type.
type Address struct {
	Street string
	City   string
}

// ClearableCompositeSnapshot exercises all three nested+clearable inner shapes
// (struct / map / slice) plus a plain atomic field for coexistence regression.
type ClearableCompositeSnapshot struct {
	eddt.Header
	// Key is the entity key.
	Key string `eddt:"entity.key"`
	// Location is a clearable struct field: FieldDelta[AddressDelta] envelope.
	Location Address `eddt:"delta.nested,delta.clearable"`
	// Tags is a clearable map field: FieldDelta[TagsMapDelta] envelope.
	Tags map[string]string `eddt:"delta.nested,delta.clearable"`
	// Groups is a clearable slice field: FieldDelta[GroupsSliceDelta] envelope.
	Groups []string `eddt:"delta.nested,delta.clearable"`
	// Count is a plain atomic field coexisting with all three clearable fields.
	Count int32
}
