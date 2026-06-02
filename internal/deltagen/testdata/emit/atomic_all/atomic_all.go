// Package atomic_all provides a Snapshot fixture exercising every atomic-row
// emission case for R-DG-015. One field per payload shape, plus the three
// tag families (delta.omit, delta.retired, delta.commutative).
// delta.nested is deliberately absent — it is a covered by R-DG-016.
package atomic_all

import eddt "go.resystems.io/eddt/runtime"

// Inner is a plain struct value used for the ShapeStructValue case.
type Inner struct{ A, B int32 }

// AtomicAllSnapshot covers every payload shape in the harmonised §1.6.3
// atomic column (R-DG-004, R-DG-005, R-DG-006 / R-DG-006, R-DG-016 / R-DG-006, R-DG-016) plus presence-axis and reserved tags:
//
//   - Key      string       — entity.key; extracted by parse stage, not in Delta
//   - Scalar   int32        — ShapeScalar; emits SetScalar *int32
//   - Pointer  *string      — ShapePointer; emits SetPointer **string
//   - Struct   Inner        — ShapeStructValue (atomic); emits SetStruct *Inner
//   - Slice    []byte       — ShapeSlice (atomic per R-DG-006, R-DG-016); emits SetSlice *[]uint8
//   - Map      map[string]int32 — ShapeMap (atomic per R-DG-006, R-DG-016); emits SetMap *map[string]int32
//   - Omitted  string       — delta.omit; suppressed from Delta
//   - Retired  string       — delta.retired; suppressed from Delta
//   - Commute  int32        — delta.commutative; emits as if untagged (§9.5)
type AtomicAllSnapshot struct {
	eddt.Header
	Key     string           `eddt:"entity.key"`
	Scalar  int32
	Pointer *string
	Struct  Inner
	Slice   []byte
	Map     map[string]int32
	Omitted string           `eddt:"delta.omit"`
	Retired string           `eddt:"delta.retired,since=2026-05-20"`
	Commute int32            `eddt:"delta.commutative"`
}
