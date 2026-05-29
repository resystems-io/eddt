// Package arrowroundtrip is the R-DG-019 cross-generator integration fixture.
//
// snapshot.go covers the delta-gen field shapes that are currently compatible
// with arrow-gen:
//
//   - ShapeScalar  (string, float64) → *T in TDelta
//   - ShapeStructValue (atomic struct) → *SubStruct in TDelta
//
// ShapePointer (**T), ShapeSlice (*[]T), and ShapeMap (*map[K]V) are excluded
// from this file because arrow-gen does not yet support those pointer-wrapped
// compound types.  See snapshot_extended.go and R-DG-023 / R-DG-019.
package arrowroundtrip

import eddt "go.resystems.io/eddt/runtime"

// ARMeta is an all-scalar sub-struct used as the ShapeStructValue atomic field.
type ARMeta struct {
	Region  string
	Version int32
}

// ARSnapshot is the R-DG-019 round-trip Snapshot fixture.
// It covers only the delta-gen shapes currently compatible with arrow-gen.
type ARSnapshot struct {
	eddt.Header
	Key   string  `eddt:"entity.key"`
	Name  string
	Score float64
	Meta  ARMeta
}
