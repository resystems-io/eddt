// snapshot_header.go is the C-09 / R-DG-052 envelope round-trip fixture.
//
// Unlike snapshot.go / snapshot_extended.go / snapshot_clearable.go — which are
// organised by delta *field shape* and treat the embedded Header as incidental —
// ARHeader exists to force the FULL runtime.Header envelope through the
// cross-generator pipeline: EntityID, the anchor pointers, the bitemporal times,
// the two-axis envelope — Provenance[]Origin{Metadata map} (lineage) and
// Quality{Gaps []SequenceRange} (completeness). One scalar payload field (Label)
// keeps the generated Delta non-trivial without re-testing shapes.
package arrowroundtrip

import eddt "go.resystems.io/eddt/runtime"

// ARHeader is the envelope round-trip Snapshot. The generated ARHeaderDelta
// embeds the same runtime.Header; the inner test populates and asserts that
// Header — both the Provenance (lineage) and Quality (completeness) axes —
// round-trips through Arrow and Parquet.
type ARHeader struct {
	eddt.Header
	Key   string `eddt:"entity.key"`
	Label string
}
