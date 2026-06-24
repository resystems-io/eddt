package runtime

import (
	"testing"
	"time"
)

// TestEntityIDIsZero verifies the zero-value detection helper used by
// HeaderAfterApply and HeaderForDiff to reject uninitialized EntityIDs.
func TestEntityIDIsZero(t *testing.T) {
	// Covers: R-DG-035
	var zero EntityID
	if !zero.IsZero() {
		t.Error("zero EntityID should be zero")
	}

	var nonZero EntityID
	nonZero[0] = 1
	if nonZero.IsZero() {
		t.Error("EntityID with non-zero byte should not be zero")
	}

	// Only the last byte set — still non-zero.
	var last EntityID
	last[31] = 0xFF
	if last.IsZero() {
		t.Error("EntityID with last byte 0xFF should not be zero")
	}
}

// TestEntityIDEquality verifies that EntityID comparison is uniform ==
// (the key property that makes cross-chain entity continuity checks simple).
func TestEntityIDEquality(t *testing.T) {
	// Covers: R-DG-035, R-DG-034, R-DG-035
	var a, b EntityID
	if a != b {
		t.Error("two zero EntityIDs should be equal")
	}

	a[0] = 1
	if a == b {
		t.Error("EntityIDs differing in first byte should not be equal")
	}
	b[0] = 1
	if a != b {
		t.Error("EntityIDs with same first byte set should be equal")
	}
}

// TestHeaderZeroValue confirms the zero value of Header is well-formed
// (no panics on access, nil pointer fields are nil).
func TestHeaderZeroValue(t *testing.T) {
	// Covers: R-DG-029, R-DG-030
	var h Header
	if h.PreviousChainID != nil {
		t.Error("PreviousChainID should be nil on zero Header")
	}
	if h.NextChainID != nil {
		t.Error("NextChainID should be nil on zero Header")
	}
	if h.Closed != nil {
		t.Error("Closed should be nil on zero Header")
	}
	if h.Provenance != nil {
		t.Error("Provenance should be nil (not empty slice) on zero Header")
	}
	if h.Quality.Gaps != nil {
		t.Error("Quality.Gaps should be nil (not empty slice) on zero Header")
	}
	if !h.EntityID.IsZero() {
		t.Error("EntityID should be zero on zero Header")
	}
}

// TestProvenanceZeroValue confirms the zero value of the Provenance slice is a
// nil, gaps-free lineage (the provenance axis carries no completeness data).
func TestProvenanceZeroValue(t *testing.T) {
	// Covers: R-CL-004
	var p Provenance
	if p != nil {
		t.Error("zero Provenance should be a nil slice")
	}
	if len(p) != 0 {
		t.Error("zero Provenance should have no Origin entries")
	}
}

// TestOriginZeroValue confirms the zero value of an Origin lineage entry is
// well-formed and carries no quality fields (completeness lives on Quality).
func TestOriginZeroValue(t *testing.T) {
	// Covers: R-CL-004
	var o Origin
	if o.ValidUntil != nil {
		t.Error("ValidUntil should be nil on zero Origin")
	}
	if o.Metadata != nil {
		t.Error("Metadata should be nil on zero Origin")
	}
}

// TestQualityZeroValue confirms the zero value of Quality discloses no gaps —
// the quality axis is empty until a consumer stamps completeness at materialise.
func TestQualityZeroValue(t *testing.T) {
	// Covers: R-CL-036
	var q Quality
	if q.Gaps != nil {
		t.Error("Gaps should be nil on zero Quality")
	}
}

// TestSequenceRange confirms inclusive Start/End semantics compile correctly.
func TestSequenceRange(t *testing.T) {
	// Covers: R-DG-032
	r := SequenceRange{Start: 3, End: 7}
	if r.Start != 3 || r.End != 7 {
		t.Errorf("unexpected SequenceRange: %+v", r)
	}
}

// TestFieldDeltaOpValues locks the wire-encoding order of the Op constants and
// confirms that OpIgnore is the zero value of FieldDeltaOp.
func TestFieldDeltaOpValues(t *testing.T) {
	// Covers: R-DG-016, delta-gen-spec §5.2
	if OpIgnore != 0 {
		t.Errorf("OpIgnore must be 0 (zero value), got %d", OpIgnore)
	}
	if OpAssert != 1 {
		t.Errorf("OpAssert must be 1, got %d", OpAssert)
	}
	if OpRetract != 2 {
		t.Errorf("OpRetract must be 2, got %d", OpRetract)
	}
	var z FieldDeltaOp
	if z != OpIgnore {
		t.Error("zero FieldDeltaOp must equal OpIgnore")
	}
}

// TestFieldDeltaZeroValue confirms that a zero-valued FieldDelta is an explicit
// no-op (Op == OpIgnore, Value is the zero of T).
func TestFieldDeltaZeroValue(t *testing.T) {
	// Covers: R-DG-016
	var fd FieldDelta[int32]
	if fd.Op != OpIgnore {
		t.Errorf("zero FieldDelta.Op must be OpIgnore, got %d", fd.Op)
	}
	if fd.Value != 0 {
		t.Errorf("zero FieldDelta[int32].Value must be 0, got %d", fd.Value)
	}
}

// TestFieldDeltaEquality confirms that FieldDelta[T] is comparable when T is
// comparable, and that Op and Value are both considered by ==.
func TestFieldDeltaEquality(t *testing.T) {
	// Covers: R-DG-016
	a := FieldDelta[int32]{Op: OpAssert, Value: 7}
	b := FieldDelta[int32]{Op: OpAssert, Value: 7}
	if a != b {
		t.Error("identical FieldDelta[int32] must compare equal")
	}

	diffOp := FieldDelta[int32]{Op: OpRetract, Value: 7}
	if a == diffOp {
		t.Error("FieldDelta with different Op must not compare equal")
	}

	diffVal := FieldDelta[int32]{Op: OpAssert, Value: 8}
	if a == diffVal {
		t.Error("FieldDelta with different Value must not compare equal")
	}

	// Instantiation breadth: ensure the generic compiles for pointer inner types
	// (the form used for compositional clearable fields — R-DG-026).
	_ = FieldDelta[*int32]{Op: OpIgnore}
	pfd := FieldDelta[*int32]{Op: OpAssert}
	if pfd.Op != OpAssert {
		t.Error("FieldDelta[*int32] Op must round-trip")
	}
}

// TestHeaderProvenanceAccumulation verifies that appending to Provenance
// preserves source order (the append-only contract from chain-lifecycle R-CL-018).
func TestHeaderProvenanceAccumulation(t *testing.T) {
	// Covers: R-DG-029, R-DG-030, R-DG-032
	now := time.Now()
	p1 := Origin{PublishedAt: now, Solution: "sol-a", Component: "c1", Instance: "i1"}
	p2 := Origin{PublishedAt: now, Solution: "sol-b", Component: "c2", Instance: "i2"}

	h := Header{Provenance: Provenance{p1}}
	h.Provenance = append(h.Provenance, p2)

	if len(h.Provenance) != 2 {
		t.Fatalf("expected 2 Provenance entries, got %d", len(h.Provenance))
	}
	if h.Provenance[0].Solution != "sol-a" {
		t.Error("first Provenance entry should be sol-a")
	}
	if h.Provenance[1].Solution != "sol-b" {
		t.Error("second Provenance entry should be sol-b")
	}
}
