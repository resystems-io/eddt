package runtime

import (
	"testing"
	"time"
)

// TestEntityIDIsZero verifies the zero-value detection helper used by
// HeaderAfterApply and HeaderForDiff to reject uninitialized EntityIDs.
func TestEntityIDIsZero(t *testing.T) {
	// Covers: R-04
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
	// Covers: R-04, Errata E-10
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
	// Covers: R-01
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
	if !h.EntityID.IsZero() {
		t.Error("EntityID should be zero on zero Header")
	}
}

// TestProvenanceZeroValue confirms the zero value of Provenance is well-formed.
func TestProvenanceZeroValue(t *testing.T) {
	// Covers: R-02
	var p Provenance
	if p.ValidUntil != nil {
		t.Error("ValidUntil should be nil on zero Provenance")
	}
	if p.Metadata != nil {
		t.Error("Metadata should be nil on zero Provenance")
	}
	if p.Gaps != nil {
		t.Error("Gaps should be nil on zero Provenance")
	}
}

// TestSequenceRange confirms inclusive Start/End semantics compile correctly.
func TestSequenceRange(t *testing.T) {
	// Covers: R-02
	r := SequenceRange{Start: 3, End: 7}
	if r.Start != 3 || r.End != 7 {
		t.Errorf("unexpected SequenceRange: %+v", r)
	}
}

// TestHeaderProvenanceAccumulation verifies that appending to Provenance
// preserves source order (the append-only contract from chain-lifecycle §3.2.1).
func TestHeaderProvenanceAccumulation(t *testing.T) {
	// Covers: R-01, R-02
	now := time.Now()
	p1 := Provenance{PublishedAt: now, Solution: "sol-a", Component: "c1", Instance: "i1"}
	p2 := Provenance{PublishedAt: now, Solution: "sol-b", Component: "c2", Instance: "i2"}

	h := Header{Provenance: []Provenance{p1}}
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
