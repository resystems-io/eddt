package runtime

import (
	"strings"
	"testing"
	"time"
)

// ── test fixtures ─────────────────────────────────────────────────────────────

// eid returns a non-zero EntityID whose first byte is b. Using distinct values
// of b across a single test gives clearly different EntityIDs without any hash
// computation.
func eid(b byte) EntityID {
	var id EntityID
	id[0] = b
	return id
}

// ts returns a UTC time from a small integer offset in hours, using time.Date
// so that no monotonic clock reading is embedded. time.Date-constructed values
// compare correctly with == and .Equal(), avoiding the subtle inequality that
// arises when mixing time.Now() values (which carry a monotonic reading) with
// time.Time values loaded from storage or constructed by time.Date.
func ts(hoursOffset int) time.Time {
	return time.Date(2025, 1, 1, hoursOffset, 0, 0, 0, time.UTC)
}

// isZeroHeader reports whether h is a zero-value Header. Defined as a helper
// rather than using reflect.DeepEqual so the package avoids an import of
// reflect in tests. The Provenance and Quality.Gaps fields are slices so ==
// cannot be used directly on Header.
func isZeroHeader(h Header) bool {
	return h.EntityID.IsZero() &&
		h.ChainID == "" &&
		h.PreviousChainID == nil &&
		h.NextChainID == nil &&
		h.Closed == nil &&
		h.Sequence == 0 &&
		h.EffectiveAt.IsZero() &&
		h.PublishedAt.IsZero() &&
		h.Provenance == nil &&
		h.Quality.Gaps == nil
}

// ── HeaderAfterApply ──────────────────────────────────────────────────────────

// TestHeaderAfterApply_HappyPath verifies every output field of HeaderAfterApply
// against the field-by-field assignment rules in chain-lifecycle-spec.md R-CL-012.
func TestHeaderAfterApply_HappyPath(t *testing.T) {
	// Covers: R-DG-029, R-DG-031
	chainID := "chain-abc"
	entity := eid(0x01)

	provS := Provenance{{Solution: "src", Component: "c1", Instance: "i1", PublishedAt: ts(0)}}
	provD := Provenance{{Solution: "tier2", Component: "c2", Instance: "i2", PublishedAt: ts(1)}}

	s := Header{
		EntityID:    entity,
		ChainID:     chainID,
		Sequence:    5,
		EffectiveAt: ts(0),
		PublishedAt: ts(0),
		Provenance:  provS,
	}
	d := Header{
		EntityID:    entity,
		ChainID:     chainID,
		Sequence:    6,
		EffectiveAt: ts(1),
		PublishedAt: ts(2),
		Provenance:  provD,
	}

	result, err := HeaderAfterApply(s, d)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// EntityID and ChainID propagate from s (chain-lifecycle R-CL-012).
	if result.EntityID != s.EntityID {
		t.Errorf("EntityID: got %x, want %x", result.EntityID, s.EntityID)
	}
	if result.ChainID != s.ChainID {
		t.Errorf("ChainID: got %q, want %q", result.ChainID, s.ChainID)
	}

	// Anchor-only fields must be nil: Apply's result is a mid-chain Snapshot,
	// not a birth Snapshot (PreviousChainID) or terminator (NextChainID, Closed).
	if result.PreviousChainID != nil {
		t.Errorf("PreviousChainID: want nil, got %v", result.PreviousChainID)
	}
	if result.NextChainID != nil {
		t.Errorf("NextChainID: want nil, got %v", result.NextChainID)
	}
	if result.Closed != nil {
		t.Errorf("Closed: want nil, got %v", result.Closed)
	}

	// Sequence, EffectiveAt, PublishedAt come from d (chain-lifecycle R-CL-012).
	if result.Sequence != d.Sequence {
		t.Errorf("Sequence: got %d, want %d", result.Sequence, d.Sequence)
	}
	// Use .Equal() for time comparison: == would fail if monotonic readings differ.
	if !result.EffectiveAt.Equal(d.EffectiveAt) {
		t.Errorf("EffectiveAt: got %v, want %v", result.EffectiveAt, d.EffectiveAt)
	}
	if !result.PublishedAt.Equal(d.PublishedAt) {
		t.Errorf("PublishedAt: got %v, want %v", result.PublishedAt, d.PublishedAt)
	}

	// Provenance = s.Provenance ⊕ d.Provenance (chain-lifecycle R-CL-018).
	wantLen := len(provS) + len(provD)
	if len(result.Provenance) != wantLen {
		t.Fatalf("Provenance length: got %d, want %d", len(result.Provenance), wantLen)
	}
	if result.Provenance[0].Solution != "src" {
		t.Errorf("Provenance[0].Solution: got %q, want %q", result.Provenance[0].Solution, "src")
	}
	if result.Provenance[1].Solution != "tier2" {
		t.Errorf("Provenance[1].Solution: got %q, want %q", result.Provenance[1].Solution, "tier2")
	}
}

// TestHeaderAfterApply_Validation exercises every validation rule in the order
// they are checked. Each subtest passes a pair (s, d) that violates exactly one
// invariant and expects a non-nil error and a zero-value result Header.
func TestHeaderAfterApply_Validation(t *testing.T) {
	// Covers: R-DG-029, R-DG-031
	entity := eid(0x01)
	closedAt := ts(0)

	// base produces a fully valid (s, d) pair. Each subtest mutates a copy.
	base := func() (Header, Header) {
		s := Header{
			EntityID:    entity,
			ChainID:     "chain-1",
			Sequence:    5,
			EffectiveAt: ts(0),
			PublishedAt: ts(0),
		}
		d := Header{
			EntityID:    entity,
			ChainID:     "chain-1",
			Sequence:    6,
			EffectiveAt: ts(1),
			PublishedAt: ts(1),
		}
		return s, d
	}

	tests := []struct {
		name        string
		mutate      func(s, d *Header)
		wantErrFrag string // substring that must appear in the error message
	}{
		{
			// R-DG-034, R-DG-035: snapshot with all-zero EntityID is always invalid.
			name:        "snapshot_EntityID_zero",
			mutate:      func(s, _ *Header) { s.EntityID = EntityID{} },
			wantErrFrag: "snapshot EntityID is zero",
		},
		{
			// R-DG-034, R-DG-035: delta with all-zero EntityID is always invalid.
			name:        "delta_EntityID_zero",
			mutate:      func(_, d *Header) { d.EntityID = EntityID{} },
			wantErrFrag: "delta EntityID is zero",
		},
		{
			// chain-lifecycle R-CL-012: entity integrity — the two notifications
			// must belong to the same logical entity.
			name:        "EntityID_mismatch",
			mutate:      func(_, d *Header) { d.EntityID = eid(0x02) },
			wantErrFrag: "EntityID mismatch",
		},
		{
			// chain-lifecycle R-CL-012: chain integrity — both notifications must
			// be on the same chain.
			name:        "ChainID_mismatch",
			mutate:      func(_, d *Header) { d.ChainID = "chain-2" },
			wantErrFrag: "ChainID mismatch",
		},
		{
			// chain-lifecycle R-CL-015: Sequence must be strictly increasing.
			// Equal Sequences indicate a duplicate or idempotent re-delivery.
			name:        "Sequence_equal",
			mutate:      func(s, d *Header) { d.Sequence = s.Sequence },
			wantErrFrag: "Sequence",
		},
		{
			// chain-lifecycle R-CL-015: Sequence must be strictly increasing.
			// A lower Sequence on the delta indicates an out-of-order application.
			name:        "Sequence_lower",
			mutate:      func(s, d *Header) { d.Sequence = s.Sequence - 1 },
			wantErrFrag: "Sequence",
		},
		{
			// chain-lifecycle R-CL-017: EffectiveAt must be non-decreasing.
			// A delta with an earlier EffectiveAt indicates a domain-time rollback.
			name:        "EffectiveAt_before",
			mutate:      func(s, d *Header) { d.EffectiveAt = s.EffectiveAt.Add(-time.Second) },
			wantErrFrag: "EffectiveAt",
		},
		{
			// chain-lifecycle R-CL-020: no notifications may be applied to a
			// chain that has already been terminated (s.Closed != nil).
			name:        "snapshot_closed",
			mutate:      func(s, _ *Header) { s.Closed = &closedAt },
			wantErrFrag: "closed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, d := base()
			tc.mutate(&s, &d)

			result, err := HeaderAfterApply(s, d)

			// A validation failure must return a non-nil error.
			if err == nil {
				t.Fatalf("expected error for %q, got nil (result: %+v)", tc.name, result)
			}
			// The error message must reference the violated invariant.
			if tc.wantErrFrag != "" && !strings.Contains(err.Error(), tc.wantErrFrag) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.wantErrFrag)
			}
			// On error, the returned Header must be the zero value — no partial
			// state for the caller to accidentally use.
			if !isZeroHeader(result) {
				t.Errorf("expected zero Header on error, got %+v", result)
			}
		})
	}
}

// TestHeaderAfterApply_EffectiveAt_Equal confirms that equal EffectiveAt values
// are accepted. The spec requires d.EffectiveAt >= s.EffectiveAt (non-decrease,
// not strict increase), so simultaneous changes sharing one instant are valid.
func TestHeaderAfterApply_EffectiveAt_Equal(t *testing.T) {
	// Covers: R-DG-029, R-DG-031 (boundary between valid and invalid for R-CL-017)
	entity := eid(0x01)
	instant := ts(3)

	s := Header{EntityID: entity, ChainID: "c", Sequence: 1, EffectiveAt: instant, PublishedAt: instant}
	d := Header{EntityID: entity, ChainID: "c", Sequence: 2, EffectiveAt: instant, PublishedAt: instant}

	if _, err := HeaderAfterApply(s, d); err != nil {
		t.Errorf("equal EffectiveAt should be accepted, got error: %v", err)
	}
}

// TestHeaderAfterApply_SequenceGap confirms that a sequence gap larger than 1
// is accepted. chain-lifecycle R-CL-016 specifies gap-tolerant apply: only strict
// monotonicity (d.Sequence > s.Sequence) is required; skipped Sequences become
// taint entries in the consumer state machine, not a rejection reason here.
func TestHeaderAfterApply_SequenceGap(t *testing.T) {
	// Covers: R-DG-029, R-DG-031 (gap-tolerant apply, R-CL-016)
	entity := eid(0x01)

	s := Header{EntityID: entity, ChainID: "c", Sequence: 1, EffectiveAt: ts(0), PublishedAt: ts(0)}
	d := Header{EntityID: entity, ChainID: "c", Sequence: 100, EffectiveAt: ts(1), PublishedAt: ts(1)}

	result, err := HeaderAfterApply(s, d)
	if err != nil {
		t.Fatalf("gap of 99 should be accepted, got error: %v", err)
	}
	if result.Sequence != 100 {
		t.Errorf("Sequence: got %d, want 100", result.Sequence)
	}
}

// TestHeaderAfterApply_Provenance_NilInputs verifies that when both s.Provenance
// and d.Provenance are nil the result is also nil (not an allocated empty slice).
// This preserves the zero-value convention and avoids spurious non-nil Provenance
// slices on Headers that have never had any lineage recorded.
func TestHeaderAfterApply_Provenance_NilInputs(t *testing.T) {
	// Covers: R-DG-029, R-DG-031 (Provenance concatenation edge case)
	entity := eid(0x01)

	s := Header{EntityID: entity, ChainID: "c", Sequence: 1, EffectiveAt: ts(0), PublishedAt: ts(0)}
	d := Header{EntityID: entity, ChainID: "c", Sequence: 2, EffectiveAt: ts(1), PublishedAt: ts(1)}
	// Both s.Provenance and d.Provenance are nil (zero value).

	result, err := HeaderAfterApply(s, d)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Provenance != nil {
		t.Errorf("both-nil Provenance inputs should yield nil result, got %v", result.Provenance)
	}
}

// TestHeaderAfterApply_Provenance_OneNil verifies that when one side's Provenance
// is nil the result equals the non-nil side's entries only.
func TestHeaderAfterApply_Provenance_OneNil(t *testing.T) {
	// Covers: R-DG-029, R-DG-031 (Provenance concatenation, one-nil case)
	entity := eid(0x01)
	prov := Provenance{{Solution: "only", Component: "c", Instance: "i", PublishedAt: ts(0)}}

	// s has Provenance, d does not.
	s := Header{EntityID: entity, ChainID: "c", Sequence: 1, EffectiveAt: ts(0), PublishedAt: ts(0), Provenance: prov}
	d := Header{EntityID: entity, ChainID: "c", Sequence: 2, EffectiveAt: ts(1), PublishedAt: ts(1)}

	result, err := HeaderAfterApply(s, d)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Provenance) != 1 || result.Provenance[0].Solution != "only" {
		t.Errorf("unexpected Provenance: %v", result.Provenance)
	}

	// d has Provenance, s does not.
	s2 := Header{EntityID: entity, ChainID: "c", Sequence: 1, EffectiveAt: ts(0), PublishedAt: ts(0)}
	d2 := Header{EntityID: entity, ChainID: "c", Sequence: 2, EffectiveAt: ts(1), PublishedAt: ts(1), Provenance: prov}

	result2, err2 := HeaderAfterApply(s2, d2)
	if err2 != nil {
		t.Fatalf("unexpected error: %v", err2)
	}
	if len(result2.Provenance) != 1 || result2.Provenance[0].Solution != "only" {
		t.Errorf("unexpected Provenance: %v", result2.Provenance)
	}
}

// TestHeaderAfterApply_Provenance_NoAlias verifies that result.Provenance does
// not share a backing array with either s.Provenance or d.Provenance. If it did,
// appending to an input slice after the call would silently corrupt the result,
// violating the append-only contract of chain-lifecycle R-CL-018.
func TestHeaderAfterApply_Provenance_NoAlias(t *testing.T) {
	// Covers: R-DG-029, R-DG-031 (Provenance slice aliasing safety)
	entity := eid(0x01)
	provS := Provenance{{Solution: "s-entry", Component: "c", Instance: "i", PublishedAt: ts(0)}}
	provD := Provenance{{Solution: "d-entry", Component: "c", Instance: "i", PublishedAt: ts(1)}}

	s := Header{EntityID: entity, ChainID: "c", Sequence: 1, EffectiveAt: ts(0), PublishedAt: ts(0), Provenance: provS}
	d := Header{EntityID: entity, ChainID: "c", Sequence: 2, EffectiveAt: ts(1), PublishedAt: ts(1), Provenance: provD}

	result, err := HeaderAfterApply(s, d)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Mutate the input slices by appending a new entry after the call.
	// If result.Provenance shared their backing array, len(result.Provenance)
	// might increase beyond the 2 entries that were present at call time.
	s.Provenance = append(s.Provenance, Origin{Solution: "s-extra"})
	d.Provenance = append(d.Provenance, Origin{Solution: "d-extra"})

	if len(result.Provenance) != 2 {
		t.Errorf("appending to input Provenance slices changed result length: got %d, want 2",
			len(result.Provenance))
	}
}

// TestHeaderAfterApply_Quality_NotPropagated verifies that HeaderAfterApply does
// not carry any Quality (completeness) signal from its inputs into the result:
// the Apply algebra never computes or propagates gaps (chain-lifecycle R-CL-016,
// R-CL-036). Completeness is the consumer's concern, stamped at materialise time.
func TestHeaderAfterApply_Quality_NotPropagated(t *testing.T) {
	// Covers: R-CL-016, R-CL-036
	entity := eid(0x01)
	s := Header{EntityID: entity, ChainID: "c", Sequence: 1, EffectiveAt: ts(0), PublishedAt: ts(0),
		Quality: Quality{Gaps: []SequenceRange{{Start: 2, End: 4}}}}
	d := Header{EntityID: entity, ChainID: "c", Sequence: 2, EffectiveAt: ts(1), PublishedAt: ts(1),
		Quality: Quality{Gaps: []SequenceRange{{Start: 7, End: 7}}}}

	result, err := HeaderAfterApply(s, d)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Quality.Gaps != nil {
		t.Errorf("Apply must not propagate Quality.Gaps; got %v", result.Quality.Gaps)
	}
}

// ── HeaderForDiff ─────────────────────────────────────────────────────────────

// TestHeaderForDiff_HappyPath verifies every output field of HeaderForDiff
// against the field-by-field assignment rules in chain-lifecycle-spec.md R-CL-013.
func TestHeaderForDiff_HappyPath(t *testing.T) {
	// Covers: R-DG-030
	entity := eid(0x01)
	chainID := "chain-xyz"

	a := Header{EntityID: entity, ChainID: chainID, Sequence: 3, EffectiveAt: ts(0), PublishedAt: ts(0)}
	b := Header{EntityID: entity, ChainID: chainID, Sequence: 7, EffectiveAt: ts(2), PublishedAt: ts(3)}

	result, err := HeaderForDiff(a, b)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// EntityID and ChainID come from b (chain-lifecycle R-CL-013).
	if result.EntityID != b.EntityID {
		t.Errorf("EntityID: got %x, want %x", result.EntityID, b.EntityID)
	}
	if result.ChainID != b.ChainID {
		t.Errorf("ChainID: got %q, want %q", result.ChainID, b.ChainID)
	}

	// Anchor-only fields must be nil: the result is a synthetic Delta.
	if result.PreviousChainID != nil {
		t.Errorf("PreviousChainID: want nil, got %v", result.PreviousChainID)
	}
	if result.NextChainID != nil {
		t.Errorf("NextChainID: want nil, got %v", result.NextChainID)
	}
	if result.Closed != nil {
		t.Errorf("Closed: want nil, got %v", result.Closed)
	}

	// Sequence, EffectiveAt, PublishedAt come from b.
	if result.Sequence != b.Sequence {
		t.Errorf("Sequence: got %d, want %d", result.Sequence, b.Sequence)
	}
	if !result.EffectiveAt.Equal(b.EffectiveAt) {
		t.Errorf("EffectiveAt: got %v, want %v", result.EffectiveAt, b.EffectiveAt)
	}
	if !result.PublishedAt.Equal(b.PublishedAt) {
		t.Errorf("PublishedAt: got %v, want %v", result.PublishedAt, b.PublishedAt)
	}

	// Provenance is always nil: Diff is a pure transformation; lineage is
	// supplied by the call site if needed (chain-lifecycle R-CL-013).
	if result.Provenance != nil {
		t.Errorf("Provenance: want nil, got %v", result.Provenance)
	}
}

// TestHeaderForDiff_ProvenanceAlwaysNil verifies that even when a and b carry
// non-nil Provenance slices, the result's Provenance is nil. The call site is
// responsible for appending its own lineage entries (chain-lifecycle R-CL-013).
func TestHeaderForDiff_ProvenanceAlwaysNil(t *testing.T) {
	// Covers: R-DG-030
	entity := eid(0x01)
	prov := Provenance{{Solution: "s", Component: "c", Instance: "i", PublishedAt: ts(0)}}

	a := Header{EntityID: entity, ChainID: "c", Sequence: 1, EffectiveAt: ts(0), PublishedAt: ts(0), Provenance: prov}
	b := Header{EntityID: entity, ChainID: "c", Sequence: 2, EffectiveAt: ts(1), PublishedAt: ts(1), Provenance: prov}

	result, err := HeaderForDiff(a, b)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Provenance != nil {
		t.Errorf("Provenance must be nil regardless of inputs, got %v", result.Provenance)
	}
}

// TestHeaderForDiff_Quality_AlwaysZero verifies that even when a and b carry a
// non-zero Quality (completeness gaps), the synthetic Delta's Quality is the
// zero value: Diff is a pure transformation and carries no quality signal
// (chain-lifecycle R-CL-036). Completeness is disclosed by a consumer at
// materialise time, not by the diff algebra.
func TestHeaderForDiff_Quality_AlwaysZero(t *testing.T) {
	// Covers: R-CL-036
	entity := eid(0x01)
	q := Quality{Gaps: []SequenceRange{{Start: 2, End: 4}}}
	a := Header{EntityID: entity, ChainID: "c", Sequence: 1, EffectiveAt: ts(0), PublishedAt: ts(0), Quality: q}
	b := Header{EntityID: entity, ChainID: "c", Sequence: 2, EffectiveAt: ts(1), PublishedAt: ts(1), Quality: q}

	result, err := HeaderForDiff(a, b)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Quality.Gaps != nil {
		t.Errorf("Quality.Gaps must be nil regardless of inputs, got %v", result.Quality.Gaps)
	}
}

// TestHeaderForDiff_Validation exercises every validation rule. Each subtest
// passes a pair (a, b) that violates exactly one invariant.
func TestHeaderForDiff_Validation(t *testing.T) {
	// Covers: R-DG-030
	entity := eid(0x01)

	base := func() (Header, Header) {
		a := Header{EntityID: entity, ChainID: "chain-1", Sequence: 3, EffectiveAt: ts(0), PublishedAt: ts(0)}
		b := Header{EntityID: entity, ChainID: "chain-1", Sequence: 7, EffectiveAt: ts(1), PublishedAt: ts(1)}
		return a, b
	}

	tests := []struct {
		name        string
		mutate      func(a, b *Header)
		wantErrFrag string
	}{
		{
			name:        "first_EntityID_zero",
			mutate:      func(a, _ *Header) { a.EntityID = EntityID{} },
			wantErrFrag: "first snapshot EntityID is zero",
		},
		{
			name:        "second_EntityID_zero",
			mutate:      func(_, b *Header) { b.EntityID = EntityID{} },
			wantErrFrag: "second snapshot EntityID is zero",
		},
		{
			name:        "EntityID_mismatch",
			mutate:      func(_, b *Header) { b.EntityID = eid(0x02) },
			wantErrFrag: "EntityID mismatch",
		},
		{
			name:        "ChainID_mismatch",
			mutate:      func(_, b *Header) { b.ChainID = "chain-2" },
			wantErrFrag: "ChainID mismatch",
		},
		{
			// chain-lifecycle R-CL-013: b.Sequence < a.Sequence is rejected.
			// b.Sequence == a.Sequence is explicitly allowed (identity diff).
			name:        "Sequence_lower",
			mutate:      func(a, b *Header) { b.Sequence = a.Sequence - 1 },
			wantErrFrag: "Sequence",
		},
		{
			name:        "EffectiveAt_before",
			mutate:      func(a, b *Header) { b.EffectiveAt = a.EffectiveAt.Add(-time.Second) },
			wantErrFrag: "EffectiveAt",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a, b := base()
			tc.mutate(&a, &b)

			result, err := HeaderForDiff(a, b)

			if err == nil {
				t.Fatalf("expected error for %q, got nil (result: %+v)", tc.name, result)
			}
			if tc.wantErrFrag != "" && !strings.Contains(err.Error(), tc.wantErrFrag) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.wantErrFrag)
			}
			if !isZeroHeader(result) {
				t.Errorf("expected zero Header on error, got %+v", result)
			}
		})
	}
}

// TestHeaderForDiff_SequenceEqual confirms that b.Sequence == a.Sequence is
// accepted. This is the identity-diff case: Diff(a, a) produces a Delta whose
// application leaves the payload unchanged. HeaderForDiff allows equal Sequences
// (>= not >) precisely to support this, unlike HeaderAfterApply which requires
// strict increase.
func TestHeaderForDiff_SequenceEqual(t *testing.T) {
	// Covers: R-DG-030 (boundary — equal Sequences allowed unlike HeaderAfterApply)
	entity := eid(0x01)
	instant := ts(5)

	a := Header{EntityID: entity, ChainID: "c", Sequence: 4, EffectiveAt: instant, PublishedAt: instant}
	b := Header{EntityID: entity, ChainID: "c", Sequence: 4, EffectiveAt: instant, PublishedAt: instant}

	result, err := HeaderForDiff(a, b)
	if err != nil {
		t.Errorf("equal Sequences should be accepted by HeaderForDiff, got error: %v", err)
	}
	if result.Sequence != 4 {
		t.Errorf("Sequence: got %d, want 4", result.Sequence)
	}
}

// TestHeaderForDiff_EffectiveAt_Equal confirms that equal EffectiveAt values
// are accepted (>= not >, same as HeaderAfterApply).
func TestHeaderForDiff_EffectiveAt_Equal(t *testing.T) {
	// Covers: R-DG-030 (boundary — equal EffectiveAt accepted)
	entity := eid(0x01)
	instant := ts(2)

	a := Header{EntityID: entity, ChainID: "c", Sequence: 1, EffectiveAt: instant, PublishedAt: instant}
	b := Header{EntityID: entity, ChainID: "c", Sequence: 2, EffectiveAt: instant, PublishedAt: instant}

	if _, err := HeaderForDiff(a, b); err != nil {
		t.Errorf("equal EffectiveAt should be accepted, got error: %v", err)
	}
}
