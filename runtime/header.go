package runtime

import (
	"errors"
	"fmt"
)

// HeaderAfterApply produces the result Header for a generated Apply(s, d)
// invocation, where s is the prior Snapshot's Header and d is the applied
// Delta's Header. It performs all chain-envelope validations mandated by
// chain-lifecycle-spec.md §6.1 plus the EntityID zero-rejection required by
// R-DG-034, R-DG-035. Delta-gen-emitted Apply methods call this function exactly once
// and must not replicate or bypass these validations (delta-gen-spec.md §5.2).
//
// On success the returned Header carries:
//   - EntityID and ChainID propagated from s (validated equal to d).
//   - PreviousChainID, NextChainID, Closed all nil (result is mid-chain, not an anchor).
//   - Sequence, EffectiveAt, PublishedAt taken from d.
//   - Provenance = s.Provenance ⊕ d.Provenance (append-only concatenation).
//
// On any validation failure HeaderAfterApply returns a zero Header and a
// non-nil error describing the violated invariant. The caller's Apply method
// must propagate the error; it must not use the zero Header.
func HeaderAfterApply(s, d Header) (Header, error) {
	// 1. Reject zero EntityIDs (R-DG-034, R-DG-035).
	//    An all-zero EntityID signals that the caller forgot to populate the
	//    entity key struct with its content-hash, or that a zero-value Header
	//    was passed in. Both inputs are checked independently so the error
	//    message identifies which side is missing.
	if s.EntityID.IsZero() {
		return Header{}, errors.New("HeaderAfterApply: snapshot EntityID is zero")
	}
	if d.EntityID.IsZero() {
		return Header{}, errors.New("HeaderAfterApply: delta EntityID is zero")
	}

	// 2. Validate entity integrity (chain-lifecycle §6.1, Inv. 4 — entity).
	//    Both notifications must belong to the same logical entity. A mismatch
	//    means two different real-world objects are being conflated on one chain,
	//    which is always a producer bug.
	if s.EntityID != d.EntityID {
		return Header{}, errors.New("HeaderAfterApply: EntityID mismatch between snapshot and delta")
	}

	// 3. Validate chain integrity (chain-lifecycle §6.1, Inv. 4 — chain).
	//    Both notifications must belong to the same chain. Cross-chain
	//    application is categorically disallowed; the consumer state machine
	//    is per-chain and cannot handle cross-chain merges.
	if s.ChainID != d.ChainID {
		return Header{}, fmt.Errorf("HeaderAfterApply: ChainID mismatch (snapshot %q, delta %q)",
			s.ChainID, d.ChainID)
	}

	// 4. Validate Sequence strict monotonicity (chain-lifecycle §6.1, Inv. 5/6).
	//    d.Sequence must be strictly greater than s.Sequence. Equal or lower
	//    Sequences indicate a duplicate, an out-of-order application, or a
	//    producer that failed to increment. Note that gaps are allowed (Inv. 6):
	//    d.Sequence = s.Sequence + 100 is valid; the skipped Sequences become
	//    taint entries in the consumer's state machine.
	if d.Sequence <= s.Sequence {
		return Header{}, fmt.Errorf(
			"HeaderAfterApply: delta Sequence %d must be strictly greater than snapshot Sequence %d",
			d.Sequence, s.Sequence)
	}

	// 5. Validate EffectiveAt non-decrease (chain-lifecycle §6.1, Inv. 7).
	//    Domain time must not roll backwards within a chain. Simultaneous changes
	//    sharing the same EffectiveAt instant are permitted (>= not >), but a
	//    strictly earlier EffectiveAt on the delta indicates a domain-model error
	//    or a mis-ordered application.
	if d.EffectiveAt.Before(s.EffectiveAt) {
		return Header{}, fmt.Errorf(
			"HeaderAfterApply: delta EffectiveAt %v is before snapshot EffectiveAt %v",
			d.EffectiveAt, s.EffectiveAt)
	}

	// 6. Validate chain finiteness (chain-lifecycle §6.1, Inv. 12).
	//    A non-nil Closed on the snapshot means the chain was already terminated
	//    by a prior terminator Snapshot. No further notifications may be applied
	//    to a closed chain; the consumer must free its per-chain waiting state
	//    (frontier, taint set) upon receiving the terminator.
	if s.Closed != nil {
		return Header{}, errors.New("HeaderAfterApply: snapshot chain is already closed (Closed != nil)")
	}

	// All validations passed. Construct the result Header per chain-lifecycle §6.1.

	// EntityID and ChainID propagate from s. Both are guaranteed equal to d by
	// the checks above; we use s as the canonical source.
	// PreviousChainID, NextChainID, and Closed are left at their zero value (nil):
	// Apply's result is always a mid-chain notification, never a birth Snapshot
	// (PreviousChainID) or a terminator (NextChainID, Closed).
	// Sequence, EffectiveAt, and PublishedAt come from d — the delta advances all
	// of these forward within the chain.
	result := Header{
		EntityID:    s.EntityID,
		ChainID:     s.ChainID,
		Sequence:    d.Sequence,
		EffectiveAt: d.EffectiveAt,
		PublishedAt: d.PublishedAt,
	}

	// Provenance = s.Provenance ⊕ d.Provenance (chain-lifecycle §3.2.1).
	// We always allocate a fresh slice so that subsequent appends to s.Provenance
	// or d.Provenance cannot silently mutate result.Provenance (or vice versa).
	// When both inputs are nil the double-append produces nil, preserving the
	// zero-value convention for "no lineage recorded yet".
	result.Provenance = append(append([]Provenance(nil), s.Provenance...), d.Provenance...)

	return result, nil
}

// HeaderForDiff produces the Header for the Delta d such that Apply(a, d) == b,
// where a and b are two Snapshots on the same chain. It performs the validations
// mandated by chain-lifecycle-spec.md §6.2 plus EntityID zero-rejection
// (R-DG-034, R-DG-035). Delta-gen-emitted Diff methods call this function exactly once and must
// not replicate or bypass these validations (delta-gen-spec.md §5.2).
//
// On success the returned Header carries:
//   - EntityID and ChainID propagated from b (validated equal to a).
//   - PreviousChainID, NextChainID, Closed all nil (result is a synthetic Delta).
//   - Sequence, EffectiveAt, PublishedAt taken from b.
//   - Provenance = nil (the default; call sites that need non-empty Provenance
//     — coalescing aggregators, audit pipelines — append their entries after
//     HeaderForDiff returns, rather than encoding a policy in this function).
//
// Unlike HeaderAfterApply, HeaderForDiff allows b.Sequence == a.Sequence. This
// supports the identity-diff case Diff(a, a), where both Snapshots have the same
// Sequence, and whose application Apply(a, Diff(a, a)) leaves payload unchanged.
//
// On any validation failure HeaderForDiff returns a zero Header and a non-nil
// error. The caller's Diff method must propagate the error.
func HeaderForDiff(a, b Header) (Header, error) {
	// 1. Reject zero EntityIDs (R-DG-034, R-DG-035).
	//    Same rationale as HeaderAfterApply: zero EntityID indicates an
	//    uninitialized key struct rather than a valid entity.
	if a.EntityID.IsZero() {
		return Header{}, errors.New("HeaderForDiff: first snapshot EntityID is zero")
	}
	if b.EntityID.IsZero() {
		return Header{}, errors.New("HeaderForDiff: second snapshot EntityID is zero")
	}

	// 2. Validate entity integrity (chain-lifecycle §6.2 — entity).
	//    Both snapshots must belong to the same logical entity. Computing a diff
	//    between two different entities would produce a semantically meaningless
	//    delta.
	if a.EntityID != b.EntityID {
		return Header{}, errors.New("HeaderForDiff: EntityID mismatch between snapshots")
	}

	// 3. Validate chain integrity (chain-lifecycle §6.2 — chain).
	//    Both snapshots must be on the same chain. Cross-chain diffs are
	//    undefined — ChainID is part of the Header of the produced Delta and
	//    must be a single, consistent value.
	if a.ChainID != b.ChainID {
		return Header{}, fmt.Errorf("HeaderForDiff: ChainID mismatch (%q vs %q)", a.ChainID, b.ChainID)
	}

	// 4. Validate Sequence ordering (chain-lifecycle §6.2, Inv. 5).
	//    b.Sequence must be >= a.Sequence. Equal Sequences are permitted to
	//    support the identity-diff Diff(a, a) (where Sequence is the same on
	//    both sides). Strictly lower Sequences indicate that b is an earlier
	//    state than a, which would produce a "backwards" delta.
	if b.Sequence < a.Sequence {
		return Header{}, fmt.Errorf(
			"HeaderForDiff: second snapshot Sequence %d is less than first snapshot Sequence %d",
			b.Sequence, a.Sequence)
	}

	// 5. Validate EffectiveAt ordering (chain-lifecycle §6.2, Inv. 7).
	//    b.EffectiveAt must be >= a.EffectiveAt. Same rationale as Sequence:
	//    the diff represents the change from a to b, so b must not be earlier
	//    in domain time than a.
	if b.EffectiveAt.Before(a.EffectiveAt) {
		return Header{}, fmt.Errorf(
			"HeaderForDiff: second snapshot EffectiveAt %v is before first snapshot EffectiveAt %v",
			b.EffectiveAt, a.EffectiveAt)
	}

	// All validations passed. Construct the result Header per chain-lifecycle §6.2.

	// EntityID and ChainID come from b (validated equal to a).
	// Sequence, EffectiveAt, and PublishedAt come from b: the produced Delta
	// describes the change needed to reach state b, so b's position metadata
	// is the correct position for the Delta.
	// PreviousChainID, NextChainID, and Closed are left at their zero value (nil):
	// the produced Delta is a synthetic, mid-chain notification, not an anchor.
	// Provenance is intentionally nil (the default): Diff is a pure transformation
	// that does not carry lineage. Call sites that need provenance entries — e.g.
	// a coalescing aggregator recording source lineage — append them after this
	// function returns.
	return Header{
		EntityID:    b.EntityID,
		ChainID:     b.ChainID,
		Sequence:    b.Sequence,
		EffectiveAt: b.EffectiveAt,
		PublishedAt: b.PublishedAt,
	}, nil
}
