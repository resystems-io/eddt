// Package runtime provides the envelope types and contract functions that
// delta-gen-emitted code calls into. It is the minimum runtime surface
// specified in eddt-delta-gen-spec.md §6 and eddt-chain-lifecycle-spec.md §3.
//
// Scope: this package delivers the baseline (Phases 1–6) types and functions.
// The tri-state clearable extension — FieldDelta[T], FieldDeltaOp, and the
// three Op* constants — is added in Phase 7 (R-DG-026). The single-type-parameter
// ApplyFieldDelta from the spec is superseded (R-DG-007); the per-field Apply
// switch is generator-emitted (R-DG-016).
package runtime

import "time"

// EntityID is a fixed-width content-hash of a Snapshot's entity key struct,
// computed by delta-gen-emitted EntityID() methods using the hash helpers in
// hash.go. The uniform [32]byte type allows cross-chain entity-equality checks
// to be expressed as plain == comparisons (R-DG-034, R-DG-035).
type EntityID [32]byte

// IsZero reports whether the EntityID is the zero value (all bytes zero).
// HeaderAfterApply and HeaderForDiff reject any Header whose EntityID is zero.
func (e EntityID) IsZero() bool { return e == EntityID{} }

// Header is the envelope carried by every Snapshot and Delta on a chain.
// It encodes chain identity, sequence position, bitemporal timestamps, anchor
// fields (for chain birth/termination linking), and accumulated lineage.
//
// Per R-DG-034, R-DG-035, Header carries EntityID [32]byte rather than a typed entity
// key — the domain-specific key struct is embedded directly in the Snapshot and
// marked eddt:"entity.key"; delta-gen emits the EntityID() method on that struct.
type Header struct {
	// EntityID is the content-hash of the entity key. Populated via the
	// EntityID() method emitted by delta-gen on the Snapshot's key struct.
	EntityID EntityID

	// ChainID is the opaque chain identifier, unique across the deployment
	// and stable for the chain's lifetime (chain-lifecycle §3.1.2).
	ChainID string

	// PreviousChainID is set only on the birth Snapshot (Sequence == 0) when
	// the chain succeeds a predecessor. Nil on all other notifications.
	PreviousChainID *string

	// NextChainID is set only on the terminator Snapshot when a successor chain
	// exists. Nil on all other notifications.
	NextChainID *string

	// Closed is set only on the terminator Snapshot. Nil on all other
	// notifications. Wall-clock instant of chain close.
	Closed *time.Time

	// Sequence is the strictly monotonic position within the chain.
	// Sequence == 0 for the birth Snapshot; strictly increasing thereafter.
	Sequence uint64

	// EffectiveAt is the domain time when the state or change took effect.
	// Monotonic non-decreasing within a chain (Inv. 7).
	EffectiveAt time.Time

	// PublishedAt is the wall-clock instant the notification was emitted.
	// Independent of EffectiveAt.
	PublishedAt time.Time

	// Provenance accumulates lineage across tier translations. Append-only;
	// downstream producers must not rewrite, reorder, or remove upstream entries.
	Provenance []Provenance
}

// Provenance records a single component's contribution to a notification's
// lineage. A conforming producer appends one entry naming itself on every
// notification it emits; a conforming aggregator appends its own entry while
// preserving upstream entries (chain-lifecycle §3.2).
type Provenance struct {
	// PublishedAt is the wall-clock instant this component emitted its output.
	PublishedAt time.Time

	// ValidUntil is an optional producer freshness assertion. Nil means no
	// assertion; non-nil means the producer vouches for the fact until this time.
	ValidUntil *time.Time

	// Solution is the solution-level identifier for this component.
	Solution string

	// Component is the component-within-solution identifier.
	Component string

	// Instance is the instance-within-component identifier.
	Instance string

	// Metadata carries optional component-specific key-value pairs, opaque to
	// the runtime.
	Metadata map[string]string

	// Gaps is a snapshot-copy of this component's taint set for its contributing
	// source chain at emission time. Immutable once emitted; not revised by
	// subsequent late-arrival processing.
	Gaps []SequenceRange
}

// SequenceRange is a closed interval [Start, End] of Sequence numbers,
// used in Provenance.Gaps to record skipped positions in a source chain.
type SequenceRange struct {
	Start, End uint64 // inclusive
}

// FieldDeltaOp tags the operation carried by a FieldDelta on a clearable
// Delta field (delta-gen-spec §5.2). OpIgnore is the zero value, so a
// zero-valued FieldDelta is an explicit no-op.
type FieldDeltaOp uint8

const (
	OpIgnore  FieldDeltaOp = 0 // leave the field unchanged
	OpAssert  FieldDeltaOp = 1 // set the field to Value
	OpRetract FieldDeltaOp = 2 // reset the field to its zero value
)

// FieldDelta is the tri-state carrier for a clearable Delta field
// (delta-gen-spec §5.2 / chain-lifecycle §3.3). Per R-DG-007 the only
// clearable form is compositional, so T is always a generated inner *delta*
// type, never the Snapshot field type.
type FieldDelta[T any] struct {
	Op    FieldDeltaOp
	Value T // valid only when Op == OpAssert; ignored otherwise
}
