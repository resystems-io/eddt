// Package runtime provides the envelope types and contract functions that
// delta-gen-emitted code calls into. It is the minimum runtime surface
// specified in eddt-delta-gen-spec.md and eddt-chain-lifecycle-spec.md.
//
// The tri-state clearable extension — FieldDelta[T], FieldDeltaOp, and the
// three Op* constants — supports R-DG-026. The per-field Apply switch is
// generator-emitted (R-DG-016).
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
// fields (for chain birth/termination linking), and two orthogonal cross-tier
// axes: accumulated lineage (Provenance — where the data came from) and
// data-quality signals (Quality — how complete it is).
//
// Per R-DG-034, R-DG-035, Header carries EntityID [32]byte rather than a typed entity
// key — the domain-specific key struct is embedded directly in the Snapshot and
// marked eddt:"entity.key"; delta-gen emits the EntityID() method on that struct.
type Header struct {
	// EntityID is the content-hash of the entity key. Populated via the
	// EntityID() method emitted by delta-gen on the Snapshot's key struct.
	EntityID EntityID

	// ChainID is the opaque chain identifier, unique across the deployment
	// and stable for the chain's lifetime (chain-lifecycle R-CL-003).
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
	// Monotonic non-decreasing within a chain (R-CL-017).
	EffectiveAt time.Time

	// PublishedAt is the wall-clock instant the notification was emitted.
	// Independent of EffectiveAt.
	PublishedAt time.Time

	// Provenance accumulates lineage (the provenance axis) across tier
	// translations. Append-only; downstream producers must not rewrite, reorder,
	// or remove upstream entries (chain-lifecycle R-CL-004, R-CL-018).
	Provenance Provenance

	// Quality carries data-quality signals (the quality axis) for this
	// notification — at minimum own-chain completeness (Gaps). It is stamped by a
	// consumer at materialisation, not by the Apply algebra (chain-lifecycle
	// R-CL-036, R-CL-016); it is the zero value on producer emissions.
	Quality Quality
}

// Provenance is the append-only lineage of a notification — the ordered
// sequence of Origin entries naming the components and sources that contributed
// to it (chain-lifecycle R-CL-004). It is the provenance axis: a *where-from*
// record, carrying no data-quality information. Apply accumulates it by
// concatenation (R-CL-018). The provenance axis grows by extending Origin.
type Provenance []Origin

// Origin records a single component's contribution to a notification's lineage.
// A conforming producer appends one entry naming itself on every notification it
// emits; a conforming aggregator appends its own entry while preserving upstream
// entries (chain-lifecycle R-CL-004). Origin records lineage only — completeness
// lives on the quality axis (Quality).
type Origin struct {
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
}

// Quality carries the data-quality assessment of a notification — the quality
// axis (chain-lifecycle R-CL-036; platform need N-EDDT-008). At minimum it
// carries own-chain completeness via Gaps. It is extensible to further quality
// signals (e.g. confidence or fidelity scores) without altering the Header.
type Quality struct {
	// Gaps are the Sequence positions of this chain that are missing from the
	// materialised state — own-chain completeness, in this chain's own Sequence
	// space. A consumer stamps this from its taint set at materialisation
	// (R-CL-019, R-CL-031); the Apply algebra never computes it (R-CL-016).
	Gaps []SequenceRange
}

// SequenceRange is a closed interval [Start, End] of Sequence numbers, used in
// Quality.Gaps to record missing positions in a chain.
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
// (delta-gen-spec §5.2 / chain-lifecycle R-CL-006). Per R-DG-007 the only
// clearable form is compositional, so T is always a generated inner *delta*
// type, never the Snapshot field type.
type FieldDelta[T any] struct {
	Op    FieldDeltaOp
	Value T // valid only when Op == OpAssert; ignored otherwise
}
