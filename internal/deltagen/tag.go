package deltagen

// tag.go implements the eddt: struct tag parser for the delta-gen pipeline.
// It is the entry point for Phase 3 (R-DG-004, R-DG-005 through R-DG-006, R-DG-007):
//
//   - R-DG-004, R-DG-005 (this file): parse the raw eddt: tag string into a structured
//     ParsedTag carrying a TagKind and any comma-separated key=value options.
//   - R-DG-006, R-DG-007: wire parseTag into walkFields; validate tag combinations and
//     per-tag field-shape constraints.
//   - R-DG-006, R-DG-007: migrate all callers to ParsedTag.Kind; consolidate entity.key
//     and delta.* tag handling onto the same parsed-tag code path.
//
// delta.clearable is recognised as a secondary tag (R-DG-004, R-DG-007, Phase 7): it sets
// ParsedTag.Clearable and never occupies ParsedTag.Kind. The Clearable ⟹
// Nested semantic constraint is enforced by validateTagCombination (R-DG-007).

import (
	"fmt"
	"strings"
)

// TagKind identifies which eddt: tag value a payload field carries.
// The zero value (TagKindNone) means no eddt: tag is present.
type TagKind int

const (
	// TagKindNone is the zero value: the field carries no eddt: struct tag.
	TagKindNone TagKind = iota

	// TagKindEntityKey corresponds to eddt:"entity.key". The tagged field is
	// the entity-key field recognised by parseKeyField (G-04). Wired into the
	// parse pipeline in R-DG-006, R-DG-007.
	TagKindEntityKey

	// TagKindNested corresponds to eddt:"delta.nested". The tagged field is
	// a struct value for which the generator recurses and emits a companion
	// <T>Delta type (delta-gen spec §9.2; Phase 5 R-DG-016).
	TagKindNested

	// TagKindOmit corresponds to eddt:"delta.omit". The tagged field is
	// excluded from the generated Delta type; Apply propagates the prior
	// Snapshot value unchanged (delta-gen spec §9.3).
	TagKindOmit

	// TagKindRetired corresponds to eddt:"delta.retired". The tagged field
	// is kept in the Snapshot for backward compatibility but excluded from
	// new Delta emissions; Apply propagates the prior value unchanged
	// (delta-gen spec §9.4). The since=<date> option is preserved.
	TagKindRetired

	// TagKindCommutative corresponds to eddt:"delta.commutative". Reserved
	// for future late-arrival-lift semantics; v1 generators accept the tag
	// without semantic effect — the tagged field emits as if untagged
	// (delta-gen spec §9.5).
	TagKindCommutative

	// TagKindClearable corresponds to eddt:"delta.clearable". It is a SECONDARY
	// tag: it never occupies ParsedTag.Kind (which holds the single primary tag)
	// — instead it sets ParsedTag.Clearable. Per R-DG-007 it is admissible
	// only combined with delta.nested; that constraint is enforced in R-DG-007.
	TagKindClearable
)

// Canonical eddt: tag value strings. Used in tagKindFor, String, and error
// messages so every site agrees on the exact spelling.
const (
	tagEntityKey        = "entity.key"
	tagDeltaNested      = "delta.nested"
	tagDeltaOmit        = "delta.omit"
	tagDeltaRetired     = "delta.retired"
	tagDeltaCommutative = "delta.commutative"
	tagDeltaClearable   = "delta.clearable"
)

// String returns the canonical eddt: tag string for k, e.g. "delta.nested".
// The zero value returns "none". Used in error messages so callers see readable
// tag names rather than raw integers.
func (k TagKind) String() string {
	switch k {
	case TagKindEntityKey:
		return tagEntityKey
	case TagKindNested:
		return tagDeltaNested
	case TagKindOmit:
		return tagDeltaOmit
	case TagKindRetired:
		return tagDeltaRetired
	case TagKindCommutative:
		return tagDeltaCommutative
	case TagKindClearable:
		return tagDeltaClearable
	default:
		return "none"
	}
}

// IsSecondary reports whether k is a secondary (modifier) tag that combines
// with a primary tag rather than occupying ParsedTag.Kind. delta.clearable is
// the only secondary kind.
func (k TagKind) IsSecondary() bool { return k == TagKindClearable }

// ParsedTag is the structured result of parsing a single eddt: tag value.
// It carries the recognised TagKind and any comma-separated key=value options
// present after the tag value. Unknown option keys are preserved without
// acting on them (R-DG-005).
type ParsedTag struct {
	Kind TagKind

	// Clearable is true when the comma-separated tag list included the
	// secondary delta.clearable modifier. Per R-DG-007 it is meaningful only
	// alongside delta.nested (Kind == TagKindNested); R-DG-007 enforces that
	// and R-DG-016 emits the FieldDelta[T] envelope.
	Clearable bool

	// Options holds the key=value pairs from the comma-separated option list
	// (e.g. since=2026-01-15 for delta.retired). Unknown keys are preserved.
	// nil when no options are present.
	Options map[string]string

	// Raw is the verbatim eddt: tag value supplied to parseTag. Preserved for
	// diagnostics, downstream dumps, and error-message context. Empty for the
	// zero value (absent tag).
	Raw string
}

// parseTag parses the raw value of an eddt: struct tag. The raw string is the
// value returned by reflect.StructTag.Get("eddt") — e.g. "delta.nested",
// "delta.retired,since=2026-01-15", "delta.nested,delta.clearable", or ""
// for an absent tag.
//
// Each comma-separated part is classified by form:
//   - Contains "=" → a key=value option (unknown keys preserved per R-DG-005).
//   - No "="  → a tag token (looked up via tagKindFor).
//     Secondary tokens (IsSecondary) set ParsedTag.Clearable (or the
//     appropriate bool for future secondaries). Primary tokens set Kind;
//     a second primary is an error.
//
// Rules:
//   - An empty raw string produces TagKindNone with no options.
//   - Recognised primary tags: "entity.key", "delta.nested", "delta.omit",
//     "delta.retired", "delta.commutative". Anything else is an error.
//   - Recognised secondary tags: "delta.clearable" (R-DG-004, R-DG-007).
//   - Two primary tokens in one tag value is an error.
//   - An empty key (e.g. "=value") is an error.
//   - An empty value (e.g. "k=") is accepted; the key maps to "".
func parseTag(raw string) (ParsedTag, error) {
	if raw == "" {
		return ParsedTag{Kind: TagKindNone, Raw: raw}, nil
	}

	result := ParsedTag{Kind: TagKindNone, Raw: raw}
	primarySet := false

	for part := range strings.SplitSeq(raw, ",") {
		if key, val, ok := strings.Cut(part, "="); ok {
			if key == "" {
				return ParsedTag{}, fmt.Errorf("malformed eddt: tag option %q: key must not be empty", part)
			}
			if result.Options == nil {
				result.Options = make(map[string]string)
			}
			result.Options[key] = val
			continue
		}

		kind, err := tagKindFor(part)
		if err != nil {
			return ParsedTag{}, err
		}

		if kind.IsSecondary() {
			switch kind {
			case TagKindClearable:
				result.Clearable = true
			}
			continue
		}

		if primarySet {
			return ParsedTag{}, fmt.Errorf("multiple primary eddt: tags in %q: %s and %s", raw, result.Kind, kind)
		}
		result.Kind = kind
		primarySet = true
	}

	return result, nil
}

// validateTagShape returns an error if a tag is incompatible with a field
// shape under the harmonised three-axis model (refinements §1.6.3;
// R-DG-004, R-DG-005, R-DG-006, R-DG-007, R-DG-016, R-DG-007).
//
// Baseline rules (this function in Phase 3):
//   - TagKindNested: requires a composite shape (struct value, slice, map).
//     Rejected on scalar and pointer — there is no decomposition axis to
//     flip on a non-composite shape.
//   - TagKindOmit / TagKindRetired / TagKindCommutative: admitted on any
//     shape (presence axis is shape-agnostic).
//   - TagKindEntityKey: shape validation lives in parseKeyField, not
//     this gate — value-typed-comparable enforcement is the key-field
//     responsibility, not the tag-shape gate's.
//   - TagKindNone: no tag, no constraint.
//
// R-DG-007 (Phase 7) extends this function to gate TagKindClearable (admitted
// on every shape per R-DG-007) and the nested + clearable combination (R-DG-007, R-DG-016).
func validateTagShape(tag ParsedTag, shape FieldShape) error {
	switch tag.Kind {
	case TagKindNested:
		switch shape {
		case ShapeScalar, ShapePointer:
			return fmt.Errorf(
				"eddt:%q requires a composite field shape (struct value, slice, map); got %v",
				tagDeltaNested, shape)
		}
	}
	return nil
}

// validateTagCombination enforces the envelope-axis constraint from
// R-DG-007: a field carrying the secondary delta.clearable modifier MUST also
// carry the primary delta.nested tag. Tri-state is the implicit norm for
// every other field shape — atomic, pointer, and struct-value fields already
// distinguish ignore / reset / assert through their generated delta pointer —
// so delta.clearable is meaningful (and admissible) only on a compositional
// (delta.nested) field. This single predicate subsumes the former
// delta.clearable + delta.omit ban: omit/retired occupy Kind with a non-Nested
// primary and so fail the same check.
//
// The orthogonal "delta.nested requires a composite shape" rule is enforced
// separately by validateTagShape, so delta.nested,delta.clearable on a
// scalar/pointer is rejected by that gate before this predicate is reached.
func validateTagCombination(tag ParsedTag) error {
	if tag.Clearable && tag.Kind != TagKindNested {
		return fmt.Errorf(
			"eddt:%q requires eddt:%q: the clearable envelope applies only to "+
				"compositional fields; drop %s (atomic, pointer, and struct-value "+
				"fields already carry tri-state via their delta pointer) or add %s",
			tagDeltaClearable, tagDeltaNested, tagDeltaClearable, tagDeltaNested)
	}
	return nil
}

// tagKindFor maps a tag value string to its TagKind.
func tagKindFor(tagVal string) (TagKind, error) {
	switch tagVal {
	case tagEntityKey:
		return TagKindEntityKey, nil
	case tagDeltaNested:
		return TagKindNested, nil
	case tagDeltaOmit:
		return TagKindOmit, nil
	case tagDeltaRetired:
		return TagKindRetired, nil
	case tagDeltaCommutative:
		return TagKindCommutative, nil
	case tagDeltaClearable:
		return TagKindClearable, nil
	default:
		return TagKindNone, fmt.Errorf(
			"unrecognised eddt: tag value %q; valid primary tags: %s, %s, %s, %s, %s; secondary: %s",
			tagVal,
			tagEntityKey, tagDeltaNested, tagDeltaOmit, tagDeltaRetired, tagDeltaCommutative,
			tagDeltaClearable)
	}
}
