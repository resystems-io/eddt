package deltagen

// tag.go implements the eddt: struct tag parser for the delta-gen pipeline.
// It is the entry point for Phase 3 (T-01 through T-03):
//
//   - T-01 (this file): parse the raw eddt: tag string into a structured
//     ParsedTag carrying a TagKind and any comma-separated key=value options.
//   - T-02: wire parseTag into walkFields; validate tag combinations and
//     per-tag field-shape constraints.
//   - T-03: migrate parseKeyField and the generator conflict warning to use
//     ParsedTag.Kind instead of literal RawTag string comparisons.
//
// delta.clearable is not recognised in the baseline generator; it is added
// in Phase 7 (CL-03) alongside the runtime FieldDelta[T] support.

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
	// parse pipeline in T-03.
	TagKindEntityKey

	// TagKindNested corresponds to eddt:"delta.nested". The tagged field is
	// a struct value for which the generator recurses and emits a companion
	// <T>Delta type (delta-gen spec §9.2; Phase 5 N-01).
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

	// TagKindClearable (delta.clearable) is deferred to Phase 7 (CL-03).
	// parseTag returns an error for this tag value in the baseline generator.
)

// ParsedTag is the structured result of parsing a single eddt: tag value.
// It carries the recognised TagKind and any comma-separated key=value options
// present after the tag value. Unknown option keys are preserved without
// acting on them (Errata E-07).
type ParsedTag struct {
	Kind TagKind

	// Options holds the key=value pairs from the comma-separated option list
	// (e.g. since=2026-01-15 for delta.retired). Unknown keys are preserved.
	// nil when no options are present.
	Options map[string]string
}

// parseTag parses the raw value of an eddt: struct tag. The raw string is the
// value returned by reflect.StructTag.Get("eddt") — e.g. "delta.nested",
// "delta.retired,since=2026-01-15", or "" for an absent tag.
//
// Grammar (Errata E-07): <tagvalue>[,<key>=<value>[,<key>=<value>...]]
//
// Rules:
//   - An empty raw string produces TagKindNone with no options.
//   - Recognised tag values: "entity.key", "delta.nested", "delta.omit",
//     "delta.retired", "delta.commutative". Anything else, including
//     "delta.clearable" (deferred to CL-03), is an error.
//   - Options are comma-separated key=value pairs. Unknown keys are
//     preserved in Options without acting on them.
//   - A bare option with no "=" separator is an error.
//   - An empty key (e.g. "=value") is an error.
//   - An empty value (e.g. "k=") is accepted; the key maps to "".
func parseTag(raw string) (ParsedTag, error) {
	if raw == "" {
		return ParsedTag{Kind: TagKindNone}, nil
	}

	parts := strings.Split(raw, ",")
	tagVal := parts[0]

	kind, err := tagKindFor(tagVal)
	if err != nil {
		return ParsedTag{}, err
	}

	if len(parts) == 1 {
		return ParsedTag{Kind: kind}, nil
	}

	opts := make(map[string]string, len(parts)-1)
	for _, opt := range parts[1:] {
		idx := strings.Index(opt, "=")
		if idx < 0 {
			return ParsedTag{}, fmt.Errorf("malformed eddt: tag option %q: expected key=value format", opt)
		}
		key := opt[:idx]
		if key == "" {
			return ParsedTag{}, fmt.Errorf("malformed eddt: tag option %q: key must not be empty", opt)
		}
		opts[key] = opt[idx+1:]
	}

	return ParsedTag{Kind: kind, Options: opts}, nil
}

// tagKindFor maps a tag value string to its TagKind.
func tagKindFor(tagVal string) (TagKind, error) {
	switch tagVal {
	case "entity.key":
		return TagKindEntityKey, nil
	case "delta.nested":
		return TagKindNested, nil
	case "delta.omit":
		return TagKindOmit, nil
	case "delta.retired":
		return TagKindRetired, nil
	case "delta.commutative":
		return TagKindCommutative, nil
	default:
		return TagKindNone, fmt.Errorf("unrecognised eddt: tag value %q", tagVal)
	}
}
