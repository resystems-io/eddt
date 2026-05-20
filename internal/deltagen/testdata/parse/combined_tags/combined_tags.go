// Package combined_tags provides a Snapshot fixture that combines entity.key
// and delta.* tags on a single struct. It is used by T-03 tests to verify
// that all eddt: tags flow through the same parsed-tag code path and produce
// consistent Tag.Kind values regardless of tag axis.
package combined_tags

import eddt "go.resystems.io/eddt/runtime"

// Inner is the nested struct used by the delta.nested payload field.
type Inner struct{ X, Y int }

// CombinedTagsSnapshot exercises all four tag families simultaneously:
//   - entity.key  — key recognition via Tag.Kind (T-03 migration).
//   - delta.omit  — presence-axis tag on a scalar payload field.
//   - delta.retired,since=2026-05-20 — presence-axis tag with option (E-07).
//   - delta.nested — granularity-axis tag on a composite (struct value).
//   - (untagged)  — baseline: no eddt: tag, Tag.Kind == TagKindNone.
type CombinedTagsSnapshot struct {
	eddt.Header
	Key     string `eddt:"entity.key"`
	Omitted int    `eddt:"delta.omit"`
	Legacy  string `eddt:"delta.retired,since=2026-05-20"`
	Sub     Inner  `eddt:"delta.nested"`
	Plain   string
}
