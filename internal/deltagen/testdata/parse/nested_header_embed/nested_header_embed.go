// Package nested_header_embed is a parse-error fixture: the delta.nested field
// Sub has a type that embeds runtime.Header, which is forbidden (§3.3.2).
// Used by TestParse_NestedFieldEmbedHeader.
package nested_header_embed

import eddt "go.resystems.io/eddt/runtime"

// AnchorSub embeds eddt.Header, making it a chain anchor — not a sub-structure.
// Using it as a delta.nested field type must be rejected at parse time.
type AnchorSub struct {
	eddt.Header
	X int32
}

// NestedHeaderEmbedSnapshot carries a delta.nested field whose type embeds
// runtime.Header; the parse stage must reject this with a §3.3.2 error.
type NestedHeaderEmbedSnapshot struct {
	eddt.Header
	Key string    `eddt:"entity.key"`
	Sub AnchorSub `eddt:"delta.nested"`
}
