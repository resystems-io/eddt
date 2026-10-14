package conformance

//go:generate go run go.resystems.io/eddt/cmd/arrow-writer-gen -p . -s ValueEmbedRecord,ShadowEmbedRecord,NestedEmbedRecord,PointerEmbedRecord,ExternalEmbedRecord,AmbiguousEmbedRecord -o embedding-arrow-writer.go -n conformance -v
//go:generate go run go.resystems.io/eddt/cmd/arrow-reader-gen -p . -s ValueEmbedRecord,ShadowEmbedRecord,NestedEmbedRecord,PointerEmbedRecord,ExternalEmbedRecord,AmbiguousEmbedRecord -o embedding-arrow-reader.go -n conformance -v

import "time"

// EmbedBase is embedded by the fixtures below in every form the generator
// distinguishes: by value, by pointer, through another embedding, and
// alongside an outer field that shadows one of its own.
type EmbedBase struct {
	BaseField string
	Shared    int64
}

// ValueEmbedRecord value-embeds EmbedBase. The documentation says the
// promoted fields flatten into the parent schema.
type ValueEmbedRecord struct {
	EmbedBase
	Extra int64
}

// ShadowEmbedRecord declares its own Shared, so Go promotes only BaseField
// from the embedding. The generator must follow the same rule.
type ShadowEmbedRecord struct {
	EmbedBase
	Shared string
}

// EmbedMid embeds EmbedBase and is itself embedded by NestedEmbedRecord,
// which makes BaseField and Shared two embeddings deep.
type EmbedMid struct {
	EmbedBase
	MidField string
}

// NestedEmbedRecord promotes MidField from one level down and BaseField and
// Shared from two levels down. Go sees all three.
type NestedEmbedRecord struct {
	EmbedMid
	Top string
}

// PointerEmbedRecord pointer-embeds EmbedBase, which the documentation says
// is skipped with a warning rather than flattened.
type PointerEmbedRecord struct {
	*EmbedBase
	Extra int64
}

// ExternalEmbedRecord embeds a struct from a package the generator has not
// loaded. Go promotes every exported time.Time method and field, there are
// no fields to promote, so the question is only whether generation survives.
type ExternalEmbedRecord struct {
	time.Time
	Name string
}

// EmbedA and EmbedB both declare Dup, so a struct embedding both cannot
// reach Dup at all. The generator must drop it without dropping A or B.
type EmbedA struct {
	A   int64
	Dup string
}

type EmbedB struct {
	B   int64
	Dup string
}

// AmbiguousEmbedRecord embeds EmbedA and EmbedB.
type AmbiguousEmbedRecord struct {
	EmbedA
	EmbedB
}
