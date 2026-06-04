package multi_struct

import eddt "go.resystems.io/eddt/runtime"

// AlphaSnapshot and BetaSnapshot coexist in the same package, exercising
// multi-struct delta-gen output where both snapshots land in a single file
// via --out. With flat function names (Apply/Diff/Coalesce/EntityID) this
// would produce compile errors; with struct-prefixed names it compiles cleanly.

type AlphaSnapshot struct {
	eddt.Header
	Key   string `eddt:"entity.key"`
	Value int32
}

type BetaSnapshot struct {
	eddt.Header
	Key   string `eddt:"entity.key"`
	Score float32
}
