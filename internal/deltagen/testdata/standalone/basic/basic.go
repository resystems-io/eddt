package basic

// BasicSnapshot is a minimal Snapshot without runtime.Header, used to test
// standalone mode code generation. All fields are scalar — no nested types or
// clearable fields — so the generated file has no imports.
//
//go:generate delta-gen --standalone BasicSnapshot
type BasicSnapshot struct {
	ID    string `eddt:"entity.key"`
	Color string
	Count int32
}
