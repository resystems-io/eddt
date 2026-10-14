// Package rejected holds the shapes the generator rejects cleanly, with a
// warning and no effect on the other fields.
package rejected

type Inner struct {
	A int64
}

type Key struct {
	A int64
}

type Rejected struct {
	Keep       string
	DoublePtr  **Inner
	PtrArray   *[2]int32
	StructKeys map[Key]int64
	Keep2      int64
}
