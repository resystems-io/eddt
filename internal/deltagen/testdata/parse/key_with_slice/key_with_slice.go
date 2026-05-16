// Package key_with_slice provides a Snapshot fixture whose entity.key field
// is a struct value containing a non-comparable (slice) sub-field. G-04 must
// reject this because slices cannot be compared with `==`, so the key struct
// as a whole is not comparable.
package key_with_slice

import eddt "go.resystems.io/eddt/runtime"

// SliceyKey contains a slice field. types.Comparable(SliceyKey) returns false
// because slices are not comparable in Go's type system. G-04's error message
// must name the offending sub-field ("IDs") so the Snapshot author can locate
// the problem quickly.
type SliceyKey struct {
	Name string
	IDs  []string
}

// SliceyKeySnapshot tags a SliceyKey-valued field as the entity key.
type SliceyKeySnapshot struct {
	eddt.Header
	Key SliceyKey `eddt:"entity.key"`
}
