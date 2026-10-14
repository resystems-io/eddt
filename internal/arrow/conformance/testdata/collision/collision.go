// Package collision defines Inner and references another package's Inner.
package collision

import "go.resystems.io/eddt/internal/arrow/conformance/testdata/collision/other"

type Inner struct {
	A int64
}

type Outer struct {
	Mine   Inner
	Theirs other.Inner
}
