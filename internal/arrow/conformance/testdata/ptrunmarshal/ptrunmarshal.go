// Package ptrunmarshal holds a slice whose elements are pointers to an
// external TextUnmarshaler type.
package ptrunmarshal

import "go.resystems.io/eddt/internal/arrow/conformance/external"

type PtrUnmarshal struct {
	Items []*external.TextOnly
}
