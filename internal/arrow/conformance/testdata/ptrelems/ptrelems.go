// Package ptrelems holds slices whose elements are pointers to non-struct
// types.
package ptrelems

import "time"

type PtrElems struct {
	Ints  []*int64
	Strs  []*string
	Times []*time.Time
}
