// Package mapkeys holds maps keyed by types other than a basic type.
package mapkeys

import (
	"net/netip"
	"time"
)

type Key struct {
	A int64
}

type ArrayKey struct {
	M map[[2]byte]int64
}

type PtrKey struct {
	M map[*string]int64
}

type TextKey struct {
	M map[netip.Addr]int64
}

type TimeKey struct {
	M map[time.Time]int64
}

type StructKey struct {
	Keep string
	M    map[Key]int64
}
