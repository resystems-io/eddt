// Package genericexternal holds generics instantiated with types from
// packages the generator has not loaded.
package genericexternal

import (
	"net/netip"

	"go.resystems.io/eddt/internal/arrow/conformance/external"
)

type Optional[T any] struct {
	Value *T
	Set   bool
}

type ExternalArgs struct {
	Addr  Optional[netip.Addr]
	Level Optional[external.Level]
}
