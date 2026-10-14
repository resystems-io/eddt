// Package tworeaders holds two structs with TextUnmarshaler fields, meant to
// be generated in two separate reader invocations.
package tworeaders

import "net/netip"

type First struct {
	Addr netip.Addr
}

type Second struct {
	Peer netip.Addr
}
