// Package no_key provides a conforming Snapshot fixture that omits the
// eddt:"entity.key" tag. It serves two R-DG-010 test cases:
//
//   - G.4 no-key error: parseSnapshot with default opts produces a "no field
//     tagged entity.key" error.
//   - G.3 override-OK: parseSnapshot with ParseOpts{KeyFieldOverride: "Peer"}
//     succeeds and surfaces Peer via KeyVar.
package no_key

import eddt "go.resystems.io/eddt/runtime"

// PeerKey is a comparable struct usable as an entity-key target via the
// ParseOpts.KeyFieldOverride hook. It is not annotated with the entity.key
// tag, so tag-path discovery does not pick it.
type PeerKey struct{ PeerID string }

// NoKeySnapshot embeds runtime.Header but has no field tagged
// eddt:"entity.key". The Peer field is a comparable struct type that the
// CLI override path can target without relying on a tag.
type NoKeySnapshot struct {
	eddt.Header
	Status int32
	Peer   PeerKey
}
