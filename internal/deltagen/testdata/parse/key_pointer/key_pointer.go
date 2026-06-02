// Package key_pointer provides a Snapshot fixture whose entity.key field is
// a pointer type. R-DG-010 must reject pointer-typed key fields: pointer equality
// is identity, not value equality, which would make two Snapshots with equal
// key contents produce different EntityID hashes.
package key_pointer

import eddt "go.resystems.io/eddt/runtime"

// PtrKey is a comparable struct used as the pointee type. It is not the
// problem — the problem is that the Snapshot field below holds a pointer to
// it rather than a value.
type PtrKey struct{ ID string }

// PtrKeySnapshot tags a *PtrKey field as the entity key. parseKeyField (R-DG-010)
// must reject this with an error mentioning "pointer".
type PtrKeySnapshot struct {
	eddt.Header
	Key *PtrKey `eddt:"entity.key"`
}
