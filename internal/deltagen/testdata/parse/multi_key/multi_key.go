// Package multi_key provides a Snapshot fixture with two fields tagged
// eddt:"entity.key". G-04 must reject this with a "multiple" error: at most
// one entity-key field is permitted per Snapshot.
package multi_key

import eddt "go.resystems.io/eddt/runtime"

// KeyA is one of two comparable key-struct candidates in MultiKeySnapshot.
type KeyA struct{ ID string }

// KeyB is the second candidate; both are tagged entity.key in the Snapshot,
// which is the error condition this fixture exercises.
type KeyB struct{ Name string }

// MultiKeySnapshot has two entity.key-tagged fields. G-04's parseKeyField
// must return an error mentioning "multiple" when it encounters this shape.
type MultiKeySnapshot struct {
	eddt.Header
	First  KeyA `eddt:"entity.key"`
	Second KeyB `eddt:"entity.key"`
}
