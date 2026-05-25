// Package nested_slice is the N-04 emit fixture. NestedSliceSnapshot exercises
// two slice fields with set-difference delta encoding (E-15):
//
//   - Names uses a scalar element type ([]string); generated code uses an O(n)
//     map[string]struct{} membership set for both Apply and Diff.
//   - Tags uses a comparable struct element type ([]Tag); all Tag fields are
//     scalar so Tag is comparable in Go, and the generator also uses == / map set.
//
// Set-difference semantics: AddedX = b.X ∖ a.X, RemovedX = a.X ∖ b.X.
// Apply: removals filtered first (survivor order preserved per E-03), then
// additions appended. Element equality for comparable types uses == (§5.2).
package nested_slice

import eddt "go.resystems.io/eddt/runtime"

// Tag is a slice element type used to exercise comparable struct comparison.
// All fields are scalar, so Tag is comparable in Go: the generator emits ==
// and the O(n) map-set path rather than reflect.DeepEqual.
type Tag struct {
	// Key is the tag key string.
	Key string
	// Val is the tag value string.
	Val string
}

// NestedSliceSnapshot is the root Snapshot processed by the generator.
//
// Delta encoding for slice fields (N-04, E-15 set-difference semantics):
//   - Names  → AddedNames []string  + RemovedNames []string
//   - Tags   → AddedTags  []Tag     + RemovedTags  []Tag
//   - Count remains an atomic *int32 field in NestedSliceSnapshotDelta.
type NestedSliceSnapshot struct {
	eddt.Header
	// Key is the entity key used for EntityID computation.
	Key string `eddt:"entity.key"`
	// Names is a string slice encoded with set-difference delta (N-04).
	Names []string `eddt:"delta.nested"`
	// Tags is a Tag slice encoded with set-difference delta (N-04).
	// Tag is comparable, so generated code uses == and the O(n) map-set path.
	Tags []Tag `eddt:"delta.nested"`
	// Count is a plain atomic field; changes produce SetCount *int32 in the delta.
	Count int32
}
