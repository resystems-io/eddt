// Package nested_slice_reflect is the N-04 non-comparable emit fixture.
// It exercises delta.nested on a slice whose element type ([]byte) is not
// comparable in Go, triggering the O(n²) reflect.DeepEqual fallback path
// (§5.2, SliceElemUseReflectEq=true) and the conditional reflect import.
package nested_slice_reflect

import eddt "go.resystems.io/eddt/runtime"

// NestedSliceReflectSnapshot carries a delta.nested slice field whose element
// type ([]byte) is not comparable: the generator sets SliceElemUseReflectEq=true
// and emits reflect.DeepEqual for element equality in both Apply and Diff.
//
// Delta encoding (N-04, E-15 set-difference semantics):
//   - Blobs → AddedBlobs [][]byte + RemovedBlobs [][]byte
type NestedSliceReflectSnapshot struct {
	eddt.Header
	// Key is the entity key used for EntityID computation.
	Key string `eddt:"entity.key"`
	// Blobs is a []byte slice; []byte is not comparable, so the generated
	// code uses the O(n²) reflect.DeepEqual fallback (§5.2).
	Blobs [][]byte `eddt:"delta.nested"`
}
