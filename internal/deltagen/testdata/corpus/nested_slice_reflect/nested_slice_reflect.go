// Package nested_slice_reflect is the HK-18 benchmark corpus fixture for a
// delta.nested slice field with non-comparable element type ([]byte).
//
// The generator sets SliceElemUseReflectEq=true and emits reflect.DeepEqual
// for element membership tests, exercising the O(n²) reflect.DeepEqual
// fallback path (§5.2) for Diff/Apply.
//
// This fixture is used by TestBenchmark_NestedSliceReflect only; it is not
// in the corpus var (no conformance property tests are added for it).
package nested_slice_reflect

import eddt "go.resystems.io/eddt/runtime"

// NestedSliceReflectSnapshot carries a delta.nested slice field whose
// element type ([]byte) is not comparable.
type NestedSliceReflectSnapshot struct {
	eddt.Header
	Key   string   `eddt:"entity.key"`
	Blobs [][]byte `eddt:"delta.nested"`
}
