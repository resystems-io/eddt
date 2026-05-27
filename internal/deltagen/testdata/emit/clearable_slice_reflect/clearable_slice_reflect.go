// Package clearable_slice_reflect is the CL-05..07 emit fixture for a
// delta.nested+delta.clearable slice field with a non-comparable element type.
//
// The element type ([]byte) is not comparable, so the generated
// DiffBlobsSliceDelta must use the O(n²) reflect.DeepEqual fallback
// (WrapperUseReflectEq=true). Verifies NeedsReflect propagation and the
// {{if .WrapperUseReflectEq}} branch in sliceWrapper.
//
// The fixture is consumed by TestEmitTemplate_Clearable_Slice_Reflect_SamePkg.
package clearable_slice_reflect

import eddt "go.resystems.io/eddt/runtime"

// ClearableSliceReflectSnapshot exercises a clearable slice field with
// non-comparable elements.
type ClearableSliceReflectSnapshot struct {
	eddt.Header
	// Key is the entity key.
	Key string `eddt:"entity.key"`
	// Blobs is a clearable slice of []byte → reflect.DeepEqual path.
	Blobs [][]byte `eddt:"delta.nested,delta.clearable"`
}
