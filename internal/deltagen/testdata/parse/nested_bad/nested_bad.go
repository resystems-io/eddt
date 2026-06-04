// Package nested_bad provides Snapshot fixtures with delta.nested applied to
// non-composite shapes (scalar, pointer). Each fixture is loaded by R-DG-006, R-DG-007
// tests to verify the harmonised granularity-axis gate rejects them with a
// descriptive error.
package nested_bad

import eddt "go.resystems.io/eddt/runtime"

// NestedScalarSnap tags a scalar payload field with delta.nested.
// Expected: R-DG-006, R-DG-007 rejects at parse time with "composite field shape".
type NestedScalarSnap struct {
	eddt.Header
	Key   string `eddt:"entity.key"`
	Count int    `eddt:"delta.nested"`
}

// NestedPointerSnap tags a pointer payload field with delta.nested.
// Expected: R-DG-006, R-DG-007 rejects at parse time with "composite field shape".
type NestedPointerSnap struct {
	eddt.Header
	Key string  `eddt:"entity.key"`
	Ptr *string `eddt:"delta.nested"`
}
