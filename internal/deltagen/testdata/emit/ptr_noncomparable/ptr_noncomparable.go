package ptrnoncomparable

import eddt "go.resystems.io/eddt/runtime"

// SliceBag is non-comparable because it contains a slice.
type SliceBag struct {
	Tags []string
}

// PtrNonComparableSnapshot exercises the PointeeUseReflectEq path (R-DG-026):
// a *SliceBag field whose pointee is non-comparable, so Diff must emit
// reflect.DeepEqual(*a.Bag, *b.Bag) inside the nil-equivalence guard.
type PtrNonComparableSnapshot struct {
	eddt.Header
	Key string `eddt:"entity.key"`
	Bag *SliceBag
}
