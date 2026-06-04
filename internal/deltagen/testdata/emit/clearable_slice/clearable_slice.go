// Package clearable_slice is the R-DG-016..07 emit fixture for a
// delta.nested+delta.clearable field whose inner shape is a slice.
//
// The generated Delta carries `Groups runtime.FieldDelta[GroupsSliceDelta]` (one field).
// A net-new GroupsSliceDelta struct is generated with AddedGroups/RemovedGroups plus
// IsEmpty/ApplyGroupsSliceDelta/DiffGroupsSliceDelta helpers. The element type (string)
// is comparable so WrapperUseReflectEq=false (O(n) set path).
//
// The fixture is consumed by TestEmitTemplate_Clearable_Slice_SamePkg in template_test.go.
package clearable_slice

import eddt "go.resystems.io/eddt/runtime"

// ClearableSliceSnapshot exercises a clearable slice field.
type ClearableSliceSnapshot struct {
	eddt.Header
	// Key is the entity key.
	Key string `eddt:"entity.key"`
	// Groups is tracked with set-diff delta AND wholesale-reset support.
	Groups []string `eddt:"delta.nested,delta.clearable"`
	// Count is a plain atomic field coexisting with the clearable field.
	Count int32
}
