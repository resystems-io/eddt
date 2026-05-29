// Package clearable_struct is the R-DG-016..07 emit fixture for a
// delta.nested+delta.clearable field whose inner shape is a named struct type.
//
// The generated Delta carries `Location runtime.FieldDelta[AddressDelta]`
// (one field, not inline siblings). The existing AddressDelta companion + ApplyAddress /
// DiffAddress funcs are emitted once (R-DG-016 dedup). The field's Apply emits an Op
// switch; the Diff emits the three-branch predicate (eq / retract / assert).
//
// The fixture is consumed by TestEmitTemplate_Clearable_Struct_SamePkg and
// TestEmitTemplate_Clearable_Struct_CrossPkg in template_test.go.
package clearable_struct

import eddt "go.resystems.io/eddt/runtime"

// Address is the inner struct type. All fields are scalars → comparable.
type Address struct {
	Street string
	City   string
}

// ClearableStructSnapshot exercises a clearable struct field.
type ClearableStructSnapshot struct {
	eddt.Header
	// Key is the entity key.
	Key string `eddt:"entity.key"`
	// Location is tracked with per-field delta AND wholesale-reset support.
	Location Address `eddt:"delta.nested,delta.clearable"`
	// Count is a plain atomic field coexisting with the clearable field.
	Count int32
}
