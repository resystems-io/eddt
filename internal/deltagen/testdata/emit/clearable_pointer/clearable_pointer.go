// Package clearable_pointer is the HK-16 emit fixture for a
// delta.nested+delta.clearable struct field whose inner struct contains a
// pointer sub-field.
//
// ContactInfo is pointer-comparable (all fields are scalar or pointer), so
// ClearableStructEqReflect is false and the generated Diff uses == for the
// outer clearable comparison.  However, the ContactInfoDelta nested companion
// emitted by buildNestedTypeView carries SetPhone **string for the *string
// sub-field, exercising the ShapePointer path inside the nested-type builder.
//
// The fixture is consumed by TestEmitTemplate_Clearable_Pointer_SamePkg.
package clearable_pointer

import eddt "go.resystems.io/eddt/runtime"

// ContactInfo has a pointer sub-field; it is comparable (pointer comparison).
type ContactInfo struct {
	Name  string
	Phone *string
}

// ClearablePointerSnapshot exercises a clearable struct field with pointer sub-fields.
type ClearablePointerSnapshot struct {
	eddt.Header
	Key     string      `eddt:"entity.key"`
	Contact ContactInfo `eddt:"delta.nested,delta.clearable"`
}
