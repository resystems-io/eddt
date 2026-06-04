package clearable

// Address is a plain sub-structure used as a delta.nested+delta.clearable field.
type Address struct {
	City string
	Zip  string
}

// ClearableSnapshot has a clearable nested field. In standalone mode the
// generated FieldDelta[AddressDelta] uses local FieldDelta/FieldDeltaOp types
// (defined in delta_types.go) rather than runtime.FieldDelta.
//
//go:generate delta-gen --standalone ClearableSnapshot
type ClearableSnapshot struct {
	ID       string  `eddt:"entity.key"`
	Firmware string  `eddt:"delta.omit"`
	Location Address `eddt:"delta.nested,delta.clearable"`
}
