// Package clearable_struct_reflect is the R-DG-026 emit fixture for a
// delta.nested+delta.clearable field whose inner struct is non-comparable
// (contains a slice field), exercising the ClearableStructEqReflect=true branch
// in template.go.
//
// When ClearableStructEqReflect is true the generated Diff uses
// reflect.DeepEqual(a.Latest, b.Latest) instead of a.Latest == b.Latest, and
// the zero-value check uses reflect.DeepEqual(b.Latest, LogEntry{}) instead of
// b.Latest == (LogEntry{}).  The reflect import must be present.
//
// The fixture is consumed by TestEmitTemplate_Clearable_Struct_Reflect_SamePkg.
package clearable_struct_reflect

import eddt "go.resystems.io/eddt/runtime"

// LogEntry has a Tags slice, making it non-comparable in Go.
type LogEntry struct {
	Message string
	Tags    []string
}

// ClearableStructReflectSnapshot exercises a clearable struct field whose
// inner struct is non-comparable (contains a slice).
type ClearableStructReflectSnapshot struct {
	eddt.Header
	Key    string   `eddt:"entity.key"`
	Latest LogEntry `eddt:"delta.nested,delta.clearable"`
	Count  int32
}
