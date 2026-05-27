// Package clearable_map_reflect is the CL-05..07 emit fixture for a
// delta.nested+delta.clearable map field with a non-comparable value type.
//
// The map value type (Bag, which contains a slice) is not comparable, so the
// generated DiffTagsMapDelta must use reflect.DeepEqual for value comparison
// (WrapperUseReflectEq=true). Verifies NeedsReflect propagation and the
// {{if .WrapperUseReflectEq}} branch in mapWrapper.
//
// The fixture is consumed by TestEmitTemplate_Clearable_Map_Reflect_SamePkg.
package clearable_map_reflect

import eddt "go.resystems.io/eddt/runtime"

// Bag has a slice field, making it non-comparable in Go.
type Bag struct {
	Items []string
}

// ClearableMapReflectSnapshot exercises a clearable map field with non-comparable value.
type ClearableMapReflectSnapshot struct {
	eddt.Header
	// Key is the entity key.
	Key string `eddt:"entity.key"`
	// Tags is a clearable map with non-comparable value → reflect.DeepEqual path.
	Tags map[string]Bag `eddt:"delta.nested,delta.clearable"`
}
