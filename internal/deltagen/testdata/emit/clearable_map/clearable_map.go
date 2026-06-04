// Package clearable_map is the R-DG-016..07 emit fixture for a
// delta.nested+delta.clearable field whose inner shape is a map.
//
// The generated Delta carries `Tags runtime.FieldDelta[TagsMapDelta]` (one field).
// A net-new TagsMapDelta struct is generated with UpdatedTags/RemovedTags plus
// IsEmpty/ApplyTagsMapDelta/DiffTagsMapDelta helpers. The map value (string) is
// comparable so WrapperUseReflectEq=false.
//
// The fixture is consumed by TestEmitTemplate_Clearable_Map_SamePkg in template_test.go.
package clearable_map

import eddt "go.resystems.io/eddt/runtime"

// ClearableMapSnapshot exercises a clearable map field.
type ClearableMapSnapshot struct {
	eddt.Header
	// Key is the entity key.
	Key string `eddt:"entity.key"`
	// Tags is tracked with per-entry delta AND wholesale-reset support.
	Tags map[string]string `eddt:"delta.nested,delta.clearable"`
	// Count is a plain atomic field coexisting with the clearable field.
	Count int32
}
