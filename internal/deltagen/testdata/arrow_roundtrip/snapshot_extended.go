package arrowroundtrip

import eddt "go.resystems.io/eddt/runtime"

// ARExtended covers the delta-gen field shapes exercised by R-DG-019:
// ShapePointer (*int32 → **int32 in delta), ShapeSlice ([]string → *[]string),
// and ShapeMap (map[string]string → *map[string]string).
type ARExtended struct {
	eddt.Header
	Key     string `eddt:"entity.key"`
	Pointer *int32
	Tags    []string
	Attrs   map[string]string
}
