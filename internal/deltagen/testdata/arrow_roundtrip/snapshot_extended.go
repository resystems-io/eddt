package arrowroundtrip

// ARExtended covers the delta-gen field shapes that are NOT yet supported by
// arrow-gen: ShapePointer (*int32 → **int32 in TDelta), ShapeSlice ([]string →
// *[]string in TDelta), and ShapeMap (map[string]string → *map[string]string in
// TDelta).
//
// This file exists so that snapshot_extended.go is a valid Go file that compiles
// on its own.  The subtests that would exercise these shapes are currently skipped
// (t.Skip) in integration_arrow_test.go, pending PR-03 + C-08.
//
// ARExtended does NOT embed eddt.Header; the skip tests only need the type to
// exist for documentation purposes.
type ARExtended struct {
	Key     string            `eddt:"entity.key"`
	Pointer *int32
	Tags    []string
	Attrs   map[string]string
}
