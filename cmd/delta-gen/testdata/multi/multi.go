// Package multi holds two Snapshot structs in a single package for use by
// TestCLI_MultiTypeAutoDerivedSplit and TestCLI_PositionalAndTypeMerge.
package multi

import eddt "go.resystems.io/eddt/runtime"

// FirstSnapshot is the first of two test snapshot types.
type FirstSnapshot struct {
	eddt.Header
	Key   string `eddt:"entity.key"`
	Value string
}

// SecondSnapshot is the second of two test snapshot types.
type SecondSnapshot struct {
	eddt.Header
	Key   string `eddt:"entity.key"`
	Count int32
}
