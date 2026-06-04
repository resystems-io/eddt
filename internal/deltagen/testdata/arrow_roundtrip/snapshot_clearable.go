package arrowroundtrip

import eddt "go.resystems.io/eddt/runtime"

// ARAddress is a comparable struct used as the clearable struct inner type.
// Being comparable, delta-gen emits the R-DG-016 companion (ARAddressDelta) with
// no reflect.DeepEqual fallback.
type ARAddress struct {
	Street string
	City   string
}

// ARClearable covers the R-DG-016, R-DG-019 V-model INT step: all three nested+clearable
// inner shapes (struct / map / slice) in a single Snapshot.
type ARClearable struct {
	eddt.Header
	Key      string            `eddt:"entity.key"`
	Location ARAddress         `eddt:"delta.nested,delta.clearable"`
	Tags     map[string]string `eddt:"delta.nested,delta.clearable"`
	Groups   []string          `eddt:"delta.nested,delta.clearable"`
}
