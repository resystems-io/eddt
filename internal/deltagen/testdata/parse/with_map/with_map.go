// Package with_map provides a Snapshot fixture with a map payload field, used
// to verify that maps are classified as ShapeMap without a parse-time error.
// Under the harmonised model (refinements §1.6.3, R-DG-006, R-DG-016), untagged
// maps are admitted and emit atomically; no tag-combination constraint applies.
package with_map

import eddt "go.resystems.io/eddt/runtime"

// MapKey is the entity-key struct for MapSnapshot. The single string field is
// comparable, so MapKey passes the comparable-fields validation (R-DG-010).
type MapKey struct{ Name string }

// MapSnapshot has a map[string]string payload field plus a conforming
// entity.key field. The parser must classify Tags as ShapeMap and return
// it without error. Under the harmonised three-axis model (refinements
// §1.6.3, R-DG-006, R-DG-016), untagged maps are admitted with the atomic
// default (SetX *map[K]V emission); no tag-combination constraint
// applies. R-DG-010: Key is removed from ParsedSnapshot.Fields, leaving exactly
// one payload field (Tags).
type MapSnapshot struct {
	eddt.Header
	Key  MapKey `eddt:"entity.key"`
	Tags map[string]string
}
