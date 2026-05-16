// Package with_map provides a Snapshot fixture with a map payload field, used
// to verify that maps are classified as ShapeMap without a parse-time error.
// Tag-combination validation (map requires delta.omit) is enforced by T-02.
package with_map

import eddt "go.resystems.io/eddt/runtime"

// MapKey is the entity-key struct for MapSnapshot. The single string field is
// comparable, so MapKey passes G-04's comparable-fields validation.
type MapKey struct{ Name string }

// MapSnapshot has a map[string]string payload field plus a conforming
// entity.key field. The parser must classify Tags as ShapeMap and return it
// without error; the tag-combination constraint (map is only valid with
// delta.omit) is a separate T-02 concern. G-04 removes Key from
// ParsedSnapshot.Fields, leaving exactly one payload field (Tags).
type MapSnapshot struct {
	eddt.Header
	Key  MapKey `eddt:"entity.key"`
	Tags map[string]string
}
