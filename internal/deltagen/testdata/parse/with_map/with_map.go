// Package with_map provides a Snapshot fixture with a map payload field, used
// to verify that maps are classified as ShapeMap without a parse-time error.
// Tag-combination validation (map requires delta.omit) is enforced by T-02.
package with_map

import eddt "go.resystems.io/eddt/runtime"

// MapSnapshot has a map[string]string payload field. The parser must classify
// it as ShapeMap and return it without error; the tag-combination constraint
// (map is only valid with delta.omit) is a separate T-02 concern.
type MapSnapshot struct {
	eddt.Header
	Tags map[string]string
}
