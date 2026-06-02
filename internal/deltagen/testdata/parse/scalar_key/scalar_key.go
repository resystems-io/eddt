// Package scalar_key provides a Snapshot fixture whose entity.key field is a
// scalar (named-string) type rather than a struct. R-DG-010 relaxed
// type-acceptance rule allows any value-typed comparable type as the entity
// key: basic types, named basic types, and structs of comparable fields.
// This fixture proves the scalar path works end-to-end.
package scalar_key

import eddt "go.resystems.io/eddt/runtime"

// IMSI is a named string type — a ShapeScalar value that is comparable and
// therefore eligible as an entity-key field type.
type IMSI string

// ScalarKeySnapshot tags an IMSI-valued field as the entity key. R-DG-010 must
// accept this without error and surface Key via ParsedSnapshot.KeyVar.
type ScalarKeySnapshot struct {
	eddt.Header
	Key    IMSI `eddt:"entity.key"`
	Status int32
}
