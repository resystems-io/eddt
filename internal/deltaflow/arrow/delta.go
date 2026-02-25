package arrow

import (
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	"github.com/apache/arrow/go/v18/parquet/schema"
)

// DeriveSchema uses the Go parquet library's reflection to derive an
// Arrow schema out of the annotated Go struct parameter.
// This is acting as the single source of truth ("Parquet backdoor").
func DeriveSchema(schemaStruct any) (*arrow.Schema, error) {
	pqNode, err := schema.NewSchemaFromStruct(schemaStruct)
	if err != nil {
		return nil, err
	}

	arrowSchema, err := pqarrow.FromParquet(pqNode, nil, nil)
	if err != nil {
		return nil, err
	}

	return arrowSchema, nil
}

// BuildRecordBatch takes the derived schema and a slice of telemetry types,
// explicitly mapping the struct fields to the columnar Arrow RecordBuilder.
// Reflection is avoided here for maximum CPU performance throughput.
//
// Note: In real implementation, the telemetry argument would likely be a concrete slice,
// or this would be a method on a builder struct specific to the type.
// For the tests, we use interface{} to represent the incoming data slice.
type RecordBuilder func(schema *arrow.Schema, telemetryData any) (arrow.Record, error)

// `DeltaOp represents the operation type for a delta.
//
// ```sql
// CREATE TYPE op_type AS ENUM ('IGNORE', 'ASSERT', 'RETRACT');
// ```
type DeltaOp int

const (
	// IGNORE means the field is not present in the delta (no change, or NOP).
	IGNORE DeltaOp = iota
	// ASSERT means the field is present in the delta and the value is the new value.
	ASSERT
	// RETRACT means the field should be reset to its default value.
	RETRACT
)

// Delta represents a change in a field's value.
type Delta[T any] struct {
	Op    DeltaOp `json:"op"`
	Value T       `json:"value"`
}
