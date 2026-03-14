package parquet

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
