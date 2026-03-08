---
name: arrow-verification
description: Double check created Arrow by then converting Arrow to Parquet and
  then using DuckDB to query the parquet.
triggers:
  - "build arrow records"
  - "verify arrow"
  - "confirm correctness of arrow records"
---

# Go Arrow Engineering Verification Rules

### Step-by-step instructions

- import "database/sql"
- import _ "github.com/duckdb/duckdb-go/v2"
- import "github.com/apache/arrow/go/v18/parquet"
- import "github.com/apache/arrow/go/v18/parquet/pqarrow"
- import go.resystems.io/eddt/internal/parquet
- Obtain the Parquet schema via schema.
- Use DeriveSchema to obtain an Arrow schema for the type
- Use the generated Arrow records as input to build Parquet using
  - `writer, err := pqarrow.NewFileWriter(schema, ...)`
  - `writer.Write(batch)`
- Use the SQL package to open a connection to the Parquet file
  - `db, err := sql.Open("duckdb", "")`
- Use the SQL connection to query the data in Parquet
  - `rows, err := db.Query(fmt.Sprintf("SELECT some_field FROM read_parquet('%s')", parquetPath))`

### Common edge cases

- The full verification method above can not be used if the golang type is not
  available to create the schema.
