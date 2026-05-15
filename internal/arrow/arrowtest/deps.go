// Package arrowtest provides shared constants for integration test fixtures.
// Version strings must match the versions declared in eddt's go.mod.
// DuckDB is a test-fixture-only dependency (not in eddt's go.mod).
package arrowtest

const (
	// ArrowDep is the go get version string for the Arrow module.
	ArrowDep = "github.com/apache/arrow-go/v18@v18.6.0"

	// DuckDBDep is the go get version string for the DuckDB driver.
	DuckDBDep = "github.com/duckdb/duckdb-go/v2@v2.10502.0"

	// ProtobufDep is the go get version string for the protobuf module.
	ProtobufDep = "google.golang.org/protobuf@v1.36.11"
)
