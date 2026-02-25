package arrow

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow/go/v18/parquet"
	"github.com/apache/arrow/go/v18/parquet/compress"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	_ "github.com/duckdb/duckdb-go/v2"
	"go.resystems.io/eddt/internal/common"
)

// Removed struct

func TestDeltaFlowEndToEnd(t *testing.T) {
	t.Run("Schema Derivation", func(t *testing.T) {
		// 1. Derive the Arrow Schema from the Go Struct
		// This tests the "Parquet backdoor" pattern.
		schema, err := DeriveSchema(&ScooterTelemetry{})
		if err != nil {
			t.Fatalf("Failed to derive schema: %v", err)
		}

		if schema == nil {
			t.Fatal("Expected a valid Arrow schema, got nil")
		}

		t.Logf("Derived Schema:\n%s", schema)

		// Basic verification: Our struct has 5 fields, the schema should too.
		if len(schema.Fields()) != 5 {
			t.Errorf("Expected 5 fields in schema, got %d", len(schema.Fields()))
		}
	})

	t.Run("RecordBatch Population", func(t *testing.T) {
		// 1. Setup the Schema and test data
		schema, err := DeriveSchema(&ScooterTelemetry{})
		if err != nil {
			t.Fatalf("Failed to derive schema: %v", err)
		}

		testData := []ScooterTelemetry{
			{ScooterID: "scooter-01", Speed: 15.5, Battery: 88, Pitch: 0.1, Roll: 0.05},
			{ScooterID: "scooter-02", Speed: 0.0, Battery: 42, Pitch: -0.2, Roll: 0.0},
		}

		// 2. Build the RecordBatch
		// This tests the custom append logic bypassing reflection.
		var builder RecordBuilder = ScooterTelemetryBuilder
		batch, err := builder(schema, testData)
		if err != nil {
			t.Fatalf("Failed to build RecordBatch: %v", err)
		}
		defer batch.Release()

		// 3. Verify Batch Shape
		if batch.NumRows() != int64(len(testData)) {
			t.Errorf("Expected %d rows, got %d", len(testData), batch.NumRows())
		}

		if batch.NumCols() != int64(len(schema.Fields())) {
			t.Errorf("Expected %d columns, got %d", len(schema.Fields()), batch.NumCols())
		}
	})

	t.Run("RecordBatch to Parquet to DuckDB", func(t *testing.T) {
		schema, err := DeriveSchema(&ScooterTelemetry{})
		if err != nil {
			t.Fatalf("Failed to derive schema: %v", err)
		}

		// -- Build RecordBatch
		const numTelemetryEntries = 1_000_000
		testData := make([]ScooterTelemetry, 0, numTelemetryEntries)

		for i := 0; i < numTelemetryEntries; i++ {
			testData = append(testData, ScooterTelemetry{
				ScooterID: fmt.Sprintf("test-%02d", i+1),
				Speed:     float64(10 + (i % 20)),
				Battery:   int32(100 - (i % 100)),
				Pitch:     0.1 * float64(i%5),
				Roll:      0.05 * float64(i%3),
			})
		}

		var builder RecordBuilder = ScooterTelemetryBuilder
		batch, err := builder(schema, testData)
		if err != nil {
			t.Fatalf("Failed to build RecordBatch: %v", err)
		}
		defer batch.Release()

		// -- Write Parquet

		tmpDir := t.TempDir()
		parquetPath := filepath.Join(tmpDir, "scooters.parquet")

		f, err := os.Create(parquetPath)
		if err != nil {
			t.Fatal(err)
		}

		writer, err := pqarrow.NewFileWriter(schema, f, parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy)), pqarrow.DefaultWriterProps())
		if err != nil {
			t.Fatal(err)
		}

		if err := writer.Write(batch); err != nil {
			t.Fatal(err)
		}

		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		f.Close()

		// -- Create a copy for manual review
		if true {
			common.CopyFile(parquetPath, "/tmp/scooters.arrow.parquet")
		}

		// -- Load Parquet via DuckDB

		db, err := sql.Open("duckdb", "")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		rows, err := db.Query(fmt.Sprintf("SELECT scooter_id, speed, battery, pitch, roll FROM read_parquet('%s') ORDER BY scooter_id", parquetPath))
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		i := 0
		for rows.Next() {
			var scooterID string
			var speed float64
			var battery int32
			var pitch float64
			var roll float64

			if err := rows.Scan(&scooterID, &speed, &battery, &pitch, &roll); err != nil {
				t.Fatal(err)
			}

			var matchedIndex int
			_, err = fmt.Sscanf(scooterID, "test-%d", &matchedIndex)
			if err != nil {
				t.Fatalf("row failed to parse scooterID: %s", scooterID)
			}
			matchedIndex -= 1

			if scooterID != testData[matchedIndex].ScooterID {
				t.Errorf("row %d: expected ScooterID %s, got %s", i, testData[matchedIndex].ScooterID, scooterID)
			}
			if speed != testData[matchedIndex].Speed {
				t.Errorf("row %d: expected Speed %f, got %f", i, testData[matchedIndex].Speed, speed)
			}
			if battery != testData[matchedIndex].Battery {
				t.Errorf("row %d: expected Battery %d, got %d", i, testData[matchedIndex].Battery, battery)
			}
			if pitch != testData[matchedIndex].Pitch {
				t.Errorf("row %d: expected Pitch %f, got %f", i, testData[matchedIndex].Pitch, pitch)
			}
			if roll != testData[matchedIndex].Roll {
				t.Errorf("row %d: expected Roll %f, got %f", i, testData[matchedIndex].Roll, roll)
			}
			i++
		}

		if i != len(testData) {
			t.Errorf("expected %d records, got %d", len(testData), i)
		}
	})
}
