package parquet

import (
	"testing"
)

// Removed struct

func TestDeltaFlowEndToEnd(t *testing.T) {

	type ThisAndThat struct {
		ThingA string
		ThingB int
		ThingC bool
		ThingD []byte
		ThingE map[string]float64
	}

	t.Run("Schema Derivation", func(t *testing.T) {
		// 1. Derive the Arrow Schema from the Go Struct
		// This tests the "Parquet backdoor" pattern.
		schema, err := DeriveSchema(&ThisAndThat{})
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
}
