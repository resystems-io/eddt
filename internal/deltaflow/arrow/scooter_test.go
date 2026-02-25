package arrow

import (
	"testing"
)

func TestScooterTelemetryDelta(t *testing.T) {
	delta := ScooterTelemetryDelta{
		ScooterID: Delta[string]{
			Op:    ASSERT,
			Value: "scooter-123",
		},
		Speed: Delta[float64]{
			Op:    ASSERT,
			Value: 25.5,
		},
		Battery: Delta[int32]{
			Op:    IGNORE,
			Value: 0,
		},
		Pitch: Delta[float64]{
			Op:    RETRACT,
			Value: 0.1,
		},
		Roll: Delta[float64]{
			Op:    ASSERT,
			Value: -0.05,
		},
	}

	if delta.ScooterID.Op != ASSERT || delta.ScooterID.Value != "scooter-123" {
		t.Errorf("expected ScooterID to be ASSERT 'scooter-123', got %v %v", delta.ScooterID.Op, delta.ScooterID.Value)
	}
	if delta.Speed.Op != ASSERT || delta.Speed.Value != 25.5 {
		t.Errorf("expected Speed to be ASSERT 25.5, got %v %v", delta.Speed.Op, delta.Speed.Value)
	}
	if delta.Battery.Op != IGNORE {
		t.Errorf("expected Battery Op to be IGNORE, got %v", delta.Battery.Op)
	}
	if delta.Pitch.Op != RETRACT || delta.Pitch.Value != 0.1 {
		t.Errorf("expected Pitch to be RETRACT 0.1, got %v %v", delta.Pitch.Op, delta.Pitch.Value)
	}
	if delta.Roll.Op != ASSERT || delta.Roll.Value != -0.05 {
		t.Errorf("expected Roll to be ASSERT -0.05, got %v %v", delta.Roll.Op, delta.Roll.Value)
	}
}

func TestScooterTelemetryDeltaSchema(t *testing.T) {
	t.Run("Schema Derivation Delta", func(t *testing.T) {
		schema, err := DeriveSchema(&ScooterTelemetryDelta{})
		if err != nil {
			t.Fatalf("Failed to derive schema: %v", err)
		}

		if schema == nil {
			t.Fatal("Expected a valid Arrow schema, got nil")
		}

		t.Logf("Derived ScooterTelemetryDelta Schema:\n%s", schema)

		if len(schema.Fields()) != 5 {
			t.Errorf("Expected 5 fields in schema, got %d", len(schema.Fields()))
		}
	})
}
