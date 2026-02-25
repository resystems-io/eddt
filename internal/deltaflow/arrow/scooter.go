package arrow

import (
	"errors"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
)

// ScooterTelemetry is the unified Go struct representing our domain entity.
// It serves as the single source of truth for both JSON ingress and Arrow/Parquet schema generation.
type ScooterTelemetry struct {
	ScooterID string  `json:"scooter_id" parquet:"name=scooter_id, logical=String"`
	Speed     float64 `json:"speed"      parquet:"name=speed"`
	Battery   int32   `json:"battery"    parquet:"name=battery"`
	Pitch     float64 `json:"pitch"      parquet:"name=pitch"` // radians
	Roll      float64 `json:"roll"       parquet:"name=roll"`  // radians
}

type ScooterTelemetryDelta struct {
	ScooterID Delta[string]  `json:"scooter_id"`
	Speed     Delta[float64] `json:"speed"`
	Battery   Delta[int32]   `json:"battery"`
	Pitch     Delta[float64] `json:"pitch"` // radians
	Roll      Delta[float64] `json:"roll"`  // radians
}

// ScooterTelemetryBuilder creates an Arrow record batch from a slice of ScooterTelemetry.
// 1. Initialize memory pool and RecordBuilder
// 2. Iterate slice and append to Column Builders (e.g. *array.StringBuilder)
// 3. Finalize with builder.NewRecord()
func ScooterTelemetryBuilder(schema *arrow.Schema, telemetryData any) (arrow.Record, error) {
	data, ok := telemetryData.([]ScooterTelemetry)
	if !ok {
		return nil, errors.New("telemetryData must be of type []ScooterTelemetry")
	}

	pool := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	scooterIDBuilder := builder.Field(0).(*array.StringBuilder)
	speedBuilder := builder.Field(1).(*array.Float64Builder)
	batteryBuilder := builder.Field(2).(*array.Int32Builder)
	pitchBuilder := builder.Field(3).(*array.Float64Builder)
	rollBuilder := builder.Field(4).(*array.Float64Builder)

	for _, t := range data {
		scooterIDBuilder.Append(t.ScooterID)
		speedBuilder.Append(t.Speed)
		batteryBuilder.Append(t.Battery)
		pitchBuilder.Append(t.Pitch)
		rollBuilder.Append(t.Roll)
	}

	return builder.NewRecord(), nil
}
