package deltaflow

import (
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
)

func Example_arrow_delta() {
	// 1. Define the "Control Codes" Enum
	// In Arrow, we often use a Dictionary or simply an Int8 to represent enums.
	// For a Union, strictly speaking, the "Type Code" acts as the enum.
	// Type ID 0 = IGNORE
	// Type ID 1 = RETRACT
	// Type ID 2 = RESET
	// Type ID 3 = VALUE (The actual data)

	// 2. Define the Union Fields
	// We create a helper function because every field uses the same pattern.
	createControlUnion := func(valueType arrow.DataType) *arrow.DenseUnionType {
		children := []arrow.Field{
			{Name: "IGNORE", Type: arrow.Null, Nullable: true},  // ID=0: No data storage needed
			{Name: "RETRACT", Type: arrow.Null, Nullable: true}, // ID=1: No data storage needed
			{Name: "RESET", Type: arrow.Null, Nullable: true},   // ID=2: No data storage needed
			{Name: "VALUE", Type: valueType, Nullable: true},    // ID=3: Holds the float/string
		}

		// The type codes map the child index to the stored byte ID
		typeCodes := []int8{0, 1, 2, 3}

		return arrow.DenseUnionOf(children, typeCodes)
	}

	// 3. Define the Schema
	scooterSchema := arrow.NewSchema(
		[]arrow.Field{
			// Field: Activity State (Union of Nulls + String)
			{Name: "activity_state", Type: createControlUnion(arrow.BinaryTypes.String)},

			// Field: Pitch (Union of Nulls + Float32)
			{Name: "pitch", Type: createControlUnion(arrow.PrimitiveTypes.Float32)},

			// Field: Roll (Union of Nulls + Float32)
			{Name: "roll", Type: createControlUnion(arrow.PrimitiveTypes.Float32)},

			// Field: Location (Union of Nulls + Struct{Lat, Lon})
			{Name: "location", Type: createControlUnion(
				arrow.StructOf(
					arrow.Field{Name: "lat", Type: arrow.PrimitiveTypes.Float64},
					arrow.Field{Name: "lon", Type: arrow.PrimitiveTypes.Float64},
				),
			)},
		},
		nil, // No metadata
	)

	fmt.Println("Schema Created Successfully:\n", scooterSchema)

	// Output:
	// Schema Created Successfully:
	//  schema:
	//   fields: 4
	//     - activity_state: type=dense_union<IGNORE: type=null, nullable=0, RETRACT: type=null, nullable=1, RESET: type=null, nullable=2, VALUE: type=utf8, nullable=3>
	//     - pitch: type=dense_union<IGNORE: type=null, nullable=0, RETRACT: type=null, nullable=1, RESET: type=null, nullable=2, VALUE: type=float32, nullable=3>
	//     - roll: type=dense_union<IGNORE: type=null, nullable=0, RETRACT: type=null, nullable=1, RESET: type=null, nullable=2, VALUE: type=float32, nullable=3>
	//     - location: type=dense_union<IGNORE: type=null, nullable=0, RETRACT: type=null, nullable=1, RESET: type=null, nullable=2, VALUE: type=struct<lat: float64, lon: float64>, nullable=3>

}
