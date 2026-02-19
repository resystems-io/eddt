package avrounion

import (
	_ "embed"

	"go.resystems.io/eddt/internal/deltaflow"
)

//go:generate avrogen -pkg avrounion -o avro_scooter_types.go -tags json:snake,yaml:upper-camel ../avro_delta.avsc ../avro_geo.avsc avro_scooter.avsc

//go:embed avro_scooter.avsc
var avro_scooter_schema []byte

func NewScooterUpdate() *ScooterUpdate {
	return &ScooterUpdate{
		ActivityState:          deltaflow.DeltaOpIgnore,
		Pitch:                  deltaflow.DeltaOpIgnore,
		Roll:                   deltaflow.DeltaOpIgnore,
		SpeedometerSpeed:       deltaflow.DeltaOpIgnore,
		MotorSpeed:             deltaflow.DeltaOpIgnore,
		MotorTemperature:       deltaflow.DeltaOpIgnore,
		SerialNumber:           deltaflow.DeltaOpIgnore,
		AssignedRider:          deltaflow.DeltaOpIgnore,
		ConnectedBatterySerial: deltaflow.DeltaOpIgnore,
		Location:               deltaflow.DeltaOpIgnore,
	}
}
