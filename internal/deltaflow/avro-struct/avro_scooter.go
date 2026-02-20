package avrostruct

import (
	_ "embed"

	"go.resystems.io/eddt/internal/deltaflow"
)

//go:generate avrogen -pkg avrostruct -o avro_scooter_types.go -tags json:snake,yaml:upper-camel ../avro_delta.avsc ../avro_geo.avsc avro_scooter.avsc

//go:embed avro_scooter.avsc
var avro_scooter_schema []byte

func NewScooterUpdate() *ScooterUpdate {
	return &ScooterUpdate{
		ActivityState:          ActivityStateUpdate{Op: string(deltaflow.DeltaOpIgnore)},
		Pitch:                  PitchUpdate{Op: string(deltaflow.DeltaOpIgnore)},
		Roll:                   RollUpdate{Op: string(deltaflow.DeltaOpIgnore)},
		SpeedometerSpeed:       SpeedometerSpeedUpdate{Op: string(deltaflow.DeltaOpIgnore)},
		MotorSpeed:             MotorSpeedUpdate{Op: string(deltaflow.DeltaOpIgnore)},
		MotorTemperature:       MotorTemperatureUpdate{Op: string(deltaflow.DeltaOpIgnore)},
		SerialNumber:           SerialNumberUpdate{Op: string(deltaflow.DeltaOpIgnore)},
		AssignedRider:          AssignedRiderUpdate{Op: string(deltaflow.DeltaOpIgnore)},
		ConnectedBatterySerial: ConnectedBatterySerialUpdate{Op: string(deltaflow.DeltaOpIgnore)},
		Location:               LocationUpdate{Op: string(deltaflow.DeltaOpIgnore)},
	}
}
