package deltaflow

import (
	_ "embed"
)

//go:generate avrogen -pkg deltaflow -o avro_scooter_types.go -tags json:snake,yaml:upper-camel avro_delta.avsc avro_geo.avsc avro_scooter.avsc

//go:embed avro_scooter.avsc
var avro_scooter_schema []byte

func NewScooterUpdate() *ScooterUpdate {
	return &ScooterUpdate{
		ActivityState:          DeltaOpIgnore,
		Pitch:                  DeltaOpIgnore,
		Roll:                   DeltaOpIgnore,
		SpeedometerSpeed:       DeltaOpIgnore,
		MotorSpeed:             DeltaOpIgnore,
		MotorTemperature:       DeltaOpIgnore,
		SerialNumber:           DeltaOpIgnore,
		AssignedRider:          DeltaOpIgnore,
		ConnectedBatterySerial: DeltaOpIgnore,
		Location:               DeltaOpIgnore,
	}
}
