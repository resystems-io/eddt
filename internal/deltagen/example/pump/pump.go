package pump

import (
	"time"

	eddt "go.resystems.io/eddt/runtime"
)

// SiteAddress is the physical installation address of a pump.
type SiteAddress struct {
	Street string
	City   string
}

// CalibrationData holds factory calibration offsets for a pump.
type CalibrationData struct {
	OffsetKPa   float32
	LastCalibAt time.Time
}

//go:generate delta-gen PumpSnapshot

// PumpSnapshot is an EDDT Snapshot for a manufacturing line pump.
//
// Field coverage:
//   - SerialNumber: scalar entity key → drives EntityID hash.
//   - PressureKPa, TempCelsius: atomic scalar fields → Set<F> *float32 in Delta.
//   - Location: delta.nested struct → SetLocation *SiteAddressDelta in Delta.
//   - Calibration: delta.nested+delta.clearable → Calibration FieldDelta[CalibrationDelta].
//   - FirmwareVer: delta.omit → absent from Delta; Apply carries value forward.
type PumpSnapshot struct {
	eddt.Header
	SerialNumber string `eddt:"entity.key"`
	PressureKPa  float32
	TempCelsius  float32
	Location     SiteAddress     `eddt:"delta.nested"`
	Calibration  CalibrationData `eddt:"delta.nested,delta.clearable"`
	FirmwareVer  string          `eddt:"delta.omit"`
}
