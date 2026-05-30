package pump_test

import (
	"testing"
	"time"

	pump "go.resystems.io/eddt/internal/deltagen/example/pump"
	"go.resystems.io/eddt/runtime"
)

// ptr is a generic helper that returns a pointer to v.
func ptr[T any](v T) *T { return &v }

// epoch is a fixed time used in all tests for reproducibility.
var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// newHeader constructs a minimal Header suitable for the first snapshot in a chain.
func newHeader(serialNumber string) runtime.Header {
	return runtime.Header{
		EntityID:    pump.EntityID(serialNumber),
		ChainID:     "chain-" + serialNumber,
		Sequence:    0,
		EffectiveAt: epoch,
		PublishedAt: epoch,
	}
}

// advanceHeader constructs a delta Header from a prior snapshot header.
func advanceHeader(prior runtime.Header, seq uint64) runtime.Header {
	return runtime.Header{
		EntityID:    prior.EntityID,
		ChainID:     prior.ChainID,
		Sequence:    seq,
		EffectiveAt: epoch.Add(time.Duration(seq) * time.Hour),
		PublishedAt: epoch.Add(time.Duration(seq) * time.Hour),
		Provenance: append(prior.Provenance, runtime.Provenance{
			PublishedAt: epoch,
			Solution:    "plant-control",
			Component:   "pressure-monitor",
		}),
	}
}

// TestEntityID verifies EntityID is non-zero and deterministic.
func TestEntityID(t *testing.T) {
	id1 := pump.EntityID("SN-4719")
	id2 := pump.EntityID("SN-4719")
	if id1.IsZero() {
		t.Fatal("EntityID should be non-zero")
	}
	if id1 != id2 {
		t.Fatal("EntityID should be deterministic for equal input")
	}
	idOther := pump.EntityID("SN-0001")
	if id1 == idOther {
		t.Fatal("EntityID should differ for different serial numbers")
	}
}

// TestDiff_atomicChange verifies Diff sets only the changed field.
func TestDiff_atomicChange(t *testing.T) {
	a := pump.PumpSnapshot{
		Header:       newHeader("SN-4719"),
		SerialNumber: "SN-4719",
		PressureKPa:  850.0,
		TempCelsius:  72.3,
		Location:     pump.SiteAddress{Street: "Mill Road 1", City: "Berlin"},
		FirmwareVer:  "v2.1.0",
	}
	b := a
	b.Header = advanceHeader(a.Header, 1)
	b.PressureKPa = 855.5

	d, err := pump.Diff(a, b)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if d.SetPressureKPa == nil || *d.SetPressureKPa != 855.5 {
		t.Errorf("SetPressureKPa: got %v, want 855.5", d.SetPressureKPa)
	}
	if d.SetTempCelsius != nil {
		t.Errorf("SetTempCelsius should be nil (unchanged), got %v", *d.SetTempCelsius)
	}
	// Location delta is zero-valued (no sub-fields changed).
	if d.Location.SetCity != nil || d.Location.SetStreet != nil {
		t.Error("Location delta should be zero-valued (unchanged)")
	}
}

// TestApply_roundTrip verifies Apply(a, Diff(a, b)) equals b in payload.
func TestApply_roundTrip(t *testing.T) {
	a := pump.PumpSnapshot{
		Header:       newHeader("SN-4719"),
		SerialNumber: "SN-4719",
		PressureKPa:  850.0,
		TempCelsius:  72.3,
		Location:     pump.SiteAddress{Street: "Mill Road 1", City: "Berlin"},
		FirmwareVer:  "v2.1.0",
	}
	b := a
	b.Header = advanceHeader(a.Header, 1)
	b.PressureKPa = 855.5
	b.Location = pump.SiteAddress{Street: "Mill Road 1", City: "Hamburg"}

	d, err := pump.Diff(a, b)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	got, err := pump.Apply(a, d)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got.PressureKPa != b.PressureKPa {
		t.Errorf("PressureKPa: got %v, want %v", got.PressureKPa, b.PressureKPa)
	}
	if got.Location.City != b.Location.City {
		t.Errorf("Location.City: got %q, want %q", got.Location.City, b.Location.City)
	}
	if got.TempCelsius != b.TempCelsius {
		t.Errorf("TempCelsius: got %v, want %v", got.TempCelsius, b.TempCelsius)
	}
	// delta.omit field must be carried forward unchanged.
	if got.FirmwareVer != a.FirmwareVer {
		t.Errorf("FirmwareVer (omit): got %q, want %q", got.FirmwareVer, a.FirmwareVer)
	}
}

// TestDirectDelta_atomic shows direct construction of a delta that updates
// only one atomic scalar field.
func TestDirectDelta_atomic(t *testing.T) {
	current := pump.PumpSnapshot{
		Header:       newHeader("SN-4719"),
		SerialNumber: "SN-4719",
		PressureKPa:  850.0,
		TempCelsius:  72.3,
		Location:     pump.SiteAddress{Street: "Mill Road 1", City: "Berlin"},
		FirmwareVer:  "v2.1.0",
	}

	// --- Direct construction: update pressure only ---
	//
	// Each pointer field in PumpSnapshotDelta is nil by default (no change).
	// Set only the fields you intend to advance.
	delta := pump.PumpSnapshotDelta{
		Header:         advanceHeader(current.Header, 1),
		SetPressureKPa: ptr(float32(855.5)),
		// SetTempCelsius: nil — temperature unchanged
		// Location:      zero-valued SiteAddressDelta — no location change
		// Calibration:   zero-valued FieldDelta (OpIgnore) — no calibration change
	}

	next, err := pump.Apply(current, delta)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if next.PressureKPa != 855.5 {
		t.Errorf("PressureKPa: got %v, want 855.5", next.PressureKPa)
	}
	if next.TempCelsius != current.TempCelsius {
		t.Errorf("TempCelsius changed unexpectedly: got %v", next.TempCelsius)
	}
}

// TestDirectDelta_nested shows direct construction of a delta that updates
// a delta.nested companion field (Location — plain SiteAddressDelta value).
func TestDirectDelta_nested(t *testing.T) {
	current := pump.PumpSnapshot{
		Header:       newHeader("SN-4719"),
		SerialNumber: "SN-4719",
		PressureKPa:  850.0,
		TempCelsius:  72.3,
		Location:     pump.SiteAddress{Street: "Mill Road 1", City: "Berlin"},
		FirmwareVer:  "v2.1.0",
	}

	// Location is a delta.nested field (without delta.clearable).
	// Its Delta type is SiteAddressDelta (a plain value embedded in PumpSnapshotDelta,
	// not a pointer). A zero-valued SiteAddressDelta means "no change".
	// Set only the sub-fields you want to advance.
	delta := pump.PumpSnapshotDelta{
		Header: advanceHeader(current.Header, 1),
		Location: pump.SiteAddressDelta{
			SetCity: ptr("Hamburg"),
			// SetStreet: nil — street unchanged
		},
	}

	next, err := pump.Apply(current, delta)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if next.Location.City != "Hamburg" {
		t.Errorf("Location.City: got %q, want Hamburg", next.Location.City)
	}
	if next.Location.Street != current.Location.Street {
		t.Errorf("Location.Street changed unexpectedly: got %q", next.Location.Street)
	}
}

// TestDirectDelta_clearable shows direct construction of a delta that uses
// the tri-state FieldDelta[CalibrationDataDelta] for the clearable field.
func TestDirectDelta_clearable(t *testing.T) {
	calibrated := time.Date(2026, 3, 15, 9, 0, 0, 0, time.UTC)
	current := pump.PumpSnapshot{
		Header:       newHeader("SN-4719"),
		SerialNumber: "SN-4719",
		PressureKPa:  850.0,
		Calibration: pump.CalibrationData{
			OffsetKPa:   0.5,
			LastCalibAt: calibrated,
		},
	}

	// OpAssert: set the clearable field to a new value.
	assertDelta := pump.PumpSnapshotDelta{
		Header: advanceHeader(current.Header, 1),
		Calibration: runtime.FieldDelta[pump.CalibrationDataDelta]{
			Op:    runtime.OpAssert,
			Value: pump.CalibrationDataDelta{SetOffsetKPa: ptr(float32(-0.3))},
		},
	}
	asserted, err := pump.Apply(current, assertDelta)
	if err != nil {
		t.Fatalf("Apply (OpAssert): %v", err)
	}
	if asserted.Calibration.OffsetKPa != -0.3 {
		t.Errorf("Calibration.OffsetKPa: got %v, want -0.3", asserted.Calibration.OffsetKPa)
	}

	// OpRetract: reset the clearable field to its zero value.
	retractDelta := pump.PumpSnapshotDelta{
		Header: advanceHeader(asserted.Header, 2),
		Calibration: runtime.FieldDelta[pump.CalibrationDataDelta]{
			Op: runtime.OpRetract,
		},
	}
	retracted, err := pump.Apply(asserted, retractDelta)
	if err != nil {
		t.Fatalf("Apply (OpRetract): %v", err)
	}
	if retracted.Calibration != (pump.CalibrationData{}) {
		t.Errorf("Calibration should be zero after OpRetract, got %+v", retracted.Calibration)
	}

	// OpIgnore (zero value): leave the clearable field unchanged.
	ignoreDelta := pump.PumpSnapshotDelta{
		Header: advanceHeader(retracted.Header, 3),
		// Calibration field is zero-valued → Op == OpIgnore → no change
	}
	ignored, err := pump.Apply(retracted, ignoreDelta)
	if err != nil {
		t.Fatalf("Apply (OpIgnore): %v", err)
	}
	if ignored.Calibration != retracted.Calibration {
		t.Errorf("Calibration changed under OpIgnore")
	}
}

// TestCoalesce verifies that Coalesce applies a slice of deltas in order.
func TestCoalesce(t *testing.T) {
	base := pump.PumpSnapshot{
		Header:       newHeader("SN-4719"),
		SerialNumber: "SN-4719",
		PressureKPa:  850.0,
		TempCelsius:  72.3,
	}

	d1 := pump.PumpSnapshotDelta{
		Header:         advanceHeader(base.Header, 1),
		SetPressureKPa: ptr(float32(855.5)),
	}
	d2 := pump.PumpSnapshotDelta{
		Header:         advanceHeader(advanceHeader(base.Header, 1), 2),
		SetPressureKPa: ptr(float32(860.0)), // overrides d1
		SetTempCelsius: ptr(float32(74.1)),
	}

	result, err := pump.Coalesce(base, []pump.PumpSnapshotDelta{d1, d2})
	if err != nil {
		t.Fatalf("Coalesce: %v", err)
	}
	if result.PressureKPa != 860.0 {
		t.Errorf("PressureKPa: got %v, want 860.0 (d2 wins)", result.PressureKPa)
	}
	if result.TempCelsius != 74.1 {
		t.Errorf("TempCelsius: got %v, want 74.1", result.TempCelsius)
	}
}
