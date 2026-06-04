// Package pump is a worked example for the delta-gen code generator.
//
// It demonstrates the complete workflow: annotating a Go struct with
// eddt:"…" tags, running delta-gen to produce the companion Delta type,
// and using the generated Apply, Diff, Coalesce, and EntityID API —
// including direct delta construction and clearable field operations.
//
// # Snapshot definition
//
// pump.go defines PumpSnapshot with every commonly used tag combination:
//
//   - SerialNumber string — scalar entity key (drives EntityID hash)
//   - PressureKPa, TempCelsius float32 — atomic scalar payload fields
//   - Location SiteAddress — delta.nested struct (companion SiteAddressDelta)
//   - Calibration CalibrationData — delta.nested+delta.clearable (FieldDelta envelope)
//   - FirmwareVer string — delta.omit (carried forward unchanged by Apply)
//
// # Generated API
//
// pump_snapshot_delta.go is the output of:
//
//	delta-gen --pkg . --out pump_snapshot_delta.go PumpSnapshot
//
// It provides Apply, Diff, Coalesce (package-level and method wrappers), and
// EntityID(k string). All functions take and return values, not pointers.
//
// # Usage examples
//
// pump_delta_test.go covers EntityID, Diff, Apply round-trip, direct delta
// construction (atomic, nested, and clearable), and Coalesce.
//
// See [docs/delta-gen.md] for the full narrative walkthrough of this example.
package pump
