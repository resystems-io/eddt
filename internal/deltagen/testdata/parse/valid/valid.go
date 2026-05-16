// Package valid provides a fixture Snapshot type that covers every supported
// non-map payload field shape for G-03 parse tests.
package valid

import (
	"time"

	eddt "go.resystems.io/eddt/runtime"
)

// UEStatus is a named scalar type backed by int32.
type UEStatus int32

// BearerID is a named scalar type backed by string.
type BearerID string

// TAI is a simple struct used for the pointer-to-struct shape fixture.
type TAI struct{ PLMN, TAC string }

// LocationInfo is a struct value used for the struct-value shape fixture.
type LocationInfo struct{ Lat, Lon float64 }

// ValidSnapshot covers every supported non-map payload field shape:
//
//   - Scalar basic:        Attached (bool)
//   - Scalar named int:    Status (UEStatus)
//   - Scalar named string: Bearer (BearerID)
//   - Pointer to struct:   TAI (*TAI)
//   - Pointer to basic:    Count (*int32)
//   - Struct value:        Location (LocationInfo)
//   - Slice:               Bearers ([]BearerID)
//   - Stdlib named struct: LastSeen (time.Time) — classified as ShapeStructValue
type ValidSnapshot struct {
	eddt.Header
	Attached bool
	Status   UEStatus
	Bearer   BearerID
	TAI      *TAI
	Count    *int32
	Location LocationInfo
	Bearers  []BearerID
	LastSeen time.Time
}
