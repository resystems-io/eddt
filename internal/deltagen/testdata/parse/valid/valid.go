// Package valid provides a fixture Snapshot type that covers every supported
// non-map payload field shape for G-03/G-04 parse tests. The struct also
// carries a conforming `eddt:"entity.key"` field (a comparable key struct)
// so that G-04's key-field discovery succeeds.
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
// All fields are comparable; LocationInfo is therefore eligible as an
// entity-key target via ParseOpts.KeyFieldOverride (exercised by G-04 G.9).
type LocationInfo struct{ Lat, Lon float64 }

// UEKey is the entity-key struct for ValidSnapshot. Both IMSI and IMEI are
// comparable, so UEKey passes the G-04 comparable-fields validation.
type UEKey struct{ IMSI, IMEI string }

// ValidSnapshot covers every supported non-map payload field shape plus a
// conforming entity.key field:
//
//   - Entity key:          Key (UEKey, tagged eddt:"entity.key")
//   - Scalar basic:        Attached (bool)
//   - Scalar named int:    Status (UEStatus)
//   - Scalar named string: Bearer (BearerID)
//   - Pointer to struct:   TAI (*TAI)
//   - Pointer to basic:    Count (*int32)
//   - Struct value:        Location (LocationInfo)
//   - Slice:               Bearers ([]BearerID)
//   - Stdlib named struct: LastSeen (time.Time) — classified as ShapeStructValue
//
// G-04 removes Key from ParsedSnapshot.Fields and surfaces it via KeyVar,
// so the eight payload fields above are what the parse stage reports.
type ValidSnapshot struct {
	eddt.Header
	Key      UEKey `eddt:"entity.key"`
	Attached bool
	Status   UEStatus
	Bearer   BearerID
	TAI      *TAI
	Count    *int32
	Location LocationInfo
	Bearers  []BearerID
	LastSeen time.Time
}
