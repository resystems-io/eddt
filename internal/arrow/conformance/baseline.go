package conformance

//go:generate go run go.resystems.io/eddt/cmd/arrow-writer-gen -p . -s BaselineRecord,PrimitivesRecord -o baseline-arrow-writer.go -n conformance -v
//go:generate go run go.resystems.io/eddt/cmd/arrow-reader-gen -p . -s BaselineRecord,PrimitivesRecord -o baseline-arrow-reader.go -n conformance -v

import "time"

// BaselineNested is the nullable nested struct used by BaselineRecord, the
// same shape the capability octet structs take in analytics/datalakerecorder.
type BaselineNested struct {
	Flag  bool
	Value uint64
}

// BaselineRecord is the control fixture. It is shaped like the records the
// generators already produce for in production (a string key, a collection
// time, nullable scalars, a byte payload, a list and a nullable nested
// struct), so a failure here means the harness itself is broken, not eddt.
type BaselineRecord struct {
	ID          string
	CollectedAt time.Time
	Count       *uint64
	Name        *string
	Payload     []byte
	Bands       []uint64
	Nested      *BaselineNested
}

// Status is a named integer with a String method declared in the input
// package. Unlike the external Level type it must resolve natively, as a
// numeric column, because the generator sees its underlying type.
type Status int32

// StatusActive is the non-zero Status the tests write.
const StatusActive Status = 7

func (s Status) String() string {
	if s == StatusActive {
		return "active"
	}
	return "inactive"
}

// PrimitivesRecord carries every primitive type the generators document,
// plus the declaration forms the parser must handle: a grouped declaration,
// a blank field and an unexported field.
type PrimitivesRecord struct {
	I8  int8
	I16 int16
	I32 int32
	I64 int64
	I   int
	U8  uint8
	U16 uint16
	U32 uint32
	U64 uint64
	U   uint
	F32 float32
	F64 float64
	B   bool
	S   string
	R   rune
	By  byte
	// Start and End share one declaration, the shape that once lost its
	// second name in eddt (fixed upstream on 2026-06-18).
	Start, End uint64
	_          int
	Status     Status
	// The generated code lives in this package, so an unexported field is
	// reachable and the generator includes it. A different --pkg-name drops it.
	hidden string
}

// NewPrimitivesRecord builds a PrimitivesRecord with every field set,
// including the unexported one the tests cannot set from outside the package
// through a composite literal in another file. It exists because the
// unexported field is part of what the round trip must prove.
func NewPrimitivesRecord(hidden string) *PrimitivesRecord {
	return &PrimitivesRecord{
		I8: -8, I16: -16, I32: -32, I64: -64, I: -1,
		U8: 8, U16: 16, U32: 32, U64: 64, U: 1,
		F32: 1.5, F64: 2.25,
		B: true, S: "text", R: 'λ', By: 0xAB,
		Start: 10, End: 20,
		Status: StatusActive,
		hidden: hidden,
	}
}

// Hidden returns the unexported field so the tests can compare it.
func (p *PrimitivesRecord) Hidden() string {
	return p.hidden
}
