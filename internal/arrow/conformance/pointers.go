package conformance

//go:generate go run go.resystems.io/eddt/cmd/arrow-writer-gen -p . -s NilVsZero -o pointers-arrow-writer.go -n conformance -v
//go:generate go run go.resystems.io/eddt/cmd/arrow-reader-gen -p . -s NilVsZero -o pointers-arrow-reader.go -n conformance -v

import (
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// NilSub is the struct behind the pointer and value struct fields of
// NilVsZero.
type NilSub struct {
	A int64
	B string
}

// NilVsZero pairs every nullable shape with the question the tests ask of
// it: does a nil come back nil, does a pointer to a zero value come back as a
// pointer to a zero value, and does an empty container stay distinct from a
// nil one.
type NilVsZero struct {
	Int    *int64
	Str    *string
	Bool   *bool
	Float  *float64
	Bytes  []byte
	PBytes *[]byte
	Struct *NilSub
	Value  NilSub
	Time   *time.Time
	Dur    *time.Duration
	Slice  []int64
	PSlice *[]int64
	Map    map[string]int64
	PMap   *map[string]int64
	PP     **int64
	PbDur  *durationpb.Duration
	PbTime *timestamppb.Timestamp
}
