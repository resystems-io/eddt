package conformance

//go:generate go run go.resystems.io/eddt/cmd/arrow-writer-gen -p . -s TimeRecord -o timerecord-arrow-writer.go -n conformance -v
//go:generate go run go.resystems.io/eddt/cmd/arrow-reader-gen -p . -s TimeRecord -o timerecord-arrow-reader.go -n conformance -v

import (
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TimeRecord carries the four well-known time types in value, pointer and
// list form. The tests feed it in-range values, zero values, and values
// outside what an int64 count of nanoseconds can hold, because the
// conversion between Go time and Arrow's fixed-width Timestamp_ns column is
// where silent corruption is most likely. A list of protobuf timestamps is
// absent because protobuf messages are always pointers and the reader the
// generator emits for a list of pointers does not compile (see
// generator_test.go).
type TimeRecord struct {
	Dur   time.Duration
	PDur  *time.Duration
	At    time.Time
	PAt   *time.Time
	Ats   []time.Time
	PbDur *durationpb.Duration
	PbAt  *timestamppb.Timestamp
}
