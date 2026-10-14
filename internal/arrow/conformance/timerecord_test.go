package conformance

import (
	"math"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// The instants an int64 count of microseconds since the Unix epoch can hold,
// which is the column the writer stores a time in.
var (
	minTimestamp = time.UnixMicro(math.MinInt64).UTC()
	maxTimestamp = time.UnixMicro(math.MaxInt64).UTC()
)

// TestTimeInRangeRoundTrip covers every well-known time type with values
// inside the representable range: microsecond precision, negative and
// extreme durations, a clock reading, a non-UTC location, the edges of
// the column and the edges of the protobuf range.
func TestTimeInRangeRoundTrip(t *testing.T) {
	precise := time.Date(2026, 3, 4, 5, 6, 7, 123456000, time.UTC)
	elsewhere := time.Date(2026, 3, 4, 5, 6, 7, 0, time.FixedZone("Somewhere", 5*3600+30*60))
	now := time.UnixMicro(time.Now().UnixMicro())
	year1 := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	year9999 := time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC)
	minDur, maxDur := time.Duration(math.MinInt64), time.Duration(math.MaxInt64)
	in := []*TimeRecord{
		{
			Dur:   -47 * time.Second,
			PDur:  &maxDur,
			At:    precise,
			PAt:   &elsewhere,
			Ats:   []time.Time{precise, now, minTimestamp, maxTimestamp},
			PbDur: durationpb.New(-90 * time.Minute),
			PbAt:  timestamppb.New(precise),
		},
		{Dur: minDur, At: minTimestamp, PbDur: durationpb.New(maxDur), PbAt: timestamppb.New(year9999)},
		{At: time.Unix(0, 0), PAt: &year1, PbAt: timestamppb.New(year1)},
	}
	roundTripBoth(t, NewTimeRecordArrowWriter(newPool(t)), func(rec arrow.RecordBatch) (loader[TimeRecord], error) {
		return NewTimeRecordArrowReader(rec)
	}, in, derefs(in), pqarrow.DefaultWriterProps())
}

// TestTimeReadBackInUTC checks that the reader rebuilds a time.Time in UTC,
// the zone the column declares, whatever the process's local zone is, so
// String and == agree with a value that was written in UTC. The test moves
// the local zone somewhere unmistakable first, otherwise a machine running
// in UTC could not tell the two apart.
func TestTimeReadBackInUTC(t *testing.T) {
	saved := time.Local                              //nolint:gosmopolitan // the local zone is the subject of the test
	time.Local = time.FixedZone("Elsewhere", 3*3600) //nolint:gosmopolitan // see above
	t.Cleanup(func() { time.Local = saved })         //nolint:gosmopolitan // see above

	when := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	rec := encode(t, NewTimeRecordArrowWriter(newPool(t)), []*TimeRecord{{At: when, PAt: &when}})
	reader, err := NewTimeRecordArrowReader(rec)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	var got TimeRecord
	reader.LoadRow(0, &got)
	if got.At != when {
		t.Errorf("At = %v, want %v exactly, zone included", got.At, when)
	}
	if zone := got.At.Location().String(); zone != "UTC" {
		t.Errorf("At location = %q, want UTC", zone)
	}
}

// TestZeroTimeRoundTrips covers the ordinary case of an optional timestamp
// left unset: the zero time.Time, in a bare field, behind a pointer and as
// a list element, is written as a value the column holds and comes back as
// the zero time through IPC and Parquet.
func TestZeroTimeRoundTrips(t *testing.T) {
	var zero time.Time
	in := []*TimeRecord{{At: zero, PAt: &zero, Ats: []time.Time{zero}}}
	rec := encode(t, NewTimeRecordArrowWriter(newPool(t)), in)
	if column(t, rec, "At").IsNull(0) {
		t.Fatalf("the zero time.Time was written as null, want a value the column holds")
	}
	for _, carrier := range []struct {
		name string
		rec  arrow.RecordBatch
	}{
		{"ipc", ipcRoundTrip(t, rec)},
		{"parquet", parquetRoundTrip(t, rec)},
	} {
		t.Run(carrier.name, func(t *testing.T) {
			reader, err := NewTimeRecordArrowReader(carrier.rec)
			if err != nil {
				t.Fatalf("new reader: %v", err)
			}
			var got TimeRecord
			reader.LoadRow(0, &got)
			for name, v := range map[string]time.Time{"At": got.At, "PAt": *got.PAt, "Ats[0]": got.Ats[0]} {
				if !v.IsZero() {
					t.Errorf("%s = %v, want the zero time", name, v)
				}
			}
		})
	}
}

// TestTimeRangeEdges pins the boundary of the microsecond column: its two
// extreme instants round-trip exactly, every year a calendar can name
// round-trips exactly, and an instant past the edge is written as null and
// reads back as the zero time rather than as a wrapped date.
func TestTimeRangeEdges(t *testing.T) {
	cases := []struct {
		name string
		in   time.Time
		null bool
	}{
		{"minimum representable", minTimestamp, false},
		{"maximum representable", maxTimestamp, false},
		{"one microsecond past the maximum", maxTimestamp.Add(time.Microsecond), true},
		{"one microsecond before the minimum", minTimestamp.Add(-time.Microsecond), true},
		{"year 1600", time.Date(1600, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{"year 3000", time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{"year 9999", time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC), false},
	}
	in := make([]*TimeRecord, len(cases))
	for i, c := range cases {
		in[i] = &TimeRecord{At: c.in}
	}
	rec := encode(t, NewTimeRecordArrowWriter(newPool(t)), in)
	reader, err := NewTimeRecordArrowReader(rec)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	got := decode(t, reader, len(in))
	for i, c := range cases {
		if column(t, rec, "At").IsNull(i) != c.null {
			t.Errorf("%s: column null = %v, want %v", c.name, !c.null, c.null)
		}
		if c.null {
			if !got[i].At.IsZero() {
				t.Errorf("%s: got %v, want the zero time for a null", c.name, got[i].At)
			}
			continue
		}
		if !got[i].At.Equal(c.in) {
			t.Errorf("%s: got %v, want %v", c.name, got[i].At, c.in)
		}
	}
}

// TestTimestamppbFullRangeRoundTrips checks that the whole protobuf
// timestamp range, years 1 to 9999, round-trips exactly, since all of it
// fits in the microsecond column.
func TestTimestamppbFullRangeRoundTrips(t *testing.T) {
	year1 := timestamppb.New(time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC))
	year9999 := timestamppb.New(time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC))
	for _, ts := range []*timestamppb.Timestamp{year1, year9999} {
		if err := ts.CheckValid(); err != nil {
			t.Fatalf("fixture is not a valid protobuf timestamp: %v", err)
		}
	}
	rec := encode(t, NewTimeRecordArrowWriter(newPool(t)), []*TimeRecord{{PbAt: year1}, {PbAt: year9999}})
	reader, err := NewTimeRecordArrowReader(rec)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	got := decode(t, reader, 2)
	for i, want := range []*timestamppb.Timestamp{year1, year9999} {
		if got[i].PbAt == nil || !got[i].PbAt.AsTime().Equal(want.AsTime()) {
			t.Errorf("row %d: PbAt = %v, want %v", i, got[i].PbAt, want)
		}
	}
}

// TestDurationpbSaturates confirms the documented behaviour for the one
// out-of-range case the generator does handle: a protobuf duration longer
// than time.Duration can hold is clamped to the extreme, not wrapped.
func TestDurationpbSaturates(t *testing.T) {
	fourHundredYears := &durationpb.Duration{Seconds: 400 * 365 * 24 * 3600}
	if err := fourHundredYears.CheckValid(); err != nil {
		t.Fatalf("fixture is not a valid protobuf duration: %v", err)
	}
	rec := encode(t, NewTimeRecordArrowWriter(newPool(t)), []*TimeRecord{{PbDur: fourHundredYears}})
	reader, err := NewTimeRecordArrowReader(rec)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	var got TimeRecord
	reader.LoadRow(0, &got)
	if got.PbDur.AsDuration() != time.Duration(math.MaxInt64) {
		t.Fatalf("PbDur = %v, want the saturated maximum duration", got.PbDur.AsDuration())
	}
	if got.PbDur.GetSeconds() == fourHundredYears.GetSeconds() {
		t.Fatalf("a 400 year protobuf duration round-tripped exactly, which an Int64 of nanoseconds cannot hold")
	}
}
