package conformance

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestNilVersusZeroRoundTrip writes three rows, one with every nullable
// field nil, one with every field pointing at a zero value, and one fully
// populated, and compares without equating nil with empty. Every distinction
// survives except the byte slice: nil pointers stay nil, pointers to zero
// values stay pointers to zero values, and nil and empty containers stay
// apart.
func TestNilVersusZeroRoundTrip(t *testing.T) {
	zeroInt, zeroStr, zeroBool, zeroFloat := int64(0), "", false, 0.0
	epoch := time.Unix(0, 0).UTC()
	zeroDur := time.Duration(0)
	zeroBytes := []byte{}
	zeroSlice := []int64{}
	zeroMap := map[string]int64{}
	zeroIntPtr := &zeroInt

	seven, text, yes, half := int64(7), "text", true, 0.5
	when := time.Date(2026, 2, 3, 4, 5, 6, 7000, time.UTC)
	hour := time.Hour
	raw := []byte{1, 2}
	slice := []int64{1, 2, 3}
	m := map[string]int64{"k": 1}
	sevenPtr := &seven

	in := []*NilVsZero{
		{},
		{
			Int: &zeroInt, Str: &zeroStr, Bool: &zeroBool, Float: &zeroFloat,
			Bytes: []byte{}, PBytes: &zeroBytes,
			Struct: &NilSub{}, Value: NilSub{},
			Time: &epoch, Dur: &zeroDur,
			Slice: []int64{}, PSlice: &zeroSlice,
			Map: map[string]int64{}, PMap: &zeroMap,
			PP:    &zeroIntPtr,
			PbDur: &durationpb.Duration{}, PbTime: &timestamppb.Timestamp{},
		},
		{
			Int: &seven, Str: &text, Bool: &yes, Float: &half,
			Bytes: raw, PBytes: &raw,
			Struct: &NilSub{A: 1, B: "b"}, Value: NilSub{A: 2, B: "c"},
			Time: &when, Dur: &hour,
			Slice: slice, PSlice: &slice,
			Map: m, PMap: &m,
			PP:    &sevenPtr,
			PbDur: durationpb.New(90 * time.Minute), PbTime: timestamppb.New(when),
		},
	}
	roundTripBoth(t, NewNilVsZeroArrowWriter(newPool(t)), func(rec arrow.RecordBatch) (loader[NilVsZero], error) {
		return NewNilVsZeroArrowReader(rec)
	}, in, derefs(in), pqarrow.DefaultWriterProps())
}

// TestLoadRowIntoReusedStruct loads a fully populated row and then an
// all-nil row into the same struct, the reuse pattern the reader documents.
// Nothing from the first row may survive into the second.
func TestLoadRowIntoReusedStruct(t *testing.T) {
	seven, text := int64(7), "text"
	when := time.Date(2026, 2, 3, 4, 5, 6, 7000, time.UTC)
	slice := []int64{1, 2, 3}
	m := map[string]int64{"k": 1}
	sevenPtr := &seven
	in := []*NilVsZero{
		{
			Int: &seven, Str: &text, Bytes: []byte{1}, PBytes: &[]byte{2},
			Struct: &NilSub{A: 1, B: "b"}, Value: NilSub{A: 2, B: "c"},
			Time: &when, Slice: slice, PSlice: &slice, Map: m, PMap: &m, PP: &sevenPtr,
			PbDur: durationpb.New(time.Second), PbTime: timestamppb.New(when),
		},
		{},
		{Slice: []int64{}, Map: map[string]int64{}},
	}
	want := derefs(in)
	rec := encode(t, NewNilVsZeroArrowWriter(newPool(t)), in)
	reader, err := NewNilVsZeroArrowReader(rec)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}

	var got NilVsZero
	reader.LoadRow(0, &got)
	if diff := cmp.Diff(want[0], got, cmpOpts...); diff != "" {
		t.Fatalf("row 0 mismatch (-want +got):\n%s", diff)
	}
	reader.LoadRow(1, &got)
	if diff := cmp.Diff(want[1], got, cmpOpts...); diff != "" {
		t.Errorf("row 1 loaded over row 0 (-want +got):\n%s", diff)
	}
	reader.LoadRow(0, &got)
	reader.LoadRow(2, &got)
	if diff := cmp.Diff(want[2], got, cmpOpts...); diff != "" {
		t.Errorf("row 2 loaded over row 0 (-want +got):\n%s", diff)
	}
}
