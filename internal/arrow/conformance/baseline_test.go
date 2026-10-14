package conformance

import (
	"math"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/google/go-cmp/cmp"
)

func TestBaselineRecordRoundTrip(t *testing.T) {
	name := "widget"
	count := uint64(42)
	in := []*BaselineRecord{
		{
			ID:          "a",
			CollectedAt: time.Date(2026, 1, 2, 3, 4, 5, 6000, time.UTC),
			Count:       &count,
			Name:        &name,
			Payload:     []byte{1, 2, 3},
			Bands:       []uint64{3, 7, 20},
			Nested:      &BaselineNested{Flag: true, Value: 7},
		},
		{ID: "b", CollectedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)},
	}
	roundTripBoth(t, NewBaselineRecordArrowWriter(newPool(t)), func(rec arrow.RecordBatch) (loader[BaselineRecord], error) {
		return NewBaselineRecordArrowReader(rec)
	}, in, derefs(in), pqarrow.DefaultWriterProps())
}

// TestPrimitivesRecordRoundTrip pushes every primitive to the edge of its
// range and checks the widened types (int to Int64, uint to Uint64, rune to
// Int32) come back exact. The unexported field is compared too: the
// generator includes it when the output package is the input package.
func TestPrimitivesRecordRoundTrip(t *testing.T) {
	extreme := NewPrimitivesRecord("hidden but present")
	extreme.I8, extreme.I16, extreme.I32, extreme.I64, extreme.I = math.MinInt8, math.MinInt16, math.MinInt32, math.MinInt64, math.MinInt
	extreme.U8, extreme.U16, extreme.U32, extreme.U64, extreme.U = math.MaxUint8, math.MaxUint16, math.MaxUint32, math.MaxUint64, math.MaxUint
	extreme.F32, extreme.F64 = math.MaxFloat32, math.SmallestNonzeroFloat64
	extreme.R = utf8MaxRune
	in := []*PrimitivesRecord{NewPrimitivesRecord("first"), extreme, {}}

	rec := encode(t, NewPrimitivesRecordArrowWriter(newPool(t)), in)
	reader, err := NewPrimitivesRecordArrowReader(ipcRoundTrip(t, rec))
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	got := decode(t, reader, len(in))
	for i, want := range in {
		if diff := cmp.Diff(*want, got[i], cmp.AllowUnexported(PrimitivesRecord{})); diff != "" {
			t.Errorf("row %d mismatch (-want +got):\n%s", i, diff)
		}
	}

	schema := NewPrimitivesRecordSchema()
	wantTypes := map[string]arrow.DataType{
		"I":      arrow.PrimitiveTypes.Int64,
		"U":      arrow.PrimitiveTypes.Uint64,
		"R":      arrow.PrimitiveTypes.Int32,
		"By":     arrow.PrimitiveTypes.Uint8,
		"Status": arrow.PrimitiveTypes.Int32,
	}
	for name, want := range wantTypes {
		idx := schema.FieldIndices(name)
		if len(idx) != 1 {
			t.Errorf("column %q: found %d times, want once", name, len(idx))
			continue
		}
		if got := schema.Field(idx[0]).Type; !arrow.TypeEqual(got, want) {
			t.Errorf("column %q: type %v, want %v", name, got, want)
		}
	}
	if got := schema.FieldIndices("_"); len(got) != 0 {
		t.Errorf("blank field made it into the schema")
	}
	for _, name := range []string{"Start", "End", "hidden"} {
		if got := schema.FieldIndices(name); len(got) != 1 {
			t.Errorf("column %q: found %d times, want once", name, len(got))
		}
	}
	for _, f := range schema.Fields() {
		if !f.Nullable {
			t.Errorf("column %q is not nullable: the generator marks every column nullable, including non-pointer Go fields", f.Name)
		}
	}
}

const utf8MaxRune = '\U0010FFFF'

// TestWriterReusedAcrossBatches checks that a writer keeps working after
// NewRecordBatch, which is how the data lake producers use it: one writer,
// one single-row batch per published message.
func TestWriterReusedAcrossBatches(t *testing.T) {
	w := NewBaselineRecordArrowWriter(newPool(t))
	t.Cleanup(w.Release)

	var batches []arrow.RecordBatch
	for i := range 3 {
		w.Append(&BaselineRecord{ID: string(rune('a' + i)), CollectedAt: time.Unix(int64(i), 0)})
		rec := w.NewRecordBatch()
		t.Cleanup(rec.Release)
		batches = append(batches, rec)
	}
	for i, rec := range batches {
		if rec.NumRows() != 1 {
			t.Fatalf("batch %d has %d rows, want 1", i, rec.NumRows())
		}
		reader, err := NewBaselineRecordArrowReader(rec)
		if err != nil {
			t.Fatalf("batch %d: new reader: %v", i, err)
		}
		var got BaselineRecord
		reader.LoadRow(0, &got)
		if want := string(rune('a' + i)); got.ID != want {
			t.Errorf("batch %d: ID = %q, want %q", i, got.ID, want)
		}
	}
}
