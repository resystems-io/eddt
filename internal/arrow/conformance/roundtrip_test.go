package conformance

import (
	"bytes"
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/testing/protocmp"
)

// cmpOpts compares decoded rows with the rows that were encoded. Times are
// compared by instant, the zone the reader uses is the subject of
// TestTimeReadBackInUTC. Nothing equates nil with empty: whether that
// distinction survives for every container kind, byte slices included, is
// one of the questions the suite answers.
var cmpOpts = []cmp.Option{
	cmp.Comparer(func(a, b time.Time) bool { return a.Equal(b) }),
	cmpopts.EquateComparable(netip.Addr{}, netip.Prefix{}),
	protocmp.Transform(),
}

// storedSchemaProps makes the Parquet writer embed the Arrow schema in the
// file, which is what lets a fixed-size list come back as one instead of a
// plain list. The data lake sink writes with pqarrow.DefaultWriterProps(),
// which does not (TestFixedSizeArraysThroughParquet).
var storedSchemaProps = pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())

// appender is the shape shared by every writer the generator emits. It is
// declared here only so the helpers below can drive any of them.
type appender[T any] interface {
	Append(*T)
	NewRecordBatch() arrow.RecordBatch
	Release()
}

// loader is the shape shared by every reader the generator emits.
type loader[T any] interface {
	LoadRow(int, *T)
}

// newPool returns an allocator that fails the test if any buffer is still
// allocated when the test ends, so a leak in generated code is caught.
func newPool(t *testing.T) memory.Allocator {
	t.Helper()
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	t.Cleanup(func() { pool.AssertSize(t, 0) })
	return pool
}

// encode appends every row through the generated writer and returns the
// batch. The batch is released when the test ends.
func encode[T any](t *testing.T, w appender[T], rows []*T) arrow.RecordBatch {
	t.Helper()
	t.Cleanup(w.Release)
	for _, row := range rows {
		w.Append(row)
	}
	rec := w.NewRecordBatch()
	t.Cleanup(rec.Release)
	return rec
}

// decode reads n rows through the generated reader, each into a fresh
// struct, and returns them.
func decode[T any](t *testing.T, r loader[T], n int) []T {
	t.Helper()
	out := make([]T, n)
	for i := range out {
		r.LoadRow(i, &out[i])
	}
	return out
}

// derefs returns the rows behind a slice of pointers, for tests whose
// expected output is exactly their input.
func derefs[T any](rows []*T) []T {
	out := make([]T, len(rows))
	for i, row := range rows {
		out[i] = *row
	}
	return out
}

// roundTripBoth encodes rows, carries the batch through IPC and through
// Parquet, and compares what each reader loads with want.
func roundTripBoth[T any](t *testing.T, w appender[T], newReader func(arrow.RecordBatch) (loader[T], error), in []*T, want []T, parquetProps pqarrow.ArrowWriterProperties) {
	t.Helper()
	rec := encode(t, w, in)
	for _, carrier := range []struct {
		name string
		rec  arrow.RecordBatch
	}{
		{"ipc", ipcRoundTrip(t, rec)},
		{"parquet", parquetRoundTripWith(t, rec, parquetProps)},
	} {
		t.Run(carrier.name, func(t *testing.T) {
			reader, err := newReader(carrier.rec)
			if err != nil {
				t.Fatalf("new reader: %v", err)
			}
			got := decode(t, reader, len(want))
			for i := range want {
				if diff := cmp.Diff(want[i], got[i], cmpOpts...); diff != "" {
					t.Errorf("row %d mismatch (-want +got):\n%s", i, diff)
				}
			}
		})
	}
}

// ipcRoundTrip carries a batch through an Arrow IPC stream, the way the
// data lake producers hand records to the sink.
func ipcRoundTrip(t *testing.T, rec arrow.RecordBatch) arrow.RecordBatch {
	t.Helper()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()))
	if err := w.Write(rec); err != nil {
		t.Fatalf("write IPC: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close IPC writer: %v", err)
	}

	r, err := ipc.NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("open IPC stream: %v", err)
	}
	t.Cleanup(r.Release)
	if !r.Next() {
		t.Fatalf("IPC stream carries no record batch: %v", r.Err())
	}
	out := r.RecordBatch()
	out.Retain()
	t.Cleanup(out.Release)
	return out
}

// parquetRoundTrip writes a batch to a Parquet file with the sink's writer
// settings and reads it back as one batch, the way anything downstream of
// the lake sees the data.
func parquetRoundTrip(t *testing.T, rec arrow.RecordBatch) arrow.RecordBatch {
	t.Helper()
	return parquetRoundTripWith(t, rec, pqarrow.DefaultWriterProps())
}

// parquetRoundTripWith is parquetRoundTrip with explicit Arrow-level writer
// properties.
func parquetRoundTripWith(t *testing.T, rec arrow.RecordBatch, arrowProps pqarrow.ArrowWriterProperties) arrow.RecordBatch {
	t.Helper()

	var buf bytes.Buffer
	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Zstd))
	pw, err := pqarrow.NewFileWriter(rec.Schema(), &buf, props, arrowProps)
	if err != nil {
		t.Fatalf("open Parquet writer: %v", err)
	}
	if err := pw.Write(rec); err != nil {
		t.Fatalf("write Parquet: %v", err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("close Parquet writer: %v", err)
	}

	pf, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("open Parquet file: %v", err)
	}
	t.Cleanup(func() { pf.Close() })
	fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("open Parquet as Arrow: %v", err)
	}
	tbl, err := fr.ReadTable(context.Background())
	if err != nil {
		t.Fatalf("read Parquet table: %v", err)
	}
	t.Cleanup(tbl.Release)

	tr := array.NewTableReader(tbl, -1)
	t.Cleanup(tr.Release)
	if !tr.Next() {
		t.Fatalf("Parquet table carries no record batch: %v", tr.Err())
	}
	out := tr.RecordBatch()
	out.Retain()
	t.Cleanup(out.Release)
	return out
}

// column returns the named top-level column of a batch.
func column(t *testing.T, rec arrow.RecordBatch, name string) arrow.Array {
	t.Helper()
	idx := rec.Schema().FieldIndices(name)
	if len(idx) != 1 {
		t.Fatalf("column %q: found %d times, want once", name, len(idx))
	}
	return rec.Column(idx[0])
}

// fieldNames lists the top-level column names of a schema, for asserting
// which fields the generator kept.
func fieldNames(schema *arrow.Schema) []string {
	names := make([]string, 0, schema.NumFields())
	for _, f := range schema.Fields() {
		names = append(names, f.Name)
	}
	return names
}
