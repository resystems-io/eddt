package fixedptr

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// TestNilArrayElementIsANullableNull shows the schema declares the elements
// of an array of pointers nullable, the writer appends a null for a nil
// element, and a Parquet file keeps that element in place.
func TestNilArrayElementIsANullableNull(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	one := int64(1)
	w := NewFixedPtrArrowWriter(pool)
	defer w.Release()
	w.Append(&FixedPtr{P: [2]*int64{&one, nil}})
	rec := w.NewRecordBatch()
	defer rec.Release()

	col := rec.Column(0).(*array.FixedSizeList)
	if !col.DataType().(*arrow.FixedSizeListType).ElemField().Nullable {
		t.Fatalf("element field is not nullable, so a nil element has no honest representation")
	}
	if col.ListValues().NullN() != 1 {
		t.Fatalf("list values carry %d nulls, want 1", col.ListValues().NullN())
	}

	var buf bytes.Buffer
	pw, err := pqarrow.NewFileWriter(rec.Schema(), &buf, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatalf("open Parquet writer: %v", err)
	}
	if err := pw.Write(rec); err != nil {
		t.Fatalf("write Parquet: %v", err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("close Parquet: %v", err)
	}
	pf, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("open Parquet: %v", err)
	}
	defer pf.Close()
	fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("open Parquet as Arrow: %v", err)
	}
	tbl, err := fr.ReadTable(context.Background())
	if err != nil {
		t.Fatalf("read table: %v", err)
	}
	defer tbl.Release()
	back := tbl.Column(0).Data().Chunk(0).(*array.List)
	start, end := back.ValueOffsets(0)
	if int(end-start) != 2 || back.ListValues().Len() != 2 || !back.ListValues().IsNull(1) {
		t.Fatalf("after Parquet: offsets [%d,%d) over %d values, %d nulls, want both elements with the second null", start, end, back.ListValues().Len(), back.ListValues().NullN())
	}
}
