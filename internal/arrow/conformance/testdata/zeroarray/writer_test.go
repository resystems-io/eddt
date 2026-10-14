package zeroarray

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestZeroLengthArrayIsLeftOut shows the generated writer constructs and
// appends without the zero-length array, which the generator skipped with
// a warning, so the struct's other field still reaches a batch.
func TestZeroLengthArrayIsLeftOut(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	w := NewZeroArrayArrowWriter(pool)
	defer w.Release()
	w.Append(&ZeroArray{K: "k"})
	rec := w.NewRecordBatch()
	defer rec.Release()
	if rec.NumCols() != 1 || rec.Schema().Field(0).Name != "K" {
		t.Fatalf("schema = %v, want the single column K", rec.Schema())
	}
}
