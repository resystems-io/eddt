package ptrunmarshal

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestReaderAllocatesElementPointers feeds the generated reader a list of
// strings, the column the writer produces for this field, and shows each
// element comes back as a pointer to a parsed value and a null element as
// nil.
func TestReaderAllocatesElementPointers(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "Items", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true}}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	lb := b.Field(0).(*array.ListBuilder)
	lb.Append(true)
	lb.ValueBuilder().(*array.StringBuilder).Append("value")
	lb.ValueBuilder().(*array.StringBuilder).AppendNull()
	rec := b.NewRecordBatch()
	defer rec.Release()

	reader, err := NewPtrUnmarshalArrowReader(rec)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	var out PtrUnmarshal
	reader.LoadRow(0, &out)
	if errs := reader.Errors(); len(errs) != 0 {
		t.Fatalf("errors = %v, want none", errs)
	}
	if len(out.Items) != 2 || out.Items[0] == nil || out.Items[0].V != "value" || out.Items[1] != nil {
		t.Fatalf("Items = %+v, want a parsed pointer and a nil", out.Items)
	}
}
