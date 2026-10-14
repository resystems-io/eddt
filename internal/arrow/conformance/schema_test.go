package conformance

import (
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// baselineBatch builds a batch by hand from an arbitrary schema, so the
// reader can be handed shapes the writer would never produce: missing
// columns, extra columns, wrong types, shuffled order, dictionary encoding.
func baselineBatch(t *testing.T, schema *arrow.Schema, fill func(b *array.RecordBuilder)) arrow.RecordBatch {
	t.Helper()
	b := array.NewRecordBuilder(newPool(t), schema)
	t.Cleanup(b.Release)
	fill(b)
	rec := b.NewRecordBatch()
	t.Cleanup(rec.Release)
	return rec
}

// TestReaderZeroesMissingColumns checks the behaviour for a record that
// lacks columns the struct has, which is what an older Parquet file looks
// like to a newer struct: construction succeeds, the present columns load,
// and a field with no column is set to its zero value, so struct reuse
// across rows cannot carry a stale value into it.
func TestReaderZeroesMissingColumns(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "ID", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "Count", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
	}, nil)
	rec := baselineBatch(t, schema, func(b *array.RecordBuilder) {
		b.Field(0).(*array.StringBuilder).Append("only-two-columns")
		b.Field(1).(*array.Uint64Builder).Append(3)
	})
	reader, err := NewBaselineRecordArrowReader(rec)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}

	stale := "stale"
	got := BaselineRecord{Name: &stale, Payload: []byte{9}, Bands: []uint64{1}, Nested: &BaselineNested{Flag: true}, CollectedAt: time.Unix(1, 0)}
	reader.LoadRow(0, &got)
	if got.ID != "only-two-columns" || got.Count == nil || *got.Count != 3 {
		t.Errorf("present columns not loaded: %+v", got)
	}
	if got.Name != nil || got.Payload != nil || got.Bands != nil || got.Nested != nil || !got.CollectedAt.IsZero() {
		t.Errorf("fields without a column kept a stale value: %+v", got)
	}
}

func TestReaderIgnoresExtraColumns(t *testing.T) {
	fields := append(NewBaselineRecordSchema().Fields(), arrow.Field{Name: "Extra", Type: arrow.PrimitiveTypes.Int64, Nullable: true})
	rec := baselineBatch(t, arrow.NewSchema(fields, nil), func(b *array.RecordBuilder) {
		b.Field(0).(*array.StringBuilder).Append("x")
		for i := 1; i < len(fields)-1; i++ {
			b.Field(i).AppendNull()
		}
		b.Field(len(fields) - 1).(*array.Int64Builder).Append(42)
	})
	reader, err := NewBaselineRecordArrowReader(rec)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	var got BaselineRecord
	reader.LoadRow(0, &got)
	if got.ID != "x" {
		t.Errorf("ID = %q, want x", got.ID)
	}
}

func TestReaderRejectsWrongColumnType(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "ID", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	rec := baselineBatch(t, schema, func(b *array.RecordBuilder) {
		b.Field(0).(*array.Int64Builder).Append(1)
	})
	_, err := NewBaselineRecordArrowReader(rec)
	if err == nil {
		t.Fatalf("reader accepted an Int64 column for a string field")
	}
	if want := `column "ID": expected *array.String or *array.Dictionary, got *array.Int64`; !strings.Contains(err.Error(), want) {
		t.Errorf("error = %q, want it to contain %q", err, want)
	}
}

func TestReaderFindsColumnsByName(t *testing.T) {
	original := NewBaselineRecordSchema().Fields()
	reversed := make([]arrow.Field, len(original))
	for i, f := range original {
		reversed[len(original)-1-i] = f
	}
	rec := baselineBatch(t, arrow.NewSchema(reversed, nil), func(b *array.RecordBuilder) {
		for i := range reversed {
			switch reversed[i].Name {
			case "ID":
				b.Field(i).(*array.StringBuilder).Append("by-name")
			case "Count":
				b.Field(i).(*array.Uint64Builder).Append(5)
			default:
				b.Field(i).AppendNull()
			}
		}
	})
	reader, err := NewBaselineRecordArrowReader(rec)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	var got BaselineRecord
	reader.LoadRow(0, &got)
	if got.ID != "by-name" || got.Count == nil || *got.Count != 5 {
		t.Errorf("columns were not matched by name: %+v", got)
	}
}

func TestReaderToleratesMissingNestedField(t *testing.T) {
	nested := arrow.StructOf(arrow.Field{Name: "Flag", Type: arrow.FixedWidthTypes.Boolean, Nullable: true})
	schema := arrow.NewSchema([]arrow.Field{{Name: "Nested", Type: nested, Nullable: true}}, nil)
	rec := baselineBatch(t, schema, func(b *array.RecordBuilder) {
		sb := b.Field(0).(*array.StructBuilder)
		sb.Append(true)
		sb.FieldBuilder(0).(*array.BooleanBuilder).Append(true)
	})
	reader, err := NewBaselineRecordArrowReader(rec)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	var got BaselineRecord
	reader.LoadRow(0, &got)
	if got.Nested == nil || !got.Nested.Flag || got.Nested.Value != 0 {
		t.Errorf("Nested = %+v, want Flag set and Value zero", got.Nested)
	}
}

// TestReaderDictionaryEncodedStrings checks the documented dictionary
// support: a dictionary-encoded string column at the top level is decoded
// transparently, while the same encoding inside a list is rejected at
// construction.
func TestReaderDictionaryEncodedStrings(t *testing.T) {
	dictType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String}

	t.Run("top level", func(t *testing.T) {
		pool := newPool(t)
		db := array.NewDictionaryBuilder(pool, dictType).(*array.BinaryDictionaryBuilder)
		defer db.Release()
		for _, v := range []string{"a", "b", "a"} {
			if err := db.AppendString(v); err != nil {
				t.Fatalf("append: %v", err)
			}
		}
		db.AppendNull()
		col := db.NewArray()
		defer col.Release()

		schema := arrow.NewSchema([]arrow.Field{{Name: "ID", Type: dictType, Nullable: true}}, nil)
		rec := array.NewRecordBatch(schema, []arrow.Array{col}, 4)
		defer rec.Release()

		reader, err := NewBaselineRecordArrowReader(rec)
		if err != nil {
			t.Fatalf("new reader: %v", err)
		}
		got := decode(t, reader, 4)
		if got[0].ID != "a" || got[1].ID != "b" || got[2].ID != "a" || got[3].ID != "" {
			t.Errorf("IDs = %q %q %q %q, want a b a and empty", got[0].ID, got[1].ID, got[2].ID, got[3].ID)
		}
	})

	t.Run("inside a list", func(t *testing.T) {
		pool := newPool(t)
		lb := array.NewListBuilder(pool, dictType)
		defer lb.Release()
		lb.Append(true)
		if err := lb.ValueBuilder().(*array.BinaryDictionaryBuilder).AppendString("a"); err != nil {
			t.Fatalf("append: %v", err)
		}
		col := lb.NewArray()
		defer col.Release()

		schema := arrow.NewSchema([]arrow.Field{{Name: "Strs", Type: arrow.ListOf(dictType), Nullable: true}}, nil)
		rec := array.NewRecordBatch(schema, []arrow.Array{col}, 1)
		defer rec.Release()

		_, err := NewScalarSlicesArrowReader(rec)
		if err == nil {
			t.Fatalf("reader accepted a dictionary-encoded list: support was added, update this test and the README")
		}
		if want := "expected *array.String, got *array.Dictionary"; !strings.Contains(err.Error(), want) {
			t.Errorf("error = %q, want it to contain %q", err, want)
		}
	})
}

// TestBatchAllocatorIsReleased is a guard for the helpers themselves: a
// batch built through the generated writer and released must leave the
// checked allocator empty.
func TestBatchAllocatorIsReleased(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	w := NewBaselineRecordArrowWriter(pool)
	w.Append(&BaselineRecord{ID: "x", Bands: []uint64{1, 2}})
	rec := w.NewRecordBatch()
	rec.Release()
	w.Release()
	pool.AssertSize(t, 0)
}
