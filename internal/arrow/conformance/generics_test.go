package conformance

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func TestGenericRecordRoundTrip(t *testing.T) {
	// The two instantiations of Pair must get their own schemas, with the
	// key and value columns typed the way each was instantiated.
	schema := NewGenericRecordSchema()
	si, ok := schema.Field(0).Type.(*arrow.StructType)
	if !ok || si.NumFields() != 2 || !arrow.TypeEqual(si.Field(0).Type, arrow.BinaryTypes.String) || !arrow.TypeEqual(si.Field(1).Type, arrow.PrimitiveTypes.Int64) {
		t.Fatalf("SI column = %v, want struct<Key: utf8, Value: int64>", schema.Field(0).Type)
	}
	is, ok := schema.Field(1).Type.(*arrow.StructType)
	if !ok || is.NumFields() != 2 || !arrow.TypeEqual(is.Field(0).Type, arrow.PrimitiveTypes.Int64) || !arrow.TypeEqual(is.Field(1).Type, arrow.BinaryTypes.String) {
		t.Fatalf("IS column = %v, want struct<Key: int64, Value: utf8>", schema.Field(1).Type)
	}

	text := "opt"
	in := []*GenericRecord{
		{
			SI:        Pair[string, int64]{Key: "k", Value: 1},
			IS:        Pair[int64, string]{Key: 2, Value: "v"},
			Items:     List[GenItem]{Items: []GenItem{{N: 1}, {N: 2}}},
			PtrItems:  List[*GenItem]{Items: []*GenItem{{N: 3}, nil}},
			Opt:       Optional[string]{Value: &text, Set: true},
			OptStruct: Optional[GenItem]{Value: &GenItem{N: 4}, Set: true},
			PPair:     &Pair[string, int64]{Key: "p", Value: 5},
			Pairs:     []Pair[string, int64]{{Key: "a", Value: 6}, {}},
			PairMap:   map[string]Pair[string, int64]{"m": {Key: "b", Value: 7}},
		},
		{},
		{Items: List[GenItem]{Items: []GenItem{}}, Pairs: []Pair[string, int64]{}, PairMap: map[string]Pair[string, int64]{}},
	}
	roundTripBoth(t, NewGenericRecordArrowWriter(newPool(t)), func(rec arrow.RecordBatch) (loader[GenericRecord], error) {
		return NewGenericRecordArrowReader(rec)
	}, in, derefs(in), pqarrow.DefaultWriterProps())
}
