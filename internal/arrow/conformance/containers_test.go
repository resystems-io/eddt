package conformance

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/google/go-cmp/cmp"
)

func TestScalarSlicesRoundTrip(t *testing.T) {
	in := []*ScalarSlices{
		{
			Ints:      []int64{-1, 0, 1},
			Strs:      []string{"", "x"},
			Bools:     []bool{true, false},
			Floats:    []float64{0.25, -1e300},
			Bytes:     []byte{0, 255},
			BytesList: [][]byte{{1}, nil, {}, {2, 3}},
			Runes:     []rune("héllo"),
			Times:     []time.Time{time.Date(2026, 5, 6, 7, 8, 9, 10000, time.UTC), time.Unix(0, 0)},
			Durs:      []time.Duration{-time.Second, 0, time.Hour},
		},
		{},
		{
			Ints:      []int64{},
			Strs:      []string{},
			Bools:     []bool{},
			Floats:    []float64{},
			Bytes:     []byte{},
			BytesList: [][]byte{},
			Runes:     []rune{},
			Times:     []time.Time{},
			Durs:      []time.Duration{},
		},
	}
	roundTripBoth(t, NewScalarSlicesArrowWriter(newPool(t)), func(rec arrow.RecordBatch) (loader[ScalarSlices], error) {
		return NewScalarSlicesArrowReader(rec)
	}, in, derefs(in), pqarrow.DefaultWriterProps())
}

func TestStructSlicesRoundTrip(t *testing.T) {
	in := []*StructSlices{
		{
			Vals:   []ContItem{{N: 1, S: "a"}, {}},
			Ptrs:   []*ContItem{{N: 2, S: "b"}, nil, {}},
			Nested: [][]ContItem{{{N: 3}}, nil, {}, {{N: 4}, {N: 5}}},
		},
		{},
		{Vals: []ContItem{}, Ptrs: []*ContItem{}, Nested: [][]ContItem{}},
	}
	roundTripBoth(t, NewStructSlicesArrowWriter(newPool(t)), func(rec arrow.RecordBatch) (loader[StructSlices], error) {
		return NewStructSlicesArrowReader(rec)
	}, in, derefs(in), pqarrow.DefaultWriterProps())
}

func TestJaggedRoundTrip(t *testing.T) {
	in := []*Jagged{
		{
			D2: [][]int32{{1, 2}, nil, {}, {3}},
			D3: [][][]string{{{"a"}, nil, {}}, nil, {}},
		},
		{},
	}
	roundTripBoth(t, NewJaggedArrowWriter(newPool(t)), func(rec arrow.RecordBatch) (loader[Jagged], error) {
		return NewJaggedArrowReader(rec)
	}, in, derefs(in), pqarrow.DefaultWriterProps())
}

func TestMapsRecordRoundTrip(t *testing.T) {
	in := []*MapsRecord{
		{
			Scalars:    map[string]int64{"a": 1, "b": 2},
			IntKeys:    map[int64]string{-1: "neg", 0: "zero"},
			BoolKeys:   map[bool]int32{true: 1, false: 0},
			FloatKeys:  map[float64]string{0.5: "half"},
			Structs:    map[string]ContItem{"x": {N: 10, S: "x"}, "zero": {}},
			PtrStructs: map[string]*ContItem{"set": {N: 11}, "nil": nil},
			Bytes:      map[string][]byte{"b": {9}, "nil": nil, "empty": {}},
			Times:      map[string]time.Time{"t": time.Date(2026, 7, 8, 9, 10, 11, 12000, time.UTC)},
			Lists:      map[string][]int64{"l": {1, 2}, "nil": nil, "empty": {}},
			OfMaps:     map[string]map[int64]ContItem{"outer": {1: {S: "inner"}}, "empty": {}, "nil": nil},
			Deep:       map[string]map[string]map[string]int64{"a": {"b": {"c": 3}}},
		},
		{},
		{
			Scalars:    map[string]int64{},
			IntKeys:    map[int64]string{},
			BoolKeys:   map[bool]int32{},
			FloatKeys:  map[float64]string{},
			Structs:    map[string]ContItem{},
			PtrStructs: map[string]*ContItem{},
			Bytes:      map[string][]byte{},
			Times:      map[string]time.Time{},
			Lists:      map[string][]int64{},
			OfMaps:     map[string]map[int64]ContItem{},
			Deep:       map[string]map[string]map[string]int64{},
		},
	}
	roundTripBoth(t, NewMapsRecordArrowWriter(newPool(t)), func(rec arrow.RecordBatch) (loader[MapsRecord], error) {
		return NewMapsRecordArrowReader(rec)
	}, in, derefs(in), pqarrow.DefaultWriterProps())
}

// TestNamedContainersRoundTrip covers named slice, map, array and byte
// slice types. The Parquet leg stores the Arrow schema because MAC is a
// fixed-size array (TestFixedSizeArraysThroughParquet).
func TestNamedContainersRoundTrip(t *testing.T) {
	in := []*NamedContainers{
		{
			Tags:   Tags{"a", "b"},
			Config: Config{"k": 1},
			MAC:    MAC{0, 1, 2, 3, 4, 5},
			Blob:   Blob{7, 8},
			Items:  Items{{N: 1, S: "i"}},
		},
		{},
	}
	roundTripBoth(t, NewNamedContainersArrowWriter(newPool(t)), func(rec arrow.RecordBatch) (loader[NamedContainers], error) {
		return NewNamedContainersArrowReader(rec)
	}, in, derefs(in), storedSchemaProps)
}

// TestPointerContainersRoundTrip covers the pointer-wrapped compounds, and
// pins the one place the encoding is lossy: a pointer to a nil pointer and a
// nil pointer both become null, so **T loses the difference between the two.
func TestPointerContainersRoundTrip(t *testing.T) {
	seven := int64(7)
	pseven := &seven
	var nilInner *int64
	strs := []string{"a"}
	emptyStrs := []string{}
	cfg := map[string]int64{"k": 2}
	emptyCfg := map[string]int64{}
	raw := []byte{1}
	emptyRaw := []byte{}
	items := []ContItem{{N: 3}}
	in := []*PointerContainers{
		{PSlice: &strs, PMap: &cfg, PBytes: &raw, PP: &pseven, PStructSlice: &items},
		{PSlice: &emptyStrs, PMap: &emptyCfg, PBytes: &emptyRaw},
		{},
	}
	roundTripBoth(t, NewPointerContainersArrowWriter(newPool(t)), func(rec arrow.RecordBatch) (loader[PointerContainers], error) {
		return NewPointerContainersArrowReader(rec)
	}, in, derefs(in), pqarrow.DefaultWriterProps())

	t.Run("pointer to nil pointer collapses to nil", func(t *testing.T) {
		rec := encode(t, NewPointerContainersArrowWriter(newPool(t)), []*PointerContainers{{PP: &nilInner}})
		reader, err := NewPointerContainersArrowReader(rec)
		if err != nil {
			t.Fatalf("new reader: %v", err)
		}
		var got PointerContainers
		reader.LoadRow(0, &got)
		if got.PP != nil {
			t.Fatalf("PP = %v, want nil: a **T whose outer pointer is set and inner pointer is nil is written as null", got.PP)
		}
	})
}

// TestNilByteSliceBecomesEmpty documents the one container kind that does
// not keep nil apart from empty. Every other slice writes nil as a null list
// and reads it back as nil (TestScalarSlicesRoundTrip). The writer never
// null-checks a byte slice, so a nil []byte is appended as a zero-length
// Binary value, and the reader returns a zero-length slice whose nil-ness
// depends only on the state of the Arrow buffer behind it. The same holds
// for a byte slice inside a list or a map, for a pointer to a nil byte
// slice, and for a named byte slice type.
func TestNilByteSliceIsNull(t *testing.T) {
	rec := encode(t, NewBaselineRecordArrowWriter(newPool(t)), []*BaselineRecord{{ID: "x"}, {ID: "y", Payload: []byte{}}})
	if !column(t, rec, "Payload").IsNull(0) {
		t.Fatalf("nil Payload was written as a value, want null")
	}
	if column(t, rec, "Payload").IsNull(1) {
		t.Fatalf("an empty Payload was written as null, want an empty value")
	}
	if !column(t, rec, "Bands").IsNull(0) {
		t.Fatalf("nil Bands was written as a value, want null")
	}
	reader, err := NewBaselineRecordArrowReader(ipcRoundTrip(t, rec))
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	got := decode(t, reader, 2)
	if got[0].Payload != nil {
		t.Errorf("Payload = %#v, want nil", got[0].Payload)
	}
	if got[0].Bands != nil {
		t.Errorf("Bands = %#v, want nil", got[0].Bands)
	}
	// An empty value reads back with length zero. Whether the slice is nil
	// or empty depends on the buffer the carrier hands the reader, so the
	// suite does not pin that half.
	if len(got[1].Payload) != 0 {
		t.Errorf("empty Payload = %#v, want length zero", got[1].Payload)
	}

	t.Run("pointer to nil byte slice", func(t *testing.T) {
		// A set pointer at a nil slice is null too, the pointee decides.
		rec := encode(t, NewPointerContainersArrowWriter(newPool(t)), []*PointerContainers{{PBytes: new([]byte)}})
		if !column(t, rec, "PBytes").IsNull(0) {
			t.Fatalf("a pointer to a nil byte slice was written as a value, want null")
		}
		reader, err := NewPointerContainersArrowReader(rec)
		if err != nil {
			t.Fatalf("new reader: %v", err)
		}
		var got PointerContainers
		reader.LoadRow(0, &got)
		if got.PBytes != nil {
			t.Errorf("PBytes = %#v, want nil", got.PBytes)
		}
	})

	t.Run("named byte slice", func(t *testing.T) {
		rec := encode(t, NewNamedContainersArrowWriter(newPool(t)), []*NamedContainers{{}})
		if !column(t, rec, "Blob").IsNull(0) {
			t.Fatalf("a nil named byte slice was written as a value, want null")
		}
		reader, err := NewNamedContainersArrowReader(ipcRoundTrip(t, rec))
		if err != nil {
			t.Fatalf("new reader: %v", err)
		}
		var got NamedContainers
		reader.LoadRow(0, &got)
		if got.Blob != nil {
			t.Errorf("Blob = %#v, want nil", got.Blob)
		}
	})
}

func arraysFixture() []*ArraysRecord {
	return []*ArraysRecord{
		{
			Tag:    [4]byte{0xDE, 0xAD, 0xBE, 0xEF},
			Items:  [3]ContItem{{N: 1, S: "a"}, {N: 2, S: "b"}, {N: 3, S: "c"}},
			Strs:   [2]string{"x", "y"},
			Matrix: [2][2]int32{{1, 2}, {3, 4}},
		},
		{},
	}
}

func TestArraysRecordRoundTrip(t *testing.T) {
	in := arraysFixture()
	roundTripBoth(t, NewArraysRecordArrowWriter(newPool(t)), func(rec arrow.RecordBatch) (loader[ArraysRecord], error) {
		return NewArraysRecordArrowReader(rec)
	}, in, derefs(in), storedSchemaProps)
}

// TestFixedSizeArraysThroughParquet shows that a fixed-size array survives
// Parquet only when the writer stores the Arrow schema in the file. With the
// sink's settings (pqarrow.DefaultWriterProps) the column comes back as a
// plain list and the generated reader refuses the batch, so a Go consumer
// cannot read a fixed-size array field out of the lake as written today.
func TestFixedSizeArraysThroughParquet(t *testing.T) {
	rec := encode(t, NewArraysRecordArrowWriter(newPool(t)), arraysFixture())

	_, err := NewArraysRecordArrowReader(parquetRoundTrip(t, rec))
	if err == nil {
		t.Fatalf("reader accepted a batch read back with the default Parquet writer settings: fixed-size arrays now survive, update this test and the README")
	}
	if want := `column "Tag": expected *array.FixedSizeList, got *array.List`; !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %q, want it to contain %q", err, want)
	}

	reader, err := NewArraysRecordArrowReader(parquetRoundTripWith(t, rec, storedSchemaProps))
	if err != nil {
		t.Fatalf("new reader after Parquet with stored schema: %v", err)
	}
	got := decode(t, reader, 2)
	for i, want := range arraysFixture() {
		if diff := cmp.Diff(*want, got[i]); diff != "" {
			t.Errorf("row %d mismatch (-want +got):\n%s", i, diff)
		}
	}
}

func TestScaleRecordRoundTrip(t *testing.T) {
	const n = 50_000
	items := make([]ContItem, n)
	for i := int32(0); i < n; i++ {
		items[i] = ContItem{N: i, S: fmt.Sprintf("row-%d", i)}
	}
	in := []*ScaleRecord{{Items: items}}
	rec := encode(t, NewScaleRecordArrowWriter(newPool(t)), in)
	reader, err := NewScaleRecordArrowReader(parquetRoundTrip(t, rec))
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	got := decode(t, reader, 1)
	if len(got[0].Items) != n {
		t.Fatalf("got %d items, want %d", len(got[0].Items), n)
	}
	for i, item := range got[0].Items {
		if item != items[i] {
			t.Fatalf("item %d = %+v, want %+v", i, item, items[i])
		}
	}
}
