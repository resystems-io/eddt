package conformance

import (
	"net"
	"net/netip"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/types/descriptorpb"

	"go.resystems.io/eddt/internal/arrow/conformance/external"
)

// TestFallbackChainRoundTrip covers the external-type fallback chain in
// both directions and pins its asymmetries. TextMarshaler and
// BinaryMarshaler types round-trip. A type whose only rung is Stringer is
// write-only: the column holds the String() output and the reader leaves
// the field zero, which for an enum means the zero enum value comes back
// with no error. A named slice of such a type is a column of names, and
// write-only for the same reason. A marshal error is written as null and
// reported through the writer's Errors.
func TestFallbackChainRoundTrip(t *testing.T) {
	text := external.TextOnly{V: "pointer"}
	stringer := external.StringerOnly{V: "pointer"}
	bin := external.BinaryOnly{V: []byte{4, 5}}
	addr := netip.MustParseAddr("2001:db8::1")
	in := []*FallbackRecord{
		{
			Text:    external.TextOnly{V: "text"},
			PText:   &text,
			Texts:   []external.TextOnly{{V: "a"}, {V: ""}},
			TextMap: map[string]external.TextOnly{"k": {V: "v"}},
			Str:     external.StringerOnly{V: "value"},
			PStr:    &stringer,
			Bin:     external.BinaryOnly{V: []byte{1, 2, 3}},
			PBin:    &bin,
			Both:    external.TextAndBinary{V: "both"},
			PtrRecv: external.PointerText{V: "ptr"},
			Addr:    netip.MustParseAddr("192.0.2.1"),
			PAddr:   &addr,
			Prefix:  netip.MustParsePrefix("10.0.0.0/8"),
			IP:      net.ParseIP("198.51.100.7"),
			Enum:    descriptorpb.FieldDescriptorProto_TYPE_STRING,
			Level:   external.LevelHigh,
			Levels:  external.Levels{external.LevelHigh},
			Failing: external.FailingMarshal{V: "refused", Fail: true},
		},
		{},
	}
	want := derefs(in)
	want[0].Str = external.StringerOnly{}
	want[0].PStr = nil
	want[0].Enum = 0
	want[0].Level = external.LevelLow
	want[0].Levels = nil
	want[0].Failing = external.FailingMarshal{}

	w := NewFallbackRecordArrowWriter(newPool(t))
	rec := encode(t, w, in)
	if got := fieldNames(rec.Schema()); !slicesContain(got, "Levels") {
		t.Errorf("Levels is not a column, want the named slice unwrapped to a list of names")
	}
	if !column(t, rec, "Failing").IsNull(0) {
		t.Errorf("column Failing holds a value, want null for a marshal that failed")
	}
	if errs := w.Errors(); len(errs) != 1 || errs[0].Row != 0 || errs[0].Field != "Failing" {
		t.Errorf("writer errors = %v, want one for Failing in row 0", errs)
	}
	wantColumns := map[string]string{
		"Both":  "text:both",
		"Str":   "stringer:value",
		"Enum":  "TYPE_STRING",
		"Level": "high",
	}
	for name, wantValue := range wantColumns {
		col := column(t, rec, name)
		if col.IsNull(0) {
			t.Errorf("column %q is null, want %q", name, wantValue)
			continue
		}
		if got := col.(*array.String).Value(0); got != wantValue {
			t.Errorf("column %q = %q, want %q", name, got, wantValue)
		}
	}

	for _, carrier := range []struct {
		name string
		rec  arrow.RecordBatch
	}{
		{"ipc", ipcRoundTrip(t, rec)},
		{"parquet", parquetRoundTrip(t, rec)},
	} {
		t.Run(carrier.name, func(t *testing.T) {
			reader, err := NewFallbackRecordArrowReader(carrier.rec)
			if err != nil {
				t.Fatalf("new reader: %v", err)
			}
			got := decode(t, reader, len(want))
			if errs := reader.Errors(); len(errs) != 0 {
				t.Errorf("unexpected read errors: %v", errs)
			}
			for i := range want {
				if diff := cmp.Diff(want[i], got[i], cmpOpts...); diff != "" {
					t.Errorf("row %d mismatch (-want +got):\n%s", i, diff)
				}
			}
		})
	}
}

func slicesContain(list []string, s string) bool {
	for _, item := range list {
		if item == s {
			return true
		}
	}
	return false
}

// strictRow is one hand-built StrictRecord row: the raw column text for
// every position that holds a Strict value, so a test can plant malformed
// payloads exactly where it wants them.
type strictRow struct {
	id, s, ps string
	ss        []string
	smap      [][2]string
	holder    string
	pholder   string
}

// appendStrictRow appends one row to a builder created from
// NewStrictRecordSchema, writing the column text as given.
func appendStrictRow(b *array.RecordBuilder, row strictRow) {
	b.Field(0).(*array.StringBuilder).Append(row.id)
	b.Field(1).(*array.StringBuilder).Append(row.s)
	b.Field(2).(*array.StringBuilder).Append(row.ps)

	list := b.Field(3).(*array.ListBuilder)
	list.Append(true)
	for _, s := range row.ss {
		list.ValueBuilder().(*array.StringBuilder).Append(s)
	}

	m := b.Field(4).(*array.MapBuilder)
	m.Append(true)
	for _, kv := range row.smap {
		m.KeyBuilder().(*array.StringBuilder).Append(kv[0])
		m.ItemBuilder().(*array.StringBuilder).Append(kv[1])
	}

	holder := b.Field(5).(*array.StructBuilder)
	holder.Append(true)
	holder.FieldBuilder(0).(*array.StringBuilder).Append(row.holder)

	pholder := b.Field(6).(*array.StructBuilder)
	pholder.Append(true)
	pholder.FieldBuilder(0).(*array.StringBuilder).Append(row.pholder)
}

func strictBatch(t *testing.T, rows ...strictRow) arrow.RecordBatch {
	t.Helper()
	b := array.NewRecordBuilder(newPool(t), NewStrictRecordSchema())
	t.Cleanup(b.Release)
	for _, row := range rows {
		appendStrictRow(b, row)
	}
	rec := b.NewRecordBatch()
	t.Cleanup(rec.Release)
	return rec
}

// TestMalformedPayloadErrors hands the generated reader a batch whose
// Strict columns carry text UnmarshalText rejects, in every position the
// reader handles differently, and checks what Errors() reports and what
// each field holds afterwards.
func TestMalformedPayloadErrors(t *testing.T) {
	good := strictRow{id: "good", s: "ok:s", ps: "ok:ps", ss: []string{"ok:a", "ok:b", "ok:c"}, smap: [][2]string{{"k1", "ok:v1"}, {"k2", "ok:v2"}}, holder: "ok:h", pholder: "ok:ph"}
	bad := strictRow{id: "bad", s: "bad-s", ps: "bad-ps", ss: []string{"ok:x", "bad-y", "ok:z"}, smap: [][2]string{{"k1", "bad-v1"}, {"k2", "ok:v2"}}, holder: "bad-h", pholder: "bad-ph"}
	onlyPointerBad := strictRow{id: "ps-only", s: "ok:s", ps: "bad-ps", ss: []string{}, smap: [][2]string{}, holder: "ok:h", pholder: "ok:ph"}
	rec := strictBatch(t, good, bad, onlyPointerBad)

	reader, err := NewStrictRecordArrowReader(rec)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}

	var got StrictRecord
	reader.LoadRow(0, &got)
	if errs := reader.Errors(); len(errs) != 0 {
		t.Fatalf("good row produced errors: %v", errs)
	}
	wantGood := StrictRecord{
		ID:      "good",
		S:       external.Strict{V: "s"},
		PS:      &external.Strict{V: "ps"},
		Ss:      []external.Strict{{V: "a"}, {V: "b"}, {V: "c"}},
		SMap:    map[string]external.Strict{"k1": {V: "v1"}, "k2": {V: "v2"}},
		Holder:  StrictHolder{S: external.Strict{V: "h"}},
		PHolder: &StrictHolder{S: external.Strict{V: "ph"}},
	}
	if diff := cmp.Diff(wantGood, got); diff != "" {
		t.Fatalf("good row (-want +got):\n%s", diff)
	}

	t.Run("errors carry row and field path", func(t *testing.T) {
		reader.ResetErrors()
		var fresh StrictRecord
		reader.LoadRow(1, &fresh)

		var paths []string
		for _, e := range reader.Errors() {
			if e.Row != 1 {
				t.Errorf("error %q reports row %d, want 1", e.Field, e.Row)
			}
			if !strings.Contains(e.Err.Error(), "lacks the ok: prefix") {
				t.Errorf("error %q wraps %q, want the UnmarshalText error", e.Field, e.Err)
			}
			paths = append(paths, e.Field)
		}
		// A struct field's path is prefixed with the struct. A list element
		// carries its index and a map value its key.
		if diff := cmp.Diff([]string{"S", "PS", "Ss[1]", "SMap[k1]", "Holder.S", "PHolder.S"}, paths); diff != "" {
			t.Errorf("error field paths (-want +got):\n%s", diff)
		}

		// Into a fresh struct the failed positions are zero. A failed map
		// value keeps its key, since the row did carry that key.
		wantFresh := StrictRecord{
			ID:      "bad",
			PS:      nil,
			Ss:      []external.Strict{{V: "x"}, {}, {V: "z"}},
			SMap:    map[string]external.Strict{"k1": {}, "k2": {V: "v2"}},
			PHolder: &StrictHolder{},
		}
		if diff := cmp.Diff(wantFresh, fresh); diff != "" {
			t.Errorf("malformed row into a fresh struct (-want +got):\n%s", diff)
		}
	})

	t.Run("failed list element is zeroed in a reused struct", func(t *testing.T) {
		// On an unmarshal error inside a list the reader records the error
		// and writes the zero value, so a struct reused across rows cannot
		// keep the previous row's element there.
		reader.ResetErrors()
		var reused StrictRecord
		reader.LoadRow(0, &reused)
		reader.LoadRow(1, &reused)
		if reused.Ss[1] != (external.Strict{}) {
			t.Fatalf("Ss[1] = %+v after a failed unmarshal over a reused struct, want the zero value", reused.Ss[1])
		}
		if v, present := reused.SMap["k1"]; !present || v != (external.Strict{}) {
			t.Errorf("SMap[k1] = %+v (present %v) after a failed unmarshal, want the zero value under the key", v, present)
		}
	})

	t.Run("errors accumulate until reset", func(t *testing.T) {
		reader.ResetErrors()
		var out StrictRecord
		reader.LoadRow(1, &out)
		reader.LoadRow(2, &out)
		if got := len(reader.Errors()); got != 7 {
			t.Fatalf("got %d accumulated errors over two rows, want 7", got)
		}
		reader.ResetErrors()
		if got := len(reader.Errors()); got != 0 {
			t.Fatalf("got %d errors after reset, want 0", got)
		}
	})

	t.Run("reset leaves a held slice intact", func(t *testing.T) {
		// A slice obtained from Errors() before ResetErrors keeps its
		// contents when the next row adds errors.
		reader.ResetErrors()
		var out StrictRecord
		reader.LoadRow(1, &out)
		held := reader.Errors()
		if held[0].Field != "S" {
			t.Fatalf("first error is %q, want S", held[0].Field)
		}
		reader.ResetErrors()
		reader.LoadRow(2, &out)
		if held[0].Field != "S" {
			t.Fatalf("held[0].Field = %q after reset and reload, want the held slice untouched", held[0].Field)
		}
		if errs := reader.Errors(); len(errs) != 1 || errs[0].Field != "PS" {
			t.Fatalf("errors after reload = %v, want one for PS", errs)
		}
	})
}
