package conformance

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// TestSkippedRecordKeepsOrdinaryFields checks that the fields around the
// skipped ones still round-trip, and that the skipped ones come back zero.
// TestSkippedShapesWarnPerField covers what the generator said about each.
func TestSkippedRecordKeepsOrdinaryFields(t *testing.T) {
	if diff := cmp.Diff([]string{"Keep", "Keep2"}, fieldNames(NewSkippedRecordSchema())); diff != "" {
		t.Fatalf("schema columns (-want +got):\n%s", diff)
	}
	in := &SkippedRecord{
		Keep:     "kept",
		Callback: func() {},
		Ch:       make(chan int),
		Addr:     0xABCD,
		C128:     complex(1, 2),
		Any:      "anything",
		Iface:    42,
		Shape:    nil,
		Stringer: fmt.Stringer(nil),
		Err:      fmt.Errorf("dropped"),
		Anon:     struct{ X int64 }{X: 1},
		AnyList:  []any{1, "two"},
		AnyMap:   map[string]any{"k": true},
		Keep2:    2,
	}
	rec := encode(t, NewSkippedRecordArrowWriter(newPool(t)), []*SkippedRecord{in})
	reader, err := NewSkippedRecordArrowReader(ipcRoundTrip(t, rec))
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	var got SkippedRecord
	reader.LoadRow(0, &got)
	if got.Keep != "kept" || got.Keep2 != 2 {
		t.Errorf("kept fields = %q %d, want kept 2", got.Keep, got.Keep2)
	}
	if got.Callback != nil || got.Ch != nil || got.Addr != 0 || got.C128 != 0 || got.Any != nil || got.Iface != nil || got.Err != nil || got.Anon.X != 0 || got.AnyList != nil || got.AnyMap != nil {
		t.Errorf("a skipped field came back set: %+v", got)
	}
}
