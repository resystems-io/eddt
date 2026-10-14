package conformance

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/google/go-cmp/cmp"
)

func TestValueEmbedFlattens(t *testing.T) {
	if diff := cmp.Diff([]string{"BaseField", "Shared", "Extra"}, fieldNames(NewValueEmbedRecordSchema())); diff != "" {
		t.Fatalf("schema columns (-want +got):\n%s", diff)
	}
	in := []*ValueEmbedRecord{{EmbedBase: EmbedBase{BaseField: "flattened", Shared: 3}, Extra: 5}, {}}
	roundTripBoth(t, NewValueEmbedRecordArrowWriter(newPool(t)), func(rec arrow.RecordBatch) (loader[ValueEmbedRecord], error) {
		return NewValueEmbedRecordArrowReader(rec)
	}, in, derefs(in), pqarrow.DefaultWriterProps())
}

// TestShadowedEmbeddedField checks the generator follows Go's promotion
// rule: the outer Shared wins, the embedded Shared is not a column, and so
// it comes back zero.
func TestShadowedEmbeddedField(t *testing.T) {
	schema := NewShadowEmbedRecordSchema()
	if diff := cmp.Diff([]string{"BaseField", "Shared"}, fieldNames(schema)); diff != "" {
		t.Fatalf("schema columns (-want +got):\n%s", diff)
	}
	if got := schema.Field(1).Type; !arrow.TypeEqual(got, arrow.BinaryTypes.String) {
		t.Fatalf("Shared column type %v, want the outer field's String", got)
	}
	in := []*ShadowEmbedRecord{{EmbedBase: EmbedBase{BaseField: "b", Shared: 9}, Shared: "outer"}}
	want := []ShadowEmbedRecord{{EmbedBase: EmbedBase{BaseField: "b"}, Shared: "outer"}}
	roundTripBoth(t, NewShadowEmbedRecordArrowWriter(newPool(t)), func(rec arrow.RecordBatch) (loader[ShadowEmbedRecord], error) {
		return NewShadowEmbedRecordArrowReader(rec)
	}, in, want, pqarrow.DefaultWriterProps())
}

// TestNestedEmbeddingDropsInnerLevel documents a silent loss: the generator
// flattens one level of embedding only. Fields promoted through two levels
// are not columns, and the generator prints no warning about them
// (TestGeneratorWarnings covers the stderr side).
func TestNestedEmbeddingDropsInnerLevel(t *testing.T) {
	if diff := cmp.Diff([]string{"MidField", "Top"}, fieldNames(NewNestedEmbedRecordSchema())); diff != "" {
		t.Fatalf("schema columns (-want +got):\n%s", diff)
	}
	in := []*NestedEmbedRecord{{EmbedMid: EmbedMid{EmbedBase: EmbedBase{BaseField: "lost", Shared: 1}, MidField: "mid"}, Top: "top"}}
	want := []NestedEmbedRecord{{EmbedMid: EmbedMid{MidField: "mid"}, Top: "top"}}
	roundTripBoth(t, NewNestedEmbedRecordArrowWriter(newPool(t)), func(rec arrow.RecordBatch) (loader[NestedEmbedRecord], error) {
		return NewNestedEmbedRecordArrowReader(rec)
	}, in, want, pqarrow.DefaultWriterProps())
}

func TestPointerEmbedDropped(t *testing.T) {
	if diff := cmp.Diff([]string{"Extra"}, fieldNames(NewPointerEmbedRecordSchema())); diff != "" {
		t.Fatalf("schema columns (-want +got):\n%s", diff)
	}
	in := []*PointerEmbedRecord{{EmbedBase: &EmbedBase{BaseField: "lost"}, Extra: 9}}
	want := []PointerEmbedRecord{{Extra: 9}}
	roundTripBoth(t, NewPointerEmbedRecordArrowWriter(newPool(t)), func(rec arrow.RecordBatch) (loader[PointerEmbedRecord], error) {
		return NewPointerEmbedRecordArrowReader(rec)
	}, in, want, pqarrow.DefaultWriterProps())
}

func TestExternalEmbedDropped(t *testing.T) {
	if diff := cmp.Diff([]string{"Name"}, fieldNames(NewExternalEmbedRecordSchema())); diff != "" {
		t.Fatalf("schema columns (-want +got):\n%s", diff)
	}
	in := []*ExternalEmbedRecord{{Time: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Name: "kept"}}
	want := []ExternalEmbedRecord{{Name: "kept"}}
	roundTripBoth(t, NewExternalEmbedRecordArrowWriter(newPool(t)), func(rec arrow.RecordBatch) (loader[ExternalEmbedRecord], error) {
		return NewExternalEmbedRecordArrowReader(rec)
	}, in, want, pqarrow.DefaultWriterProps())
}

func TestAmbiguousEmbedDropsOnlyTheAmbiguousField(t *testing.T) {
	if diff := cmp.Diff([]string{"A", "B"}, fieldNames(NewAmbiguousEmbedRecordSchema())); diff != "" {
		t.Fatalf("schema columns (-want +got):\n%s", diff)
	}
	in := []*AmbiguousEmbedRecord{{EmbedA: EmbedA{A: 1, Dup: "a"}, EmbedB: EmbedB{B: 2, Dup: "b"}}}
	want := []AmbiguousEmbedRecord{{EmbedA: EmbedA{A: 1}, EmbedB: EmbedB{B: 2}}}
	roundTripBoth(t, NewAmbiguousEmbedRecordArrowWriter(newPool(t)), func(rec arrow.RecordBatch) (loader[AmbiguousEmbedRecord], error) {
		return NewAmbiguousEmbedRecordArrowReader(rec)
	}, in, want, pqarrow.DefaultWriterProps())
}
