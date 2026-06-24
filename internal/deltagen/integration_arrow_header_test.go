package deltagen

// integration_arrow_header_test.go verifies R-DG-052 (Arrow columnar round-trip)
// for the full runtime.Header envelope embedded in a generated Delta — the case
// the shape-organised subtests in integration_arrow_test.go leave unexercised
// (they populate only Header.ChainID and Header.Sequence).
//
// In particular it proves the Header's two orthogonal envelope axes survive the
// projection:
//   - Provenance (R-CL-004, R-CL-018): an append-only list of Origin lineage
//     structs, each carrying a map (Metadata) — a list-of-struct-with-map. The
//     provenance axis is gaps-free.
//   - Quality (R-CL-036): a struct carrying a nested list of SequenceRange
//     structs (Gaps) — own-chain completeness, a struct-with-nested-list-of-struct.
//
// Between them they exercise the deepest composites the projection must carry. A
// schema-presence guard runs before the value assertions so that
// arrow-writer-gen's silent-skip path (a dropped column → a warning, not an
// error) fails loudly rather than passing on an absent column; it also asserts
// the de-conflation in the schema — Gaps live on Quality, never on Provenance.
//
// Test matrix:
//
//	TestIntegration_ArrowRoundTrip/HeaderProvenance   (R-DG-052, R-CL-004, R-CL-036)

import "testing"

// arrowHeaderRoundTripCheck runs the R-DG-052 ARHeader pipeline: delta-gen +
// arrow gens + Arrow round-trip + DuckDB Parquet verification of the full Header
// envelope (EntityID, anchor pointers, bitemporal times, Provenance, Quality).
func arrowHeaderRoundTripCheck(t *testing.T) {
	t.Helper()
	runArrowPipeline(t, arrowPipelineOpts{
		fixtures:      []string{"snapshot_header.go"},
		deltaTarget:   "ARHeader",
		writerTargets: []string{"ARHeader", "ARHeaderDelta"},
		innerTestFile: "roundtrip_header_test.go",
		innerTestSrc:  arrowHeaderInnerTest,
		runPattern:    "TestARHeaderDeltaRoundTrip",
		withDuckDB:    true,
	})
}

const arrowHeaderInnerTest = `package arrowroundtrip

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	_ "github.com/duckdb/duckdb-go/v2"
	eddt "go.resystems.io/eddt/runtime"
)

func strptr(s string) *string        { return &s }
func timeptr(t time.Time) *time.Time { return &t }

func TestARHeaderDeltaRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	eff0 := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	pub0 := time.Date(2026, 1, 2, 3, 4, 6, 0, time.UTC)
	valid0 := time.Date(2026, 1, 2, 4, 0, 0, 0, time.UTC)
	eff1 := time.Date(2026, 1, 2, 5, 0, 0, 0, time.UTC)
	pub1 := time.Date(2026, 1, 2, 5, 0, 1, 0, time.UTC)
	closed1 := time.Date(2026, 1, 2, 6, 0, 0, 0, time.UTC)

	var eid eddt.EntityID
	for i := range eid {
		eid[i] = byte(i + 1)
	}

	// Row 0: rich Header exercising both envelope axes —
	//   - Provenance (lineage): two Origin entries, each with a non-empty
	//     Metadata map. The provenance axis carries no completeness data.
	//   - Quality (completeness): a Gaps list with two SequenceRange entries —
	//     this chain's own missing positions, stamped at materialise time.
	// PreviousChainID is set (birth-with-predecessor).
	row0 := ARHeaderDelta{
		Header: eddt.Header{
			EntityID:        eid,
			ChainID:         "chain-A",
			PreviousChainID: strptr("chain-prev"),
			Sequence:        0,
			EffectiveAt:     eff0,
			PublishedAt:     pub0,
			Provenance: []eddt.Origin{
				{
					PublishedAt: pub0,
					ValidUntil:  timeptr(valid0),
					Solution:    "sol-1",
					Component:   "comp-1",
					Instance:    "inst-1",
					Metadata:    map[string]string{"k1": "v1", "k2": "v2"},
				},
				{
					PublishedAt: pub0,
					Solution:    "sol-2",
					Component:   "comp-2",
					Instance:    "inst-2",
					Metadata:    map[string]string{"x": "y"},
				},
			},
			Quality: eddt.Quality{
				Gaps: []eddt.SequenceRange{{Start: 2, End: 4}, {Start: 9, End: 9}},
			},
		},
		SetLabel: strptr("label-0"),
	}

	// Row 1: terminator-ish — NextChainID + Closed set, empty Provenance, and a
	// zero-value Quality (no gaps: a complete materialised state).
	row1 := ARHeaderDelta{
		Header: eddt.Header{
			EntityID:    eid,
			ChainID:     "chain-A",
			NextChainID: strptr("chain-next"),
			Closed:      timeptr(closed1),
			Sequence:    1,
			EffectiveAt: eff1,
			PublishedAt: pub1,
		},
		SetLabel: strptr("label-1"),
	}

	writer := NewARHeaderDeltaArrowWriter(pool)
	defer writer.Release()
	writer.Append(&row0)
	writer.Append(&row1)
	rec := writer.NewRecordBatch()
	defer rec.Release()

	if rec.NumRows() != 2 {
		t.Fatalf("expected 2 rows, got %d", rec.NumRows())
	}

	// --- Schema-presence guard (defeat arrow-writer-gen's silent-skip) ---
	schema := rec.Schema()
	for _, name := range []string{
		"EntityID", "ChainID", "PreviousChainID", "NextChainID", "Closed",
		"Sequence", "EffectiveAt", "PublishedAt", "Provenance", "Quality",
	} {
		if len(schema.FieldIndices(name)) == 0 {
			t.Fatalf("schema missing envelope column %q (arrow-writer-gen may have skipped it)", name)
		}
	}
	// Provenance (lineage axis) must be List<Struct{ ... Metadata: Map }> and must
	// NOT carry Gaps — completeness moved to the Quality axis (R-CL-004, R-CL-036).
	provField := schema.Field(schema.FieldIndices("Provenance")[0])
	provList, ok := provField.Type.(*arrow.ListType)
	if !ok {
		t.Fatalf("Provenance: expected ListType, got %T", provField.Type)
	}
	provStruct, ok := provList.Elem().(*arrow.StructType)
	if !ok {
		t.Fatalf("Provenance element: expected StructType, got %T", provList.Elem())
	}
	mdf, ok := provStruct.FieldByName("Metadata")
	if !ok {
		t.Fatalf("Provenance struct missing Metadata field")
	}
	if _, ok := mdf.Type.(*arrow.MapType); !ok {
		t.Fatalf("Provenance.Metadata: expected MapType, got %T", mdf.Type)
	}
	if _, ok := provStruct.FieldByName("Gaps"); ok {
		t.Fatalf("Provenance struct must not carry Gaps (completeness lives on Quality)")
	}

	// Quality (completeness axis) must be Struct{ Gaps: List<Struct{Start,End}> }.
	qField := schema.Field(schema.FieldIndices("Quality")[0])
	qStruct, ok := qField.Type.(*arrow.StructType)
	if !ok {
		t.Fatalf("Quality: expected StructType, got %T", qField.Type)
	}
	qGapsf, ok := qStruct.FieldByName("Gaps")
	if !ok {
		t.Fatalf("Quality struct missing Gaps field")
	}
	if _, ok := qGapsf.Type.(*arrow.ListType); !ok {
		t.Fatalf("Quality.Gaps: expected ListType, got %T", qGapsf.Type)
	}

	// --- Arrow reader round-trip ---
	reader, err := NewARHeaderDeltaArrowReader(rec)
	if err != nil {
		t.Fatalf("NewARHeaderDeltaArrowReader: %v", err)
	}
	var got0, got1 ARHeaderDelta
	reader.LoadRow(0, &got0)
	reader.LoadRow(1, &got1)

	if got0.EntityID != eid {
		t.Errorf("row0 EntityID: got %v want %v", got0.EntityID, eid)
	}
	if got0.ChainID != "chain-A" || got0.Sequence != 0 {
		t.Errorf("row0 ChainID/Sequence: got %q/%d", got0.ChainID, got0.Sequence)
	}
	if !got0.EffectiveAt.Equal(eff0) || !got0.PublishedAt.Equal(pub0) {
		t.Errorf("row0 timestamps: EffectiveAt=%v PublishedAt=%v", got0.EffectiveAt, got0.PublishedAt)
	}
	if got0.PreviousChainID == nil || *got0.PreviousChainID != "chain-prev" {
		t.Errorf("row0 PreviousChainID: %v", got0.PreviousChainID)
	}
	if got0.NextChainID != nil {
		t.Errorf("row0 NextChainID: want nil, got %q", *got0.NextChainID)
	}
	if got0.Closed != nil {
		t.Errorf("row0 Closed: want nil, got %v", *got0.Closed)
	}

	// Provenance (lineage axis): two Origin entries, gaps-free.
	if len(got0.Provenance) != 2 {
		t.Fatalf("row0 Provenance len: got %d want 2", len(got0.Provenance))
	}
	p0 := got0.Provenance[0]
	if p0.Solution != "sol-1" || p0.Component != "comp-1" || p0.Instance != "inst-1" {
		t.Errorf("row0 prov[0] ids: %q/%q/%q", p0.Solution, p0.Component, p0.Instance)
	}
	if p0.ValidUntil == nil || !p0.ValidUntil.Equal(valid0) {
		t.Errorf("row0 prov[0] ValidUntil: %v", p0.ValidUntil)
	}
	if len(p0.Metadata) != 2 || p0.Metadata["k1"] != "v1" || p0.Metadata["k2"] != "v2" {
		t.Errorf("row0 prov[0] Metadata: %v", p0.Metadata)
	}
	p1 := got0.Provenance[1]
	if p1.ValidUntil != nil {
		t.Errorf("row0 prov[1] ValidUntil: want nil, got %v", *p1.ValidUntil)
	}
	if len(p1.Metadata) != 1 || p1.Metadata["x"] != "y" {
		t.Errorf("row0 prov[1] Metadata: %v", p1.Metadata)
	}

	// Quality (completeness axis): own-chain Gaps round-trip.
	if len(got0.Quality.Gaps) != 2 ||
		got0.Quality.Gaps[0] != (eddt.SequenceRange{Start: 2, End: 4}) ||
		got0.Quality.Gaps[1] != (eddt.SequenceRange{Start: 9, End: 9}) {
		t.Errorf("row0 Quality.Gaps: %v", got0.Quality.Gaps)
	}

	if got1.PreviousChainID != nil {
		t.Errorf("row1 PreviousChainID: want nil, got %q", *got1.PreviousChainID)
	}
	if got1.NextChainID == nil || *got1.NextChainID != "chain-next" {
		t.Errorf("row1 NextChainID: %v", got1.NextChainID)
	}
	if got1.Closed == nil || !got1.Closed.Equal(closed1) {
		t.Errorf("row1 Closed: %v", got1.Closed)
	}
	if len(got1.Provenance) != 0 {
		t.Errorf("row1 Provenance: want empty, got %v", got1.Provenance)
	}
	if len(got1.Quality.Gaps) != 0 {
		t.Errorf("row1 Quality.Gaps: want empty, got %v", got1.Quality.Gaps)
	}

	// --- DuckDB Parquet verification ---
	parquetPath := filepath.Join(t.TempDir(), "header.parquet")
	f, err := os.Create(parquetPath)
	if err != nil {
		t.Fatalf("create parquet: %v", err)
	}
	pw, err := pqarrow.NewFileWriter(rec.Schema(), f, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatalf("pqarrow.NewFileWriter: %v", err)
	}
	if err := pw.Write(rec); err != nil {
		t.Fatalf("pqWriter.Write: %v", err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("pqWriter.Close: %v", err)
	}
	f.Close()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("sql.Open duckdb: %v", err)
	}
	defer db.Close()

	// Provenance length per row (row1 empty → NULL or 0, the E-04 nil/empty duality).
	var prov0Len int64
	if err := db.QueryRow(fmt.Sprintf(
		"SELECT len(Provenance) FROM read_parquet('%s') WHERE Sequence = 0", parquetPath,
	)).Scan(&prov0Len); err != nil {
		t.Fatalf("duckdb prov0 len: %v", err)
	}
	if prov0Len != 2 {
		t.Errorf("duckdb row0 len(Provenance): got %d want 2", prov0Len)
	}
	var prov1Empty bool
	if err := db.QueryRow(fmt.Sprintf(
		"SELECT Provenance IS NULL OR len(Provenance) = 0 FROM read_parquet('%s') WHERE Sequence = 1", parquetPath,
	)).Scan(&prov1Empty); err != nil {
		t.Fatalf("duckdb prov1 empty: %v", err)
	}
	if !prov1Empty {
		t.Errorf("duckdb row1 Provenance: want null/empty")
	}

	// row0 Provenance[1] nested lineage fields (DuckDB lists are 1-indexed):
	// Solution, Metadata['k1'], Metadata cardinality. No Gaps on the lineage axis.
	var sol, md1 string
	var mdCard int64
	if err := db.QueryRow(fmt.Sprintf(
		"SELECT Provenance[1].Solution, Provenance[1].Metadata['k1'], cardinality(Provenance[1].Metadata) "+
			"FROM read_parquet('%s') WHERE Sequence = 0", parquetPath,
	)).Scan(&sol, &md1, &mdCard); err != nil {
		t.Fatalf("duckdb row0 nested Provenance: %v", err)
	}
	if sol != "sol-1" {
		t.Errorf("duckdb row0 prov[1].Solution: got %q want sol-1", sol)
	}
	if md1 != "v1" {
		t.Errorf("duckdb row0 prov[1].Metadata['k1']: got %q want v1", md1)
	}
	if mdCard != 2 {
		t.Errorf("duckdb row0 prov[1].Metadata cardinality: got %d want 2", mdCard)
	}

	// row0 Quality.Gaps nested completeness fields (DuckDB lists are 1-indexed):
	// the own-chain Gaps list and its first SequenceRange.
	var qGapsLen int64
	var qGapStart, qGapEnd uint64
	if err := db.QueryRow(fmt.Sprintf(
		"SELECT len(Quality.Gaps), Quality.Gaps[1].Start, Quality.Gaps[1].End "+
			"FROM read_parquet('%s') WHERE Sequence = 0", parquetPath,
	)).Scan(&qGapsLen, &qGapStart, &qGapEnd); err != nil {
		t.Fatalf("duckdb row0 Quality.Gaps: %v", err)
	}
	if qGapsLen != 2 {
		t.Errorf("duckdb row0 len(Quality.Gaps): got %d want 2", qGapsLen)
	}
	if qGapStart != 2 || qGapEnd != 4 {
		t.Errorf("duckdb row0 Quality.Gaps[1]: got {%d,%d} want {2,4}", qGapStart, qGapEnd)
	}
	// row1 Quality has no gaps (complete state).
	var q1GapsEmpty bool
	if err := db.QueryRow(fmt.Sprintf(
		"SELECT Quality.Gaps IS NULL OR len(Quality.Gaps) = 0 FROM read_parquet('%s') WHERE Sequence = 1", parquetPath,
	)).Scan(&q1GapsEmpty); err != nil {
		t.Fatalf("duckdb row1 Quality.Gaps: %v", err)
	}
	if !q1GapsEmpty {
		t.Errorf("duckdb row1 Quality.Gaps: want null/empty")
	}

	// Anchor-pointer nullability per row.
	var prevNull0, nextNull0, closedNull0 bool
	if err := db.QueryRow(fmt.Sprintf(
		"SELECT PreviousChainID IS NULL, NextChainID IS NULL, Closed IS NULL FROM read_parquet('%s') WHERE Sequence = 0", parquetPath,
	)).Scan(&prevNull0, &nextNull0, &closedNull0); err != nil {
		t.Fatalf("duckdb row0 anchors: %v", err)
	}
	if prevNull0 || !nextNull0 || !closedNull0 {
		t.Errorf("duckdb row0 anchors: prevNull=%v nextNull=%v closedNull=%v", prevNull0, nextNull0, closedNull0)
	}
	var prevNull1, nextNull1, closedNull1 bool
	if err := db.QueryRow(fmt.Sprintf(
		"SELECT PreviousChainID IS NULL, NextChainID IS NULL, Closed IS NULL FROM read_parquet('%s') WHERE Sequence = 1", parquetPath,
	)).Scan(&prevNull1, &nextNull1, &closedNull1); err != nil {
		t.Fatalf("duckdb row1 anchors: %v", err)
	}
	if !prevNull1 || nextNull1 || closedNull1 {
		t.Errorf("duckdb row1 anchors: prevNull=%v nextNull=%v closedNull=%v", prevNull1, nextNull1, closedNull1)
	}

	// EntityID column present and non-null in Parquet (byte-exactness is covered
	// by the Arrow reader assertion above).
	var eidNull bool
	if err := db.QueryRow(fmt.Sprintf(
		"SELECT EntityID IS NULL FROM read_parquet('%s') WHERE Sequence = 0", parquetPath,
	)).Scan(&eidNull); err != nil {
		t.Fatalf("duckdb EntityID null-check: %v", err)
	}
	if eidNull {
		t.Errorf("duckdb row0 EntityID: want non-null")
	}
}
`
