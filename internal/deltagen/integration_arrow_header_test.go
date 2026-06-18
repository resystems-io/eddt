package deltagen

// integration_arrow_header_test.go verifies R-DG-052 (Arrow columnar round-trip)
// for the full runtime.Header envelope embedded in a generated Delta — the case
// the shape-organised subtests in integration_arrow_test.go leave unexercised
// (they populate only Header.ChainID and Header.Sequence).
//
// In particular it proves the Provenance shape (R-CL-004) and its append-only
// accumulation (R-DG-032, R-CL-018) survive the projection: Provenance is a list
// of structs each carrying a map (Metadata) and a nested list of structs (Gaps),
// the deepest composite the projection must carry. A schema-presence guard runs
// before the value assertions so that arrow-writer-gen's silent-skip path (a
// dropped column → a warning, not an error) fails loudly rather than passing on
// an absent column.
//
// Test matrix:
//
//	TestIntegration_ArrowRoundTrip/HeaderProvenance   (R-DG-052, R-CL-004)

import "testing"

// arrowHeaderRoundTripCheck runs the R-DG-052 ARHeader pipeline: delta-gen +
// arrow gens + Arrow round-trip + DuckDB Parquet verification of the full Header
// envelope (EntityID, anchor pointers, bitemporal times, Provenance).
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

	// Row 0: rich Header — PreviousChainID set; two Provenance entries, each with
	// a non-empty Metadata map and a non-empty Gaps slice.
	row0 := ARHeaderDelta{
		Header: eddt.Header{
			EntityID:        eid,
			ChainID:         "chain-A",
			PreviousChainID: strptr("chain-prev"),
			Sequence:        0,
			EffectiveAt:     eff0,
			PublishedAt:     pub0,
			Provenance: []eddt.Provenance{
				{
					PublishedAt: pub0,
					ValidUntil:  timeptr(valid0),
					Solution:    "sol-1",
					Component:   "comp-1",
					Instance:    "inst-1",
					Metadata:    map[string]string{"k1": "v1", "k2": "v2"},
					Gaps:        []eddt.SequenceRange{{Start: 2, End: 4}, {Start: 9, End: 9}},
				},
				{
					PublishedAt: pub0,
					Solution:    "sol-2",
					Component:   "comp-2",
					Instance:    "inst-2",
					Metadata:    map[string]string{"x": "y"},
					Gaps:        []eddt.SequenceRange{{Start: 100, End: 200}},
				},
			},
		},
		SetLabel: strptr("label-0"),
	}

	// Row 1: terminator-ish — NextChainID + Closed set, empty Provenance.
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
		"Sequence", "EffectiveAt", "PublishedAt", "Provenance",
	} {
		if len(schema.FieldIndices(name)) == 0 {
			t.Fatalf("schema missing envelope column %q (arrow-writer-gen may have skipped it)", name)
		}
	}
	// Provenance must be List<Struct{ ... Metadata: Map, Gaps: List<Struct> }>.
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
	gapsf, ok := provStruct.FieldByName("Gaps")
	if !ok {
		t.Fatalf("Provenance struct missing Gaps field")
	}
	if _, ok := gapsf.Type.(*arrow.ListType); !ok {
		t.Fatalf("Provenance.Gaps: expected ListType, got %T", gapsf.Type)
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
	if len(p0.Gaps) != 2 || p0.Gaps[0] != (eddt.SequenceRange{Start: 2, End: 4}) || p0.Gaps[1] != (eddt.SequenceRange{Start: 9, End: 9}) {
		t.Errorf("row0 prov[0] Gaps: %v", p0.Gaps)
	}
	p1 := got0.Provenance[1]
	if p1.ValidUntil != nil {
		t.Errorf("row0 prov[1] ValidUntil: want nil, got %v", *p1.ValidUntil)
	}
	if len(p1.Metadata) != 1 || p1.Metadata["x"] != "y" {
		t.Errorf("row0 prov[1] Metadata: %v", p1.Metadata)
	}
	if len(p1.Gaps) != 1 || p1.Gaps[0] != (eddt.SequenceRange{Start: 100, End: 200}) {
		t.Errorf("row0 prov[1] Gaps: %v", p1.Gaps)
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

	// row0 Provenance[1] nested fields (DuckDB lists are 1-indexed): Solution,
	// Metadata['k1'], Metadata cardinality, Gaps length and Gaps[1].Start/End.
	var sol, md1 string
	var mdCard, gapsLen int64
	var gapStart, gapEnd uint64
	if err := db.QueryRow(fmt.Sprintf(
		"SELECT Provenance[1].Solution, Provenance[1].Metadata['k1'], cardinality(Provenance[1].Metadata), "+
			"len(Provenance[1].Gaps), Provenance[1].Gaps[1].Start, Provenance[1].Gaps[1].End "+
			"FROM read_parquet('%s') WHERE Sequence = 0", parquetPath,
	)).Scan(&sol, &md1, &mdCard, &gapsLen, &gapStart, &gapEnd); err != nil {
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
	if gapsLen != 2 {
		t.Errorf("duckdb row0 prov[1].Gaps len: got %d want 2", gapsLen)
	}
	if gapStart != 2 || gapEnd != 4 {
		t.Errorf("duckdb row0 prov[1].Gaps[1]: got {%d,%d} want {2,4}", gapStart, gapEnd)
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
