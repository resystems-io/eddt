package deltagen

// integration_arrow_test.go implements C-06: cross-generator integration
// round-trip — delta-gen × arrow-writer-gen × arrow-reader-gen.
//
// C-06 verifies the cross-subsystem contract: the type shapes delta-gen emits
// are compatible with the arrow generators' supported field-shape vocabulary,
// end-to-end.  All three generators are invoked programmatically via their Go
// APIs; no CLI subprocesses.  The replace directive in the temp module's go.mod
// ensures the local in-tree arrow-gen code is used (not a remote version).
//
// Fixture: testdata/arrow_roundtrip/snapshot.go — defines ARSnapshot and ARMeta,
// covering ShapeScalar and ShapeStructValue delta fields.
//
// Test matrix:
//
//	TestIntegration_ArrowRoundTrip/ScalarAndStruct    PASS  (C-06)
//	TestIntegration_ArrowRoundTrip/ShapePointer       PASS  (C-08)
//	TestIntegration_ArrowRoundTrip/ShapeSliceAtomic   PASS  (C-08)
//	TestIntegration_ArrowRoundTrip/ShapeMapAtomic     PASS  (C-08)
//	TestIntegration_ArrowRoundTrip/NestedClearable    PASS  (CL-09)

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"go.resystems.io/eddt/internal/arrow/arrowtest"
	readergen "go.resystems.io/eddt/internal/arrow/reader-gen"
	writergen "go.resystems.io/eddt/internal/arrow/writer-gen"
)

// TestIntegration_ArrowRoundTrip wires delta-gen, arrow-writer-gen, and
// arrow-reader-gen together in an isolated temp module and verifies that a
// synthetic ARSnapshotDelta value survives an Arrow encode/decode cycle.
func TestIntegration_ArrowRoundTrip(t *testing.T) {
	t.Run("ScalarAndStruct", func(t *testing.T) {
		arrowRoundTripCheck(t)
	})

	t.Run("ShapePointer", func(t *testing.T) {
		arrowExtendedRoundTripCheck(t)
	})

	t.Run("ShapeSliceAtomic", func(t *testing.T) {
		arrowExtendedRoundTripCheck(t)
	})

	t.Run("ShapeMapAtomic", func(t *testing.T) {
		arrowExtendedRoundTripCheck(t)
	})

	t.Run("NestedClearable", func(t *testing.T) {
		arrowClearableRoundTripCheck(t)
	})
}

// arrowPipelineOpts configures a single cross-generator (delta-gen +
// arrow-writer-gen + arrow-reader-gen) pipeline run.
type arrowPipelineOpts struct {
	fixtures      []string // files to copy from testdata/arrow_roundtrip/
	deltaTarget   string   // struct name for delta-gen TargetStructs
	writerTargets []string // struct names for arrow-writer-gen / arrow-reader-gen
	innerTestFile string   // filename for the injected inner test
	innerTestSrc  string   // source of the injected inner test
	runPattern    string   // -run pattern for go test
	withDuckDB    bool     // fetch the DuckDB dep before running the inner test
}

// runArrowPipeline sets up an isolated temp module, runs delta-gen followed by
// arrow-writer-gen and arrow-reader-gen, writes the injected inner test, and
// executes go test.  All three arrow subtests share this implementation and
// differ only in their arrowPipelineOpts.
//
// GOFLAGS=-mod=mod is set so that go/packages accepts the minimal go.mod.
// go.resystems.io/eddt/runtime is passed as a second input package to the arrow
// generators so that resolveEmbeddedFields can find the runtime.Header AST.
func runArrowPipeline(t *testing.T, opts arrowPipelineOpts) {
	t.Helper()
	t.Setenv("GOFLAGS", "-mod=mod")

	tmpDir := t.TempDir()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	moduleRoot := filepath.Clean(filepath.Join(wd, "..", ".."))

	modContent := fmt.Sprintf(
		"module %s\n\ngo 1.25.0\n\nrequire go.resystems.io/eddt v0.0.0\n\nreplace go.resystems.io/eddt => %s\n",
		"arrowroundtrip", moduleRoot,
	)
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte(modContent), 0644); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}
	goSum, err := os.ReadFile(filepath.Join(moduleRoot, "go.sum"))
	if err != nil {
		t.Fatalf("read go.sum: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "go.sum"), goSum, 0644); err != nil {
		t.Fatalf("write go.sum: %v", err)
	}

	for _, name := range opts.fixtures {
		src, err := os.ReadFile(filepath.Join("testdata", "arrow_roundtrip", name))
		if err != nil {
			t.Fatalf("read fixture %s: %v", name, err)
		}
		if err := os.WriteFile(filepath.Join(tmpDir, name), src, 0644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	deltaPath := filepath.Join(tmpDir, "delta.go")
	if err := New(Config{
		InputPkgs:     []string{tmpDir},
		TargetStructs: []string{opts.deltaTarget},
		OutPath:       deltaPath,
	}).Run(); err != nil {
		t.Fatalf("delta-gen Run(): %v", err)
	}
	assertGofmtClean(t, deltaPath)

	runBuildCmd(t, tmpDir, "go", "get", arrowtest.ArrowDep)

	arrowInputPkgs := []string{tmpDir, "go.resystems.io/eddt/runtime"}

	writerPath := filepath.Join(tmpDir, "arrow_writer.go")
	wg := writergen.NewGenerator(arrowInputPkgs, opts.writerTargets, writerPath, false, nil)
	if err := wg.Run(""); err != nil {
		t.Fatalf("arrow-writer-gen Run(): %v", err)
	}
	assertGofmtClean(t, writerPath)

	readerPath := filepath.Join(tmpDir, "arrow_reader.go")
	rg := readergen.NewGenerator(arrowInputPkgs, opts.writerTargets, readerPath, false, nil)
	if err := rg.Run(""); err != nil {
		t.Fatalf("arrow-reader-gen Run(): %v", err)
	}
	assertGofmtClean(t, readerPath)

	if opts.withDuckDB {
		runBuildCmd(t, tmpDir, "go", "get", arrowtest.DuckDBDep)
	}

	if err := os.WriteFile(filepath.Join(tmpDir, opts.innerTestFile), []byte(opts.innerTestSrc), 0644); err != nil {
		t.Fatalf("write %s: %v", opts.innerTestFile, err)
	}

	runBuildCmd(t, tmpDir, "go", "mod", "tidy")
	runBuildCmd(t, tmpDir, "go", "test", "-v", "-run", opts.runPattern, ".")
}

// arrowRoundTripCheck runs the C-06 ARSnapshot pipeline: delta-gen + arrow gens
// + Arrow round-trip inner test.
func arrowRoundTripCheck(t *testing.T) {
	t.Helper()
	runArrowPipeline(t, arrowPipelineOpts{
		fixtures:      []string{"snapshot.go", "snapshot_extended.go"},
		deltaTarget:   "ARSnapshot",
		writerTargets: []string{"ARSnapshot", "ARSnapshotDelta"},
		innerTestFile: "roundtrip_test.go",
		innerTestSrc:  arrowSnapshotInnerTest,
		runPattern:    "TestARSnapshotDeltaRoundTrip",
	})
}

const arrowSnapshotInnerTest = `package arrowroundtrip

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	eddt "go.resystems.io/eddt/runtime"
)

func TestARSnapshotDeltaRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	name := "Alice"
	score := 3.14
	meta := ARMeta{Region: "us-east", Version: 42}

	want := ARSnapshotDelta{
		Header: eddt.Header{
			ChainID:  "chain-001",
			Sequence: 7,
		},
		SetName:  &name,
		SetScore: &score,
		SetMeta:  &meta,
	}

	writer := NewARSnapshotDeltaArrowWriter(pool)
	defer writer.Release()
	writer.Append(&want)
	rec := writer.NewRecord()
	defer rec.Release()

	if rec.NumRows() != 1 {
		t.Fatalf("expected 1 row, got %d", rec.NumRows())
	}

	reader, err := NewARSnapshotDeltaArrowReader(rec)
	if err != nil {
		t.Fatalf("NewARSnapshotDeltaArrowReader: %v", err)
	}

	var got ARSnapshotDelta
	reader.LoadRow(0, &got)

	if got.Header.ChainID != want.Header.ChainID {
		t.Errorf("ChainID: got %q, want %q", got.Header.ChainID, want.Header.ChainID)
	}
	if got.Header.Sequence != want.Header.Sequence {
		t.Errorf("Sequence: got %d, want %d", got.Header.Sequence, want.Header.Sequence)
	}
	if got.SetName == nil || *got.SetName != *want.SetName {
		t.Errorf("SetName: got %v, want %v", got.SetName, want.SetName)
	}
	if got.SetScore == nil || *got.SetScore != *want.SetScore {
		t.Errorf("SetScore: got %v, want %v", got.SetScore, want.SetScore)
	}
	if got.SetMeta == nil || *got.SetMeta != *want.SetMeta {
		t.Errorf("SetMeta: got %v, want %v", got.SetMeta, want.SetMeta)
	}
}
`

// arrowExtendedRoundTripCheck runs the C-08 ARExtended pipeline: delta-gen +
// arrow gens + Arrow round-trip + DuckDB Parquet verification.
func arrowExtendedRoundTripCheck(t *testing.T) {
	t.Helper()
	runArrowPipeline(t, arrowPipelineOpts{
		fixtures:      []string{"snapshot.go", "snapshot_extended.go"},
		deltaTarget:   "ARExtended",
		writerTargets: []string{"ARExtended", "ARExtendedDelta"},
		innerTestFile: "roundtrip_extended_test.go",
		innerTestSrc:  arrowExtendedInnerTest,
		runPattern:    "TestARExtendedDeltaRoundTrip",
		withDuckDB:    true,
	})
}

const arrowExtendedInnerTest = `package arrowroundtrip

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	_ "github.com/duckdb/duckdb-go/v2"
	eddt "go.resystems.io/eddt/runtime"
)

func TestARExtendedDeltaRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewARExtendedDeltaArrowWriter(pool)
	defer writer.Release()

	// Row 0: all compound fields nil → Arrow null.
	row0 := ARExtendedDelta{Header: eddt.Header{ChainID: "c0", Sequence: 0}}

	// Row 1: all compound fields non-nil.
	val := int32(42)
	pval := &val
	tags := []string{"a", "b"}
	attrs := map[string]string{"k": "v"}
	row1 := ARExtendedDelta{
		Header:     eddt.Header{ChainID: "c1", Sequence: 1},
		SetPointer: &pval,
		SetTags:    &tags,
		SetAttrs:   &attrs,
	}

	writer.Append(&row0)
	writer.Append(&row1)
	rec := writer.NewRecord()
	defer rec.Release()

	if rec.NumRows() != 2 {
		t.Fatalf("expected 2 rows, got %d", rec.NumRows())
	}

	// --- Arrow reader round-trip ---
	reader, err := NewARExtendedDeltaArrowReader(rec)
	if err != nil {
		t.Fatalf("NewARExtendedDeltaArrowReader: %v", err)
	}

	var got0, got1 ARExtendedDelta
	reader.LoadRow(0, &got0)
	reader.LoadRow(1, &got1)

	if got0.SetPointer != nil {
		t.Errorf("row0 SetPointer: want nil, got non-nil")
	}
	if got0.SetTags != nil {
		t.Errorf("row0 SetTags: want nil, got non-nil")
	}
	if got0.SetAttrs != nil {
		t.Errorf("row0 SetAttrs: want nil, got non-nil")
	}
	if got1.SetPointer == nil || *got1.SetPointer == nil || **got1.SetPointer != 42 {
		t.Errorf("row1 SetPointer: want &&42, got %v", got1.SetPointer)
	}
	if got1.SetTags == nil || len(*got1.SetTags) != 2 ||
		(*got1.SetTags)[0] != "a" || (*got1.SetTags)[1] != "b" {
		t.Errorf("row1 SetTags: want &[a b], got %v", got1.SetTags)
	}
	if got1.SetAttrs == nil || len(*got1.SetAttrs) != 1 || (*got1.SetAttrs)["k"] != "v" {
		t.Errorf("row1 SetAttrs: want &{k:v}, got %v", got1.SetAttrs)
	}

	// --- DuckDB Parquet verification ---
	parquetPath := filepath.Join(t.TempDir(), "extended.parquet")
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

	// Row 0: all compound columns must be NULL.
	var ptrNull, tagsNull, attrsNull bool
	err = db.QueryRow(fmt.Sprintf(
		"SELECT SetPointer IS NULL, SetTags IS NULL, SetAttrs IS NULL FROM read_parquet('%s') LIMIT 1 OFFSET 0",
		parquetPath,
	)).Scan(&ptrNull, &tagsNull, &attrsNull)
	if err != nil {
		t.Fatalf("DuckDB null-row query: %v", err)
	}
	if !ptrNull || !tagsNull || !attrsNull {
		t.Errorf("row0: want all NULL, got SetPointer=%v SetTags=%v SetAttrs=%v", ptrNull, tagsNull, attrsNull)
	}

	// Row 1: non-null values — verify via scalar extraction.
	// Use len() for the list column and cardinality() for the map column.
	var ptrVal int32
	var tagsLen, attrsLen int64
	err = db.QueryRow(fmt.Sprintf(
		"SELECT SetPointer, len(SetTags), cardinality(SetAttrs) FROM read_parquet('%s') LIMIT 1 OFFSET 1",
		parquetPath,
	)).Scan(&ptrVal, &tagsLen, &attrsLen)
	if err != nil {
		t.Fatalf("DuckDB value-row query: %v", err)
	}
	if ptrVal != 42 {
		t.Errorf("row1 SetPointer (DuckDB): want 42, got %d", ptrVal)
	}
	if tagsLen != 2 {
		t.Errorf("row1 SetTags len (DuckDB): want 2, got %d", tagsLen)
	}
	if attrsLen != 1 {
		t.Errorf("row1 SetAttrs len (DuckDB): want 1, got %d", attrsLen)
	}
}
`

// arrowClearableRoundTripCheck runs the CL-09 ARClearable pipeline: delta-gen +
// arrow gens + Arrow round-trip + DuckDB Parquet verification for all three
// Op states across struct / map / slice clearable shapes.
func arrowClearableRoundTripCheck(t *testing.T) {
	t.Helper()
	runArrowPipeline(t, arrowPipelineOpts{
		fixtures:      []string{"snapshot.go", "snapshot_extended.go", "snapshot_clearable.go"},
		deltaTarget:   "ARClearable",
		writerTargets: []string{"ARClearable", "ARClearableDelta"},
		innerTestFile: "roundtrip_clearable_test.go",
		innerTestSrc:  arrowClearableInnerTest,
		runPattern:    "TestARClearableDeltaRoundTrip",
		withDuckDB:    true,
	})
}

const arrowClearableInnerTest = `package arrowroundtrip

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	_ "github.com/duckdb/duckdb-go/v2"
	eddt "go.resystems.io/eddt/runtime"
)

func TestARClearableDeltaRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewARClearableDeltaArrowWriter(pool)
	defer writer.Release()

	// Row 0: all envelopes at zero value → OpIgnore.
	row0 := ARClearableDelta{Header: eddt.Header{ChainID: "c", Sequence: 1}}

	// Row 1: all envelopes OpAssert with inner-delta payloads.
	street := "1 Main"
	row1 := ARClearableDelta{
		Header: eddt.Header{ChainID: "c", Sequence: 2},
		Location: eddt.FieldDelta[ARAddressDelta]{
			Op:    eddt.OpAssert,
			Value: ARAddressDelta{SetStreet: &street},
		},
		Tags: eddt.FieldDelta[TagsMapDelta]{
			Op:    eddt.OpAssert,
			Value: TagsMapDelta{UpdatedTags: map[string]string{"k": "v"}},
		},
		Groups: eddt.FieldDelta[GroupsSliceDelta]{
			Op:    eddt.OpAssert,
			Value: GroupsSliceDelta{AddedGroups: []string{"x"}},
		},
	}

	// Row 2: all envelopes OpRetract; Value sub-struct is zero.
	row2 := ARClearableDelta{
		Header:   eddt.Header{ChainID: "c", Sequence: 3},
		Location: eddt.FieldDelta[ARAddressDelta]{Op: eddt.OpRetract},
		Tags:     eddt.FieldDelta[TagsMapDelta]{Op: eddt.OpRetract},
		Groups:   eddt.FieldDelta[GroupsSliceDelta]{Op: eddt.OpRetract},
	}

	writer.Append(&row0)
	writer.Append(&row1)
	writer.Append(&row2)
	rec := writer.NewRecord()
	defer rec.Release()

	if rec.NumRows() != 3 {
		t.Fatalf("expected 3 rows, got %d", rec.NumRows())
	}

	// --- Arrow reader round-trip ---
	reader, err := NewARClearableDeltaArrowReader(rec)
	if err != nil {
		t.Fatalf("NewARClearableDeltaArrowReader: %v", err)
	}

	var got0, got1, got2 ARClearableDelta
	reader.LoadRow(0, &got0)
	reader.LoadRow(1, &got1)
	reader.LoadRow(2, &got2)

	// Row 0: all Op == OpIgnore.
	if got0.Location.Op != eddt.OpIgnore {
		t.Errorf("row0 Location.Op: want OpIgnore, got %v", got0.Location.Op)
	}
	if got0.Tags.Op != eddt.OpIgnore {
		t.Errorf("row0 Tags.Op: want OpIgnore, got %v", got0.Tags.Op)
	}
	if got0.Groups.Op != eddt.OpIgnore {
		t.Errorf("row0 Groups.Op: want OpIgnore, got %v", got0.Groups.Op)
	}

	// Row 1: all Op == OpAssert; inner-delta payloads preserved.
	if got1.Location.Op != eddt.OpAssert {
		t.Errorf("row1 Location.Op: want OpAssert, got %v", got1.Location.Op)
	}
	if got1.Location.Value.SetStreet == nil || *got1.Location.Value.SetStreet != "1 Main" {
		t.Errorf("row1 Location.Value.SetStreet: want %q, got %v", "1 Main", got1.Location.Value.SetStreet)
	}
	if got1.Tags.Op != eddt.OpAssert {
		t.Errorf("row1 Tags.Op: want OpAssert, got %v", got1.Tags.Op)
	}
	if got1.Tags.Value.UpdatedTags["k"] != "v" {
		t.Errorf("row1 Tags.Value.UpdatedTags[k]: want %q, got %q", "v", got1.Tags.Value.UpdatedTags["k"])
	}
	if len(got1.Tags.Value.RemovedTags) != 0 {
		t.Errorf("row1 Tags.Value.RemovedTags: want empty, got %v", got1.Tags.Value.RemovedTags)
	}
	if got1.Groups.Op != eddt.OpAssert {
		t.Errorf("row1 Groups.Op: want OpAssert, got %v", got1.Groups.Op)
	}
	if len(got1.Groups.Value.AddedGroups) != 1 || got1.Groups.Value.AddedGroups[0] != "x" {
		t.Errorf("row1 Groups.Value.AddedGroups: want [x], got %v", got1.Groups.Value.AddedGroups)
	}
	if len(got1.Groups.Value.RemovedGroups) != 0 {
		t.Errorf("row1 Groups.Value.RemovedGroups: want empty, got %v", got1.Groups.Value.RemovedGroups)
	}

	// Row 2: all Op == OpRetract; Value sub-struct is zero.
	if got2.Location.Op != eddt.OpRetract {
		t.Errorf("row2 Location.Op: want OpRetract, got %v", got2.Location.Op)
	}
	if got2.Location.Value.SetStreet != nil {
		t.Errorf("row2 Location.Value.SetStreet: want nil, got %v", got2.Location.Value.SetStreet)
	}
	if got2.Tags.Op != eddt.OpRetract {
		t.Errorf("row2 Tags.Op: want OpRetract, got %v", got2.Tags.Op)
	}
	if len(got2.Tags.Value.UpdatedTags) != 0 {
		t.Errorf("row2 Tags.Value.UpdatedTags: want empty, got %v", got2.Tags.Value.UpdatedTags)
	}
	if got2.Groups.Op != eddt.OpRetract {
		t.Errorf("row2 Groups.Op: want OpRetract, got %v", got2.Groups.Op)
	}
	if len(got2.Groups.Value.AddedGroups) != 0 {
		t.Errorf("row2 Groups.Value.AddedGroups: want empty, got %v", got2.Groups.Value.AddedGroups)
	}

	// --- DuckDB Parquet verification ---
	parquetPath := filepath.Join(t.TempDir(), "clearable.parquet")
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

	// Verify Op values for all three rows × all three fields.
	rows, err := db.Query(fmt.Sprintf(
		"SELECT Location.Op, Tags.Op, Groups.Op FROM read_parquet('%s') ORDER BY Sequence",
		parquetPath,
	))
	if err != nil {
		t.Fatalf("DuckDB Op query: %v", err)
	}
	defer rows.Close()
	type opRow struct{ locOp, tagsOp, groupsOp int32 }
	var opRows []opRow
	for rows.Next() {
		var r opRow
		if err := rows.Scan(&r.locOp, &r.tagsOp, &r.groupsOp); err != nil {
			t.Fatalf("DuckDB Op scan: %v", err)
		}
		opRows = append(opRows, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("DuckDB Op rows: %v", err)
	}
	if len(opRows) != 3 {
		t.Fatalf("DuckDB: expected 3 Op rows, got %d", len(opRows))
	}
	// Row 0: all OpIgnore (0).
	if opRows[0].locOp != 0 || opRows[0].tagsOp != 0 || opRows[0].groupsOp != 0 {
		t.Errorf("DuckDB row0 Op: want 0/0/0, got %d/%d/%d", opRows[0].locOp, opRows[0].tagsOp, opRows[0].groupsOp)
	}
	// Row 1: all OpAssert (1).
	if opRows[1].locOp != 1 || opRows[1].tagsOp != 1 || opRows[1].groupsOp != 1 {
		t.Errorf("DuckDB row1 Op: want 1/1/1, got %d/%d/%d", opRows[1].locOp, opRows[1].tagsOp, opRows[1].groupsOp)
	}
	// Row 2: all OpRetract (2).
	if opRows[2].locOp != 2 || opRows[2].tagsOp != 2 || opRows[2].groupsOp != 2 {
		t.Errorf("DuckDB row2 Op: want 2/2/2, got %d/%d/%d", opRows[2].locOp, opRows[2].tagsOp, opRows[2].groupsOp)
	}

	// Row 1 nested field extraction: Location.Value.SetStreet, Tags.Value.UpdatedTags['k'],
	// Groups.Value.AddedGroups[1] (DuckDB lists are 1-indexed).
	var locStreet string
	var tagsVal, groupsFirst string
	err = db.QueryRow(fmt.Sprintf(
		"SELECT Location.Value.SetStreet, Tags.Value.UpdatedTags['k'], Groups.Value.AddedGroups[1] FROM read_parquet('%s') WHERE Sequence = 2",
		parquetPath,
	)).Scan(&locStreet, &tagsVal, &groupsFirst)
	if err != nil {
		t.Fatalf("DuckDB row1 nested query: %v", err)
	}
	if locStreet != "1 Main" {
		t.Errorf("DuckDB row1 Location.Value.SetStreet: want %q, got %q", "1 Main", locStreet)
	}
	if tagsVal != "v" {
		t.Errorf("DuckDB row1 Tags.Value.UpdatedTags[k]: want %q, got %q", "v", tagsVal)
	}
	if groupsFirst != "x" {
		t.Errorf("DuckDB row1 Groups.Value.AddedGroups[1]: want %q, got %q", "x", groupsFirst)
	}

	// Row 2 zero-payload check: confirm Value sub-fields are NULL or empty.
	// pqarrow may encode a nil/empty slice as NULL — accept either.
	var tagsUpdatedNull, groupsAddedNull bool
	err = db.QueryRow(fmt.Sprintf(
		"SELECT Tags.Value.UpdatedTags IS NULL OR cardinality(Tags.Value.UpdatedTags) = 0,"+
			" Groups.Value.AddedGroups IS NULL OR len(Groups.Value.AddedGroups) = 0"+
			" FROM read_parquet('%s') WHERE Sequence = 3",
		parquetPath,
	)).Scan(&tagsUpdatedNull, &groupsAddedNull)
	if err != nil {
		t.Fatalf("DuckDB row2 zero-payload query: %v", err)
	}
	if !tagsUpdatedNull {
		t.Errorf("DuckDB row2 Tags.Value.UpdatedTags: want null/empty, got non-empty")
	}
	if !groupsAddedNull {
		t.Errorf("DuckDB row2 Groups.Value.AddedGroups: want null/empty, got non-empty")
	}
}
`
