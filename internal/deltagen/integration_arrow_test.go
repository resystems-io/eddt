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
}

// arrowRoundTripCheck sets up an isolated temp module, runs delta-gen followed
// by arrow-writer-gen and arrow-reader-gen against the arrowroundtrip fixture,
// writes an inner go test that round-trips an ARSnapshotDelta value through an
// Arrow record, and executes it via go test.
//
// Module layout:
//
//	tmpDir/go.mod          — module arrowroundtrip; replace go.resystems.io/eddt
//	tmpDir/go.sum          — copied from the eddt module root
//	tmpDir/snapshot.go     — corpus fixture (ARSnapshot + ARMeta)
//	tmpDir/delta.go        — delta-gen output
//	tmpDir/arrow_writer.go — arrow-writer-gen output
//	tmpDir/arrow_reader.go — arrow-reader-gen output
//	tmpDir/roundtrip_test.go — inner test executed by go test
//
// GOFLAGS=-mod=mod is set so that go/packages accepts the minimal go.mod.
// go.resystems.io/eddt/runtime is passed as a second input package to the arrow
// generators so that resolveEmbeddedFields can find the runtime.Header AST and
// promote its fields into the generated Arrow code.
func arrowRoundTripCheck(t *testing.T) {
	t.Helper()
	t.Setenv("GOFLAGS", "-mod=mod")

	tmpDir := t.TempDir()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	moduleRoot := filepath.Clean(filepath.Join(wd, "..", ".."))

	// Write go.mod before any generator call so go/packages resolves imports
	// relative to this module, not the eddt module root.
	modName := "arrowroundtrip"
	modContent := fmt.Sprintf(
		"module %s\n\ngo 1.25.0\n\nrequire go.resystems.io/eddt v0.0.0\n\nreplace go.resystems.io/eddt => %s\n",
		modName, moduleRoot,
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

	// Copy the fixture (both files must be in the same package).
	for _, name := range []string{"snapshot.go", "snapshot_extended.go"} {
		src, err := os.ReadFile(filepath.Join("testdata", "arrow_roundtrip", name))
		if err != nil {
			t.Fatalf("read fixture %s: %v", name, err)
		}
		if err := os.WriteFile(filepath.Join(tmpDir, name), src, 0644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	// Step 1: run delta-gen to produce delta.go.
	deltaPath := filepath.Join(tmpDir, "delta.go")
	if err := New(Config{
		InputPkgs:     []string{tmpDir},
		TargetStructs: []string{"ARSnapshot"},
		OutPath:       deltaPath,
	}).Run(); err != nil {
		t.Fatalf("delta-gen Run(): %v", err)
	}
	assertGofmtClean(t, deltaPath)

	// Step 2: fetch the Arrow dependency so packages.Load can resolve imports in
	// the arrow-gen output files.
	runBuildCmd(t, tmpDir, "go", "get", arrowtest.ArrowDep)

	// inputPkgs for the arrow generators: the temp module plus the eddt runtime
	// package.  Passing the runtime import path ensures go.resystems.io/eddt/runtime
	// is in allPkgs so that resolveEmbeddedFields can walk runtime.Header's fields.
	arrowInputPkgs := []string{tmpDir, "go.resystems.io/eddt/runtime"}
	targets := []string{"ARSnapshot", "ARSnapshotDelta"}

	// Step 3: run arrow-writer-gen.
	writerPath := filepath.Join(tmpDir, "arrow_writer.go")
	wg := writergen.NewGenerator(arrowInputPkgs, targets, writerPath, false, nil)
	if err := wg.Run(""); err != nil {
		t.Fatalf("arrow-writer-gen Run(): %v", err)
	}
	assertGofmtClean(t, writerPath)

	// Step 4: run arrow-reader-gen.
	readerPath := filepath.Join(tmpDir, "arrow_reader.go")
	rg := readergen.NewGenerator(arrowInputPkgs, targets, readerPath, false, nil)
	if err := rg.Run(""); err != nil {
		t.Fatalf("arrow-reader-gen Run(): %v", err)
	}
	assertGofmtClean(t, readerPath)

	// Step 5: write the inner test.
	innerTest := `package arrowroundtrip

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
	if err := os.WriteFile(filepath.Join(tmpDir, "roundtrip_test.go"), []byte(innerTest), 0644); err != nil {
		t.Fatalf("write roundtrip_test.go: %v", err)
	}

	// Step 6: tidy and run.
	runBuildCmd(t, tmpDir, "go", "mod", "tidy")
	runBuildCmd(t, tmpDir, "go", "test", "-v", "-run", "TestARSnapshotDeltaRoundTrip", ".")
}

// arrowExtendedRoundTripCheck sets up an isolated temp module, runs delta-gen
// followed by arrow-writer-gen and arrow-reader-gen against ARExtended, writes
// an inner go test that round-trips an ARExtendedDelta through an Arrow record
// and verifies NULL vs value via DuckDB, and executes it via go test.
//
// All three subtests (ShapePointer, ShapeSliceAtomic, ShapeMapAtomic) call this
// helper because ARExtendedDelta carries all three field shapes (**int32,
// *[]string, *map[string]string) in a single struct.
func arrowExtendedRoundTripCheck(t *testing.T) {
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

	for _, name := range []string{"snapshot.go", "snapshot_extended.go"} {
		src, err := os.ReadFile(filepath.Join("testdata", "arrow_roundtrip", name))
		if err != nil {
			t.Fatalf("read fixture %s: %v", name, err)
		}
		if err := os.WriteFile(filepath.Join(tmpDir, name), src, 0644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	// Step 1: delta-gen → ARExtendedDelta.
	deltaPath := filepath.Join(tmpDir, "delta.go")
	if err := New(Config{
		InputPkgs:     []string{tmpDir},
		TargetStructs: []string{"ARExtended"},
		OutPath:       deltaPath,
	}).Run(); err != nil {
		t.Fatalf("delta-gen Run(): %v", err)
	}
	assertGofmtClean(t, deltaPath)

	// Step 2: fetch Arrow dependency before packages.Load runs inside the generators.
	runBuildCmd(t, tmpDir, "go", "get", arrowtest.ArrowDep)

	arrowInputPkgs := []string{tmpDir, "go.resystems.io/eddt/runtime"}
	targets := []string{"ARExtended", "ARExtendedDelta"}

	// Step 3: arrow-writer-gen.
	writerPath := filepath.Join(tmpDir, "arrow_writer.go")
	wg := writergen.NewGenerator(arrowInputPkgs, targets, writerPath, false, nil)
	if err := wg.Run(""); err != nil {
		t.Fatalf("arrow-writer-gen Run(): %v", err)
	}
	assertGofmtClean(t, writerPath)

	// Step 4: arrow-reader-gen.
	readerPath := filepath.Join(tmpDir, "arrow_reader.go")
	rg := readergen.NewGenerator(arrowInputPkgs, targets, readerPath, false, nil)
	if err := rg.Run(""); err != nil {
		t.Fatalf("arrow-reader-gen Run(): %v", err)
	}
	assertGofmtClean(t, readerPath)

	// Step 5: fetch DuckDB dependency for the inner test, write the inner test.
	runBuildCmd(t, tmpDir, "go", "get", arrowtest.DuckDBDep)

	innerTest := `package arrowroundtrip

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
	if err := os.WriteFile(filepath.Join(tmpDir, "roundtrip_extended_test.go"), []byte(innerTest), 0644); err != nil {
		t.Fatalf("write roundtrip_extended_test.go: %v", err)
	}

	// Step 6: tidy and run.
	runBuildCmd(t, tmpDir, "go", "mod", "tidy")
	runBuildCmd(t, tmpDir, "go", "test", "-v", "-run", "TestARExtendedDeltaRoundTrip", ".")
}
