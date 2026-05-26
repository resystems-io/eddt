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
// Arrow-gen shape compatibility note:
//
//	gencommon.fieldInfoFromExpr handles *ast.StarExpr only for inner types
//	Ident / SelectorExpr / IndexExpr / IndexListExpr.  The following shapes
//	are NOT yet supported and have skipped subtests:
//
//	  ShapePointer  → **T in TDelta        — *ast.StarExpr inner is *ast.StarExpr
//	  ShapeSlice    → *[]T in TDelta       — *ast.StarExpr inner is *ast.ArrayType
//	  ShapeMap      → *map[K]V in TDelta   — *ast.StarExpr inner is *ast.MapType
//
//	PR-03 extends gencommon to support these shapes; C-08 removes the t.Skip.
//
// Test matrix (C-06):
//
//	TestIntegration_ArrowRoundTrip/ScalarAndStruct    PASS
//	TestIntegration_ArrowRoundTrip/ShapePointer       SKIP (PR-03 / C-08)
//	TestIntegration_ArrowRoundTrip/ShapeSliceAtomic   SKIP (PR-03 / C-08)
//	TestIntegration_ArrowRoundTrip/ShapeMapAtomic     SKIP (PR-03 / C-08)

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
		t.Skip("arrow-gen does not support **T (pointer-to-pointer delta); requires PR-03 + C-08")
		// After PR-03: generate ARExtendedDelta from testdata/arrow_roundtrip/snapshot_extended.go,
		// run writer-gen + reader-gen, round-trip SetPointer **int32 via Arrow.
	})

	t.Run("ShapeSliceAtomic", func(t *testing.T) {
		t.Skip("arrow-gen does not support *[]T (atomic-slice delta, E-15); requires PR-03 + C-08")
		// After PR-03: round-trip SetTags *[]string via Arrow.
	})

	t.Run("ShapeMapAtomic", func(t *testing.T) {
		t.Skip("arrow-gen does not support *map[K]V (atomic-map delta, E-16); requires PR-03 + C-08")
		// After PR-03: round-trip SetAttrs *map[string]string via Arrow.
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
