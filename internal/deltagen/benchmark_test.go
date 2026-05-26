package deltagen

// benchmark_test.go implements C-07: subprocess benchmark harness for the
// three generated operations — Apply, Diff, and Coalesce.
//
// Each outer test function generates a delta source for a corpus case, writes
// it alongside the fixture and an inner benchmark file into an isolated temp
// module, and runs go test -bench=. -benchtime=1s -run=^$ to collect baseline
// throughput numbers.  No regression assertions are made; the baseline is
// recorded in the commit message.
//
// Test matrix (C-07):
//
//	TestBenchmark_Baseline   — BenchmarkApply / BenchmarkDiff / BenchmarkCoalesce
//	TestBenchmark_Composite  — BenchmarkApply / BenchmarkDiff / BenchmarkCoalesce

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestBenchmark_Baseline runs Apply, Diff, and Coalesce benchmarks against the
// baseline corpus case (all five atomic shapes).
func TestBenchmark_Baseline(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "delta.go")
	cfg := Config{
		InputPkgs:     []string{"./testdata/corpus/baseline"},
		TargetStructs: []string{"BaselineSnapshot"},
		OutPath:       outPath,
	}
	if err := New(cfg).Run(); err != nil {
		t.Fatalf("Run(): %v", err)
	}
	src, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read generated file: %v", err)
	}
	benchmarkCheckCorpus(t, "baseline", "baseline", src, baselineBenchmarkTest)
}

// TestBenchmark_Composite runs Apply, Diff, and Coalesce benchmarks against the
// composite corpus case (delta.nested struct/map/slice shapes).
func TestBenchmark_Composite(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "delta.go")
	cfg := Config{
		InputPkgs:     []string{"./testdata/corpus/composite"},
		TargetStructs: []string{"CompositeSnapshot"},
		OutPath:       outPath,
	}
	if err := New(cfg).Run(); err != nil {
		t.Fatalf("Run(): %v", err)
	}
	src, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read generated file: %v", err)
	}
	benchmarkCheckCorpus(t, "composite", "composite", src, compositeBenchmarkTest)
}

// benchmarkCheckCorpus writes the corpus fixture, the generated delta source,
// and an injected benchmark file into an isolated temp module and runs
// go test -bench=. -benchtime=1s -run=^$.
//
// Structure mirrors coalesceCheckCorpus exactly; only the injected filename
// and go test flags differ.
func benchmarkCheckCorpus(t *testing.T, dir, pkgName string, generatedSrc []byte, testSrc string) {
	t.Helper()

	tmpDir := t.TempDir()

	// Two levels up: internal/deltagen → internal → module root.
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	moduleRoot := filepath.Clean(filepath.Join(wd, "..", ".."))

	// Copy the fixture source file as snapshot.go in the temp module.
	fixtureDir := filepath.Join("testdata", "corpus", dir)
	entries, err := os.ReadDir(fixtureDir)
	if err != nil {
		t.Fatalf("readdir %s: %v", fixtureDir, err)
	}
	wroteFixture := false
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".go") {
			fixtureSrc, err := os.ReadFile(filepath.Join(fixtureDir, e.Name()))
			if err != nil {
				t.Fatalf("read fixture %s: %v", e.Name(), err)
			}
			if err := os.WriteFile(filepath.Join(tmpDir, "snapshot.go"), fixtureSrc, 0644); err != nil {
				t.Fatalf("write snapshot.go: %v", err)
			}
			wroteFixture = true
			break
		}
	}
	if !wroteFixture {
		t.Fatalf("no .go file found in %s", fixtureDir)
	}

	// Write the generated delta source and assert it is gofmt-clean.
	deltaPath := filepath.Join(tmpDir, "delta.go")
	if err := os.WriteFile(deltaPath, generatedSrc, 0644); err != nil {
		t.Fatalf("write delta.go: %v", err)
	}
	assertGofmtClean(t, deltaPath)

	// Write the benchmark test source.
	if err := os.WriteFile(filepath.Join(tmpDir, "benchmark_test.go"), []byte(testSrc), 0644); err != nil {
		t.Fatalf("write benchmark_test.go: %v", err)
	}

	// Write go.mod with a replace directive pointing at the local module root.
	modContent := "module " + pkgName + "\n\ngo 1.25.0\n\nrequire go.resystems.io/eddt v0.0.0\n\nreplace go.resystems.io/eddt => " + moduleRoot + "\n"
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte(modContent), 0644); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}

	// Copy go.sum so transitive dependencies resolve locally.
	goSum, err := os.ReadFile(filepath.Join(moduleRoot, "go.sum"))
	if err != nil {
		t.Fatalf("read go.sum: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "go.sum"), goSum, 0644); err != nil {
		t.Fatalf("write go.sum: %v", err)
	}

	runBuildCmd(t, tmpDir, "go", "test", "-mod=mod", "-bench=.", "-benchtime=1s", "-run=^$", "./...")
}

// baselineBenchmarkTest is the inner benchmark for the baseline corpus case.
//
// Fixtures are pre-computed in TestMain via Diff(snap0, snap1) to produce a
// valid delta.  BenchmarkApply, BenchmarkDiff, and BenchmarkCoalesce each run
// for -benchtime=1s and report allocations.
const baselineBenchmarkTest = `package baseline_test

import (
	"os"
	"testing"
	"time"

	"baseline"
	eddt "go.resystems.io/eddt/runtime"
)

var (
	benchSnap0  baseline.BaselineSnapshot
	benchSnap1  baseline.BaselineSnapshot
	benchDelta  baseline.BaselineSnapshotDelta
	benchDeltas []baseline.BaselineSnapshotDelta
)

func TestMain(m *testing.M) {
	fixedID := eddt.EntityID{1}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ptr42 := int32(42)

	benchSnap0 = baseline.BaselineSnapshot{
		Header: eddt.Header{EntityID: fixedID, ChainID: "bench",
			Sequence: 0, EffectiveAt: now},
		Key: "k",
	}
	benchSnap1 = baseline.BaselineSnapshot{
		Header: eddt.Header{EntityID: fixedID, ChainID: "bench",
			Sequence: 1, EffectiveAt: now},
		Key: "k", Name: "after", Priority: &ptr42,
		Meta:  baseline.MetaInfo{Region: "us-east", Version: 1},
		Tags:  []string{"alpha", "beta", "gamma"},
		Attrs: map[string]string{"env": "prod", "tier": "web"},
		Score: 99,
	}

	var err error
	benchDelta, err = baseline.Diff(benchSnap0, benchSnap1)
	if err != nil {
		panic("benchmark setup: " + err.Error())
	}
	benchDeltas = []baseline.BaselineSnapshotDelta{benchDelta}
	os.Exit(m.Run())
}

func BenchmarkApply(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_, _ = baseline.Apply(benchSnap0, benchDelta)
	}
}

func BenchmarkDiff(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_, _ = baseline.Diff(benchSnap0, benchSnap1)
	}
}

func BenchmarkCoalesce(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_, _ = baseline.Coalesce(benchSnap0, benchDeltas)
	}
}
`

// compositeBenchmarkTest is the inner benchmark for the composite corpus case.
//
// Covers delta.nested shapes: Details ContactDetails (N-01),
// Labels map[string]string (N-03), Groups []string (N-04), Rank int32 (atomic).
const compositeBenchmarkTest = `package composite_test

import (
	"os"
	"testing"
	"time"

	"composite"
	eddt "go.resystems.io/eddt/runtime"
)

var (
	benchSnap0  composite.CompositeSnapshot
	benchSnap1  composite.CompositeSnapshot
	benchDelta  composite.CompositeSnapshotDelta
	benchDeltas []composite.CompositeSnapshotDelta
)

func TestMain(m *testing.M) {
	fixedID := eddt.EntityID{1}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	benchSnap0 = composite.CompositeSnapshot{
		Header: eddt.Header{EntityID: fixedID, ChainID: "bench",
			Sequence: 0, EffectiveAt: now},
		Key: "k",
	}
	benchSnap1 = composite.CompositeSnapshot{
		Header: eddt.Header{EntityID: fixedID, ChainID: "bench",
			Sequence: 1, EffectiveAt: now},
		Key:     "k",
		Details: composite.ContactDetails{Email: "a@b.com", Phone: "555-0100"},
		Labels:  map[string]string{"env": "prod"},
		Groups:  []string{"admin", "ops"},
		Rank:    7,
	}

	var err error
	benchDelta, err = composite.Diff(benchSnap0, benchSnap1)
	if err != nil {
		panic("benchmark setup: " + err.Error())
	}
	benchDeltas = []composite.CompositeSnapshotDelta{benchDelta}
	os.Exit(m.Run())
}

func BenchmarkApply(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_, _ = composite.Apply(benchSnap0, benchDelta)
	}
}

func BenchmarkDiff(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_, _ = composite.Diff(benchSnap0, benchSnap1)
	}
}

func BenchmarkCoalesce(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_, _ = composite.Coalesce(benchSnap0, benchDeltas)
	}
}
`
