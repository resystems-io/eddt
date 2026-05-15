package readergen

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"go.resystems.io/eddt/internal/arrow/arrowtest"
	writergen "go.resystems.io/eddt/internal/arrow/writer-gen"
)

// runBenchmarkCmd runs a go test benchmark and maps the results to the outer benchmark.
func runBenchmarkCmd(b *testing.B, dir string) {
	b.Helper()
	cmd := exec.Command("go", "test", "-bench=.", "-benchmem")
	cmd.Dir = dir

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		b.Fatalf("Benchmark failed: %v\nStdout: %s\nStderr: %s", err, stdout.String(), stderr.String())
	}

	fmt.Printf("\n--- Inner Benchmark Output ---\n%s\n------------------------------\n", stdout.String())

	// Parse the output to report metrics to the outer benchmark.
	// A typical line: BenchmarkArrowLoadRow-12    2686888    451.9 ns/op    0 B/op    0 allocs/op
	lines := strings.Split(stdout.String(), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "Benchmark") {
			parts := strings.Fields(line)
			if len(parts) >= 3 {
				for i, p := range parts {
					if p == "ns/op" && i > 0 {
						var nsOp float64
						fmt.Sscanf(parts[i-1], "%f", &nsOp)
						b.ReportMetric(nsOp, "ns/op")
					}
					if p == "B/op" && i > 0 {
						var bOp float64
						fmt.Sscanf(parts[i-1], "%f", &bOp)
						b.ReportMetric(bOp, "B/op")
					}
					if p == "allocs/op" && i > 0 {
						var allocsOp float64
						fmt.Sscanf(parts[i-1], "%f", &allocsOp)
						b.ReportMetric(allocsOp, "allocs/op")
					}
				}
			}
		}
	}
}

// BenchmarkArrowReaders performs a macro/integration benchmark of the generated Arrow
// reading code.
//
// This uses the same outer/inner subprocess technique as writer-gen's benchmarks.
// See writer-gen/integration_benchmark_test.go for a detailed explanation of why
// this approach is necessary for benchmarking generated code.
//
// In short: the outer benchmark generates both writer and reader code into a temporary
// Go workspace, emits an inner benchmark_test.go that tightly loops around LoadRow,
// and runs 'go test -bench=.' as a subprocess. Metrics are parsed from stdout and
// reported to the outer benchmark via b.ReportMetric.
func BenchmarkArrowReaders(b *testing.B) {

	setupWorkspace := func(b *testing.B, structCode string, structNames []string, testCode string) string {
		b.Helper()
		tmpDir := b.TempDir()

		if err := os.WriteFile(filepath.Join(tmpDir, "dummy.go"), []byte(structCode), 0644); err != nil {
			b.Fatalf("Failed to write dummy.go: %v", err)
		}

		modContent := "module dummy\n\ngo 1.25.0\n"
		if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte(modContent), 0644); err != nil {
			b.Fatalf("Failed to write go.mod: %v", err)
		}

		// Fetch arrow dependency before generation so packages.Load resolves imports.
		cmd := exec.Command("go", "get", arrowtest.ArrowDep)
		cmd.Dir = tmpDir
		if out, err := cmd.CombinedOutput(); err != nil {
			b.Fatalf("go get arrow failed: %v\n%s", err, out)
		}

		// Generate writer (needed to build the Arrow record that the reader will consume).
		writerOut := filepath.Join(tmpDir, "dummy_arrow_writer.go")
		wg := writergen.NewGenerator([]string{tmpDir}, structNames, writerOut, false, nil)
		if err := wg.Run(""); err != nil {
			b.Fatalf("writer-gen Run() failed: %v", err)
		}

		// Generate reader (the code under benchmark).
		readerOut := filepath.Join(tmpDir, "dummy_arrow_reader.go")
		rg := NewGenerator([]string{tmpDir}, structNames, readerOut, false, nil)
		if err := rg.Run(""); err != nil {
			b.Fatalf("reader-gen Run() failed: %v", err)
		}

		if err := os.WriteFile(filepath.Join(tmpDir, "benchmark_test.go"), []byte(testCode), 0644); err != nil {
			b.Fatalf("Failed to write benchmark_test.go: %v", err)
		}

		cmd = exec.Command("go", "mod", "tidy")
		cmd.Dir = tmpDir
		if out, err := cmd.CombinedOutput(); err != nil {
			b.Fatalf("go mod tidy failed: %v\n%s", err, out)
		}

		return tmpDir
	}

	b.Run("SimpleStruct", func(b *testing.B) {
		structCode := `package dummy

type Simple struct {
	ID    int32
	Name  string
	Score float64
	Valid bool
}
`
		testCode := `package dummy

import (
	"testing"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func BenchmarkArrowLoadRowSimpleStruct(b *testing.B) {
	pool := memory.NewGoAllocator()

	writer := NewSimpleArrowWriter(pool)

	const numRows = 1000
	for i := 0; i < numRows; i++ {
		writer.Append(&Simple{
			ID:    int32(i),
			Name:  "Alice",
			Score: 42.5,
			Valid: true,
		})
	}
	rec := writer.NewRecord()
	defer rec.Release()
	writer.Release()

	reader, err := NewSimpleArrowReader(rec)
	if err != nil {
		b.Fatal(err)
	}

	var out Simple
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader.LoadRow(i%numRows, &out)
	}
}
`
		tmpDir := setupWorkspace(b, structCode, []string{"Simple"}, testCode)
		runBenchmarkCmd(b, tmpDir)
	})

	b.Run("ComplexStruct", func(b *testing.B) {
		structCode := `package dummy

type Address struct {
	ZipCode int32
	City    string
}

type Complex struct {
	ID      int32
	Address Address
	History []Address
	Config  map[string]float64
}
`
		testCode := `package dummy

import (
	"testing"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func BenchmarkArrowLoadRowComplexStruct(b *testing.B) {
	pool := memory.NewGoAllocator()

	writer := NewComplexArrowWriter(pool)

	const numRows = 100
	for i := 0; i < numRows; i++ {
		writer.Append(&Complex{
			ID:      int32(i),
			Address: Address{ZipCode: 90210, City: "Beverly Hills"},
			History: []Address{
				{ZipCode: 10001, City: "New York"},
				{ZipCode: 90001, City: "Los Angeles"},
			},
			Config: map[string]float64{"score1": 99.5, "score2": 80.0},
		})
	}
	rec := writer.NewRecord()
	defer rec.Release()
	writer.Release()

	reader, err := NewComplexArrowReader(rec)
	if err != nil {
		b.Fatal(err)
	}

	// Pre-allocate with capacity so the benchmark measures the R6 reuse path,
	// not initial allocation.
	var out Complex
	out.History = make([]Address, 0, 4)
	out.Config = make(map[string]float64, 4)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader.LoadRow(i%numRows, &out)
	}
}
`
		tmpDir := setupWorkspace(b, structCode, []string{"Complex"}, testCode)
		runBenchmarkCmd(b, tmpDir)
	})

	// GenericPrimArg benchmarks LoadRow on a struct whose only non-trivial field
	// is FieldDelta[int32] — a generic instantiation with a primitive type argument.
	// This sub-benchmark isolates two reader code paths introduced by GI-01/02/03:
	//   - The named-type cast: FieldDeltaOp(col.Value(i)) for the Op Int8 sub-column.
	//   - The two-sub-field struct reader path for FieldDelta_Int32 (Op + Value).
	// All output fields are value types, so the R6 zero-allocation reuse path is
	// trivially exercised with no pointer pre-allocation needed.
	b.Run("GenericPrimArg", func(b *testing.B) {
		structCode := `package dummy

// FieldDeltaOp is a named type over int8. The generated reader casts it back
// explicitly: FieldDeltaOp(col.Value(i)). This exercises the named-type cast
// path in the reader template.
type FieldDeltaOp int8

// FieldDelta is the generic clearable-field carrier. The reader produces a
// derived struct FieldDelta_Int32 reader with Op (Int8) and Value (Int32) columns.
type FieldDelta[T any] struct {
	Op    FieldDeltaOp
	Value T
}

// SnapshotScalar is the benchmark payload. It holds a single clearable scalar
// field; the reader expands FieldDelta[int32] from two Arrow sub-columns.
type SnapshotScalar struct {
	Seq    int64
	Scalar FieldDelta[int32]
}
`
		testCode := `package dummy

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

// BenchmarkLoadRowGenericPrimArg measures the per-row cost of LoadRow on a struct
// containing a FieldDelta[int32] field. Each call exercises:
//   - Int64 column read for Seq.
//   - FieldDelta_Int32 sub-reader: Int8 read with FieldDeltaOp named-type cast for Op,
//     Int32 read for Value.
//
// All output fields are value types, so 0 allocs/op is expected: there is
// no pointer indirection and the R6 reuse path requires no pre-allocation.
func BenchmarkLoadRowGenericPrimArg(b *testing.B) {
	pool := memory.NewGoAllocator()

	// Build a 1000-row Arrow record using the generated writer.
	// Larger row count amortises record-build overhead; struct is lightweight.
	const numRows = 1000
	writer := NewSnapshotScalarArrowWriter(pool)
	for i := 0; i < numRows; i++ {
		writer.Append(&SnapshotScalar{
			Seq:    int64(i),
			Scalar: FieldDelta[int32]{Op: 1, Value: int32(i)},
		})
	}
	rec := writer.NewRecord()
	defer rec.Release()
	writer.Release()

	// Create the generated reader over the Arrow record.
	reader, err := NewSnapshotScalarArrowReader(rec)
	if err != nil {
		b.Fatal(err)
	}

	// Pre-declare the output struct once outside the timed loop.
	// Because SnapshotScalar is entirely value-typed, this is all that is
	// needed for the R6 zero-allocation reuse path.
	var out SnapshotScalar

	b.ReportAllocs()
	b.ResetTimer()
	// Wrap row index modulo numRows so b.N can be arbitrarily large without
	// stepping outside the record bounds.
	for i := 0; i < b.N; i++ {
		reader.LoadRow(i%numRows, &out)
	}
}
`
		tmpDir := setupWorkspace(b, structCode, []string{"SnapshotScalar"}, testCode)
		runBenchmarkCmd(b, tmpDir)
	})

	// GenericStructArg benchmarks LoadRow on a struct whose only non-trivial field
	// is FieldDelta[*Inner] — a generic instantiation where the type argument is a
	// pointer to a struct. This sub-benchmark isolates the pointer-type-arg reader path:
	//   - IsValid(i) check on the nullable Value struct column.
	//   - Non-null: reuse out.PtrStruct.Value if already allocated (R6), else allocate.
	//   - Null: set out.PtrStruct.Value = nil (pointer assign, no heap allocation).
	//
	// The pre-built record contains an even mix of null and non-null Value entries
	// (even rows non-null, odd rows null). Pre-allocating out.PtrStruct.Value primes
	// the R6 reuse path for non-null rows. Null rows then set the pointer to nil,
	// so the *next* non-null row must reallocate — expect ~0.5 allocs/op across the mix.
	b.Run("GenericStructArg", func(b *testing.B) {
		structCode := `package dummy

type FieldDeltaOp int8

type FieldDelta[T any] struct {
	Op    FieldDeltaOp
	Value T
}

// Inner is the pointed-to struct. Its single string field produces a String
// sub-column nested inside the nullable Value struct reader.
type Inner struct {
	Z string
}

// SnapshotPtr holds a clearable pointer-to-struct field. The reader produces
// a FieldDelta_PtrInner helper: Op (Int8), Value (nullable Struct{Z: String}).
type SnapshotPtr struct {
	Seq       int64
	PtrStruct FieldDelta[*Inner]
}
`
		testCode := `package dummy

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

// BenchmarkLoadRowGenericStructArg measures the per-row cost of LoadRow on a struct
// containing a FieldDelta[*Inner] field. The pre-built record alternates between:
//   - Even rows (non-null Value): reader checks IsValid → reuses out.PtrStruct.Value
//     if already allocated (R6), else allocates a new Inner.
//   - Odd rows (null Value): reader checks IsValid → sets out.PtrStruct.Value = nil.
//
// Because null rows set out.PtrStruct.Value to nil, the subsequent non-null row
// must re-allocate. Across the 50/50 mix, expect approximately 0.5 allocs/op.
// Pre-allocating out.PtrStruct.Value before the loop primes the R6 path for the
// very first non-null row only.
func BenchmarkLoadRowGenericStructArg(b *testing.B) {
	pool := memory.NewGoAllocator()

	// Build 100 rows alternating null/non-null Value so both reader branches
	// contribute equally to the reported ns/op.
	const numRows = 100
	inner := &Inner{Z: "hello"}
	writer := NewSnapshotPtrArrowWriter(pool)
	for i := 0; i < numRows; i++ {
		var v *Inner
		if i%2 == 0 {
			v = inner // even: non-null Value → String sub-column populated
		}
		writer.Append(&SnapshotPtr{
			Seq:       int64(i),
			PtrStruct: FieldDelta[*Inner]{Op: 1, Value: v},
		})
	}
	rec := writer.NewRecord()
	defer rec.Release()
	writer.Release()

	reader, err := NewSnapshotPtrArrowReader(rec)
	if err != nil {
		b.Fatal(err)
	}

	// Pre-allocate out.PtrStruct.Value so the first non-null row in the loop
	// exercises R6 reuse rather than a cold allocation. Null rows will set this
	// to nil; subsequent non-null rows after a null row will allocate once each.
	var out SnapshotPtr
	out.PtrStruct.Value = &Inner{}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader.LoadRow(i%numRows, &out)
	}
}
`
		tmpDir := setupWorkspace(b, structCode, []string{"SnapshotPtr"}, testCode)
		runBenchmarkCmd(b, tmpDir)
	})

	// GenericCombined benchmarks LoadRow on the full delta-model Snapshot struct,
	// which carries both a FieldDelta[int32] scalar field and a FieldDelta[*Inner]
	// pointer-to-struct field alongside a plain int64 sequence number. This is the
	// closest benchmark to a real eddt workload and represents the aggregate cost
	// of both generic field code paths in a single LoadRow call.
	//
	// All 1000 rows carry a non-null PtrStruct.Value. Pre-allocating out.PtrStruct.Value
	// means every iteration exercises the R6 zero-allocation reuse path, so 0 allocs/op
	// is the expected steady-state result.
	b.Run("GenericCombined", func(b *testing.B) {
		structCode := `package dummy

type FieldDeltaOp int8

type FieldDelta[T any] struct {
	Op    FieldDeltaOp
	Value T
}

type Inner struct {
	Z string
}

// Snapshot is the representative delta-model struct. It mirrors the fixture
// used in TestGenericClearableField and covers both generic field variants
// in a single generated reader.
type Snapshot struct {
	Seq       int64
	Scalar    FieldDelta[int32]
	PtrStruct FieldDelta[*Inner]
}
`
		testCode := `package dummy

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

// BenchmarkLoadRowGenericCombined measures the aggregate per-row cost of LoadRow
// on Snapshot, which exercises both generic field paths in a single call:
//   - FieldDelta[int32]: Int8 cast + Int32 sub-reader (FieldDelta_Int32).
//   - FieldDelta[*Inner]: Int8 cast + non-null Struct sub-reader (FieldDelta_PtrInner),
//     with R6 pointer reuse for Inner since all rows carry a non-null Value.
//
// With out.PtrStruct.Value pre-allocated and all rows non-null, the R6 reuse
// path is exercised on every iteration — 0 allocs/op is the expected result.
// Use this benchmark to track overall delta-model read throughput end-to-end.
func BenchmarkLoadRowGenericCombined(b *testing.B) {
	pool := memory.NewGoAllocator()

	// Build 1000 rows, all with a non-null PtrStruct.Value, to exercise the
	// steady-state R6 reuse path without null-branch interference.
	const numRows = 1000
	inner := &Inner{Z: "world"}
	writer := NewSnapshotArrowWriter(pool)
	for i := 0; i < numRows; i++ {
		writer.Append(&Snapshot{
			Seq:       int64(i),
			Scalar:    FieldDelta[int32]{Op: 1, Value: int32(i)},
			PtrStruct: FieldDelta[*Inner]{Op: 2, Value: inner},
		})
	}
	rec := writer.NewRecord()
	defer rec.Release()
	writer.Release()

	reader, err := NewSnapshotArrowReader(rec)
	if err != nil {
		b.Fatal(err)
	}

	// Pre-allocate out.PtrStruct.Value to prime R6 reuse from the very first
	// iteration. Since all rows are non-null, no reallocation occurs in steady state.
	var out Snapshot
	out.PtrStruct.Value = &Inner{}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader.LoadRow(i%numRows, &out)
	}
}
`
		tmpDir := setupWorkspace(b, structCode, []string{"Snapshot"}, testCode)
		runBenchmarkCmd(b, tmpDir)
	})
}
