package writergen

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"go.resystems.io/eddt/internal/arrow/arrowtest"
)

// runBenchmarkCmd runs a go test benchmark and maps the results to the outer benchmark.
func runBenchmarkCmd(b *testing.B, dir string) {
	b.Helper()
	cmd := exec.Command("go", "test", "-bench=.", "-benchmem")
	cmd.Dir = dir

	// We want to capture the output so we can see the results
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		b.Fatalf("Benchmark failed: %v\nStdout: %s\nStderr: %s", err, stdout.String(), stderr.String())
	}

	// Print the output of the inner benchmark so we can actually see it in the test logs
	fmt.Printf("\n--- Inner Benchmark Output ---\n%s\n------------------------------\n", stdout.String())

	// Parse the output to report the metric correctly to the outer benchmark
	// A typical line looks like: BenchmarkArrowAppendSimpleStruct-12    	 2686888	       451.9 ns/op	       0 B/op	       0 allocs/op
	lines := strings.Split(stdout.String(), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "Benchmark") {
			parts := strings.Fields(line)
			if len(parts) >= 3 {
				// We don't need to report iterations because the outer benchmark doesn't loop N times,
				// but we can proxy the ns/op to ReportMetric for tracking.

				// Find ns/op
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

// BenchmarkArrowWriters performs a macro/integration benchmark of the generated Arrow
// appending code.
//
// ## How this technique works:
//
//  1. The outer benchmark (this function) runs standard Go 'testing.B' setup routines.
//  2. We dynamically create a temporary Go workspace containing our target payload struct
//     and execute the 'arrow-writer-gen' code generator against it.
//  3. We emit a dedicated, inner 'benchmark_test.go' into this temporary workspace that
//     uses the standard 'b.Loop()' / 'for i:=0; i<b.N' tightly wrapped around the
//     *generated* 'Append()' method.
//  4. We execute 'go test -bench=.' in the temporary workspace as a sub-process.
//  5. We parse the stdout of that sub-process to extract the exact 'ns/op', 'B/op', etc.,
//     and pipe those metrics directly into the outer benchmark using 'b.ReportMetric'.
//
// ## Why this is necessary:
//
// Go is a compiled language; we cannot natively execute code that was generated at runtime
// within the same process without resorting to Go Plugins (which are highly restrictive,
// limited by OS, and often unstable).
//
// If we tried to benchmark by using 'os/exec' from *inside* the outer 'b.N' loop,
// the benchmark would overwhelmingly measure the overhead of launching sub-processes,
// parsing, and IPC, completely dwarfing the nanosecond-level performance of the Arrow
// appender.
//
// By pushing the 'testing.B' loop cleanly into the generated module and running it
// natively via the Go toolchain, we guarantee that the measured time is exclusively
// the execution of the generated 'Append()' method with zero delegate or execution overhead.
func BenchmarkArrowWriters(b *testing.B) {

	setupWorkspace := func(b *testing.B, structCode string, structName string, testCode string) string {
		tmpDir := b.TempDir()

		if err := os.WriteFile(filepath.Join(tmpDir, "dummy.go"), []byte(structCode), 0644); err != nil {
			b.Fatalf("Failed to write dummy.go: %v", err)
		}

		modContent := "module dummy\n\ngo 1.25.0\n"
		if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte(modContent), 0644); err != nil {
			b.Fatalf("Failed to write go.mod: %v", err)
		}

		outPath := filepath.Join(tmpDir, "dummy_arrow_writer.go")
		g := NewGenerator([]string{tmpDir}, []string{structName}, outPath, false, nil)
		if err := g.Run(""); err != nil {
			b.Fatalf("Generator.Run() failed: %v", err)
		}

		if err := os.WriteFile(filepath.Join(tmpDir, "benchmark_test.go"), []byte(testCode), 0644); err != nil {
			b.Fatalf("Failed to write benchmark_test.go: %v", err)
		}

		// Initialize modules
		cmd := exec.Command("go", "get", arrowtest.ArrowDep)
		cmd.Dir = tmpDir
		if err := cmd.Run(); err != nil {
			b.Fatalf("go get arrow failed: %v", err)
		}

		cmd = exec.Command("go", "mod", "tidy")
		cmd.Dir = tmpDir
		if err := cmd.Run(); err != nil {
			b.Fatalf("go mod tidy failed: %v", err)
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

func BenchmarkArrowAppendSimpleStruct(b *testing.B) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)

	writer := NewSimpleArrowWriter(pool)
	defer writer.Release()

	record := Simple{
		ID: 100,
		Name: "Alice",
		Score: 42.5,
		Valid: true,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writer.Append(&record)
	}
}
`
		tmpDir := setupWorkspace(b, structCode, "Simple", testCode)
		runBenchmarkCmd(b, tmpDir)
	})

	b.Run("ComplexStruct", func(b *testing.B) {
		structCode := `package dummy

type Address struct {
	ZipCode int32
	City    string
}

type Complex struct {
	ID        int32
	Address   Address
	History   []Address
	Config    map[string]float64
}
`
		testCode := `package dummy

import (
	"testing"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func BenchmarkArrowAppendComplexStruct(b *testing.B) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)

	writer := NewComplexArrowWriter(pool)
	defer writer.Release()

	record := Complex{
		ID: 100,
		Address: Address{ZipCode: 90210, City: "Beverly Hills"},
		History: []Address{{ZipCode: 10001, City: "NY"}, {ZipCode: 90001, City: "LA"}},
		Config: map[string]float64{"score1": 99.5, "score2": 80.0},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writer.Append(&record)
	}
}
`
		tmpDir := setupWorkspace(b, structCode, "Complex", testCode)
		runBenchmarkCmd(b, tmpDir)
	})

	// GenericPrimArg benchmarks Append on a struct whose only non-trivial field
	// is FieldDelta[int32] — a generic instantiation with a primitive type argument.
	// This sub-benchmark isolates two code paths introduced by GI-01/GI-02:
	//   - The named-type-over-primitive cast: int8(row.Scalar.Op) for FieldDeltaOp.
	//   - The two-sub-field struct builder path for FieldDelta_Int32 (Op + Value).
	b.Run("GenericPrimArg", func(b *testing.B) {
		structCode := `package dummy

// FieldDeltaOp is a named type over int8. The generated writer casts it
// explicitly: int8(row.Scalar.Op). This exercises the named-type-over-primitive
// path in gencommon's fieldInfoFromType.
type FieldDeltaOp int8

// FieldDelta is the generic clearable-field carrier. The generator produces a
// derived struct FieldDelta_Int32 with Op (Int8) and Value (Int32) columns.
type FieldDelta[T any] struct {
	Op    FieldDeltaOp
	Value T
}

// SnapshotScalar is the benchmark payload. It holds a single clearable scalar
// field; the generator expands FieldDelta[int32] into two Arrow sub-columns.
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

// BenchmarkAppendGenericPrimArg measures the per-row cost of Append on a struct
// containing a FieldDelta[int32] field. Each call exercises:
//   - Int64 append for Seq.
//   - StructBuilder begin/end for the FieldDelta_Int32 sub-column.
//   - Int8 append with FieldDeltaOp named-type cast for Op.
//   - Int32 append for Value.
func BenchmarkAppendGenericPrimArg(b *testing.B) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0) // confirms Release() frees all Arrow builder memory

	writer := NewSnapshotScalarArrowWriter(pool)
	defer writer.Release()

	// Non-zero Op and Value prevent the compiler from constant-folding the
	// cast and append into a no-op.
	row := SnapshotScalar{
		Seq:    1,
		Scalar: FieldDelta[int32]{Op: 1, Value: 42},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writer.Append(&row)
	}
}
`
		tmpDir := setupWorkspace(b, structCode, "SnapshotScalar", testCode)
		runBenchmarkCmd(b, tmpDir)
	})

	// GenericStructArg benchmarks Append on a struct whose only non-trivial field
	// is FieldDelta[*Inner] — a generic instantiation where the type argument is a
	// pointer to a struct. This sub-benchmark isolates the pointer-type-arg path:
	//   - Nil Value  → AppendNull on the Value struct sub-builder (no recursion).
	//   - Non-nil Value → struct begin/end + String append for Inner.Z.
	//
	// The inner loop alternates 50/50 between both variants so both branches
	// contribute equally to the reported ns/op.
	b.Run("GenericStructArg", func(b *testing.B) {
		structCode := `package dummy

type FieldDeltaOp int8

type FieldDelta[T any] struct {
	Op    FieldDeltaOp
	Value T
}

// Inner is the pointed-to struct. Its single string field produces a String
// sub-column nested inside the nullable Value struct builder.
type Inner struct {
	Z string
}

// SnapshotPtr holds a clearable pointer-to-struct field. The generator produces
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

// BenchmarkAppendGenericStructArg measures the per-row cost of Append on a struct
// containing a FieldDelta[*Inner] field. The loop alternates between:
//   - Non-nil Value (even i): appends Op, then recurses into Inner.Z (String).
//   - Nil Value    (odd i):  appends Op, then AppendNull on the Value sub-builder.
//
// Both rows are pre-constructed outside the loop so only the Append call itself
// is measured; struct initialisation overhead does not appear in the result.
func BenchmarkAppendGenericStructArg(b *testing.B) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)

	writer := NewSnapshotPtrArrowWriter(pool)
	defer writer.Release()

	inner := &Inner{Z: "hello"}
	// Pre-construct both row variants outside the timed loop.
	rowNonNil := SnapshotPtr{Seq: 1, PtrStruct: FieldDelta[*Inner]{Op: 2, Value: inner}}
	rowNil    := SnapshotPtr{Seq: 2, PtrStruct: FieldDelta[*Inner]{Op: 0, Value: nil}}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			writer.Append(&rowNonNil) // non-nil: recurse into Inner.Z
		} else {
			writer.Append(&rowNil)    // nil: AppendNull on Value struct builder
		}
	}
}
`
		tmpDir := setupWorkspace(b, structCode, "SnapshotPtr", testCode)
		runBenchmarkCmd(b, tmpDir)
	})

	// GenericCombined benchmarks Append on the full delta-model Snapshot struct,
	// which carries both a FieldDelta[int32] scalar field and a FieldDelta[*Inner]
	// pointer-to-struct field alongside a plain int64 sequence number. This is the
	// closest benchmark to a real eddt workload and represents the aggregate cost
	// of both generic field code paths in a single Append call.
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
// in a single generated writer.
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

// BenchmarkAppendGenericCombined measures the aggregate per-row cost of Append
// on Snapshot, which exercises both generic field paths in a single call:
//   - FieldDelta[int32]: Int8 cast + Int32 sub-builder (FieldDelta_Int32)
//   - FieldDelta[*Inner]: Int8 cast + nullable Struct sub-builder (FieldDelta_PtrInner)
//
// Use this benchmark to track overall delta-model write throughput end-to-end.
func BenchmarkAppendGenericCombined(b *testing.B) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)

	writer := NewSnapshotArrowWriter(pool)
	defer writer.Release()

	inner := &Inner{Z: "world"}
	row := Snapshot{
		Seq:       99,
		Scalar:    FieldDelta[int32]{Op: 1, Value: 42},
		PtrStruct: FieldDelta[*Inner]{Op: 2, Value: inner},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writer.Append(&row)
	}
}
`
		tmpDir := setupWorkspace(b, structCode, "Snapshot", testCode)
		runBenchmarkCmd(b, tmpDir)
	})
}
