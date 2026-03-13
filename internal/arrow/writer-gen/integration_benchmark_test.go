package writergen

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
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
		cmd := exec.Command("go", "get", "github.com/apache/arrow/go/v18@v18.0.0-20241007013041-ab95a4d25142")
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
	"github.com/apache/arrow/go/v18/arrow/memory"
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
	"github.com/apache/arrow/go/v18/arrow/memory"
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
}
