package readergen

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

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
		cmd := exec.Command("go", "get", "github.com/apache/arrow/go/v18@v18.0.0-20241007013041-ab95a4d25142")
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
	"github.com/apache/arrow/go/v18/arrow/memory"
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
	"github.com/apache/arrow/go/v18/arrow/memory"
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
}
