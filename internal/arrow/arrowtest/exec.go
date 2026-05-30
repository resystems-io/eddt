// Package arrowtest provides shared utilities for the arrow-writer-gen and
// arrow-reader-gen integration test suites. The helpers here cover running
// subcommands, setting up isolated Go module workspaces, and executing
// inner benchmark processes.
package arrowtest

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// SetupModule creates a fresh temp directory, writes goCode to dummy.go, and
// writes a minimal go.mod (module name "dummy", go 1.25.0). It returns the
// temp directory path. Use it as the common scaffold for both writer-gen and
// reader-gen integration tests; each generator's setupIntegrationTest then
// runs its own generation step on top.
func SetupModule(t testing.TB, goCode string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "dummy.go"), []byte(goCode), 0644); err != nil {
		t.Fatalf("SetupModule: write dummy.go: %v", err)
	}
	const modContent = "module dummy\n\ngo 1.25.0\n"
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(modContent), 0644); err != nil {
		t.Fatalf("SetupModule: write go.mod: %v", err)
	}
	return dir
}

// RunCmd runs an external command in dir, logging its combined stdout+stderr
// output via t.Log. It calls t.Fatal if the command exits with an error.
func RunCmd(t testing.TB, dir, command string, args ...string) {
	t.Helper()
	cmd := exec.Command(command, args...)
	cmd.Dir = dir
	var outBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &outBuf

	err := cmd.Run()
	outputStr := outBuf.String()
	t.Logf("Running command: %s %s\nOutput:\n%s", command, strings.Join(args, " "), outputStr)

	if err != nil {
		t.Fatalf("Command '%s %s' failed: %v\nOutput: %s", command, strings.Join(args, " "), err, outputStr)
	}
}

// RunInnerTest writes testCode to dummy_test.go in tmpDir, fetches ArrowDep
// followed by any extraDeps (e.g. arrowtest.DuckDBDep for writer-gen tests),
// runs go mod tidy, then executes go test -v with an optional run filter.
func RunInnerTest(t *testing.T, tmpDir, testCode, testRunFilter string, extraDeps ...string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(tmpDir, "dummy_test.go"), []byte(testCode), 0644); err != nil {
		t.Fatalf("RunInnerTest: write dummy_test.go: %v", err)
	}
	RunCmd(t, tmpDir, "go", "get", ArrowDep)
	for _, dep := range extraDeps {
		RunCmd(t, tmpDir, "go", "get", dep)
	}
	RunCmd(t, tmpDir, "go", "mod", "tidy")

	args := []string{"test", "-v"}
	if testRunFilter != "" {
		args = append(args, "-run", testRunFilter)
	}
	args = append(args, ".")
	RunCmd(t, tmpDir, "go", args...)
}

// RunBenchmarkCmd runs go test -bench=. -benchmem in dir and reports the
// ns/op, B/op, and allocs/op metrics to b via b.ReportMetric. It also
// prints the raw benchmark output to stdout for visibility in CI logs.
func RunBenchmarkCmd(b *testing.B, dir string) {
	b.Helper()
	cmd := exec.Command("go", "test", "-bench=.", "-benchmem")
	cmd.Dir = dir
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		b.Fatalf("Benchmark failed: %v\nStdout: %s\nStderr: %s", err, stdout.String(), stderr.String())
	}
	fmt.Printf("\n--- Inner Benchmark Output ---\n%s\n------------------------------\n", stdout.String())
	for _, line := range strings.Split(stdout.String(), "\n") {
		if !strings.HasPrefix(line, "Benchmark") {
			continue
		}
		parts := strings.Fields(line)
		for i, p := range parts {
			if i == 0 {
				continue
			}
			switch p {
			case "ns/op", "B/op", "allocs/op":
				var v float64
				fmt.Sscanf(parts[i-1], "%f", &v)
				b.ReportMetric(v, p)
			}
		}
	}
}
