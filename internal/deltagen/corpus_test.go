package deltagen

// corpus_test.go implements C-01: conformance corpus tests.
//
// Three representative Snapshot fixtures under testdata/corpus/ cover the full
// baseline surface area of the generator.  Each corpus case is exercised by
// three subtests:
//
//   - Parse:    the fixture loads and parses without error.
//   - Generate: the generator runs without error and writes a file.
//   - Compile:  the generated output builds in an isolated module (go build).
//
// The Compile subtest uses go build (not go test) because C-01's scope is
// compilation correctness only.  Behavioural correctness is tested in C-02..C-07.
//
// Test matrix (C-01):
//   TestCorpus_All/BaselineSnapshot/Parse
//   TestCorpus_All/BaselineSnapshot/Generate
//   TestCorpus_All/BaselineSnapshot/Compile
//   TestCorpus_All/ClearableCompositeSnapshot/Parse
//   TestCorpus_All/ClearableCompositeSnapshot/Generate
//   TestCorpus_All/ClearableCompositeSnapshot/Compile
//   TestCorpus_All/CompositeSnapshot/Parse
//   TestCorpus_All/CompositeSnapshot/Generate
//   TestCorpus_All/CompositeSnapshot/Compile
//   TestCorpus_All/SessionSnapshot/Parse
//   TestCorpus_All/SessionSnapshot/Generate
//   TestCorpus_All/SessionSnapshot/Compile

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// corpusCase describes one entry in the C-01 conformance corpus.
type corpusCase struct {
	// dir is the subdirectory under testdata/corpus/.
	dir string
	// name is the Snapshot struct name passed to the generator.
	name string
	// pkg is the Go module name used in the isolated compile check.
	pkg string
}

// corpus is the authoritative C-01 conformance corpus.
//
// Every case must parse without error, generate without error, and compile
// in an isolated module.
var corpus = []corpusCase{
	// baseline: all 5 atomic shapes + all baseline presence tags + scalar key.
	{dir: "baseline", name: "BaselineSnapshot", pkg: "baseline"},
	// clearable_composite: delta.nested+delta.clearable on struct+map+slice + atomic (CL-08).
	{dir: "clearable_composite", name: "ClearableCompositeSnapshot", pkg: "clearable_composite"},
	// composite: delta.nested on struct + map + slice together + atomic coexistence.
	{dir: "composite", name: "CompositeSnapshot", pkg: "composite"},
	// struct_key: struct-valued entity.key, multi-field EntityID hash (EM-05).
	{dir: "struct_key", name: "SessionSnapshot", pkg: "struct_key"},
}

// TestCorpus_All is the C-01 table-driven test.
//
// For each corpus case it runs three subtests: Parse, Generate, and Compile.
// Compile is skipped (not failed) if Generate did not produce output.
func TestCorpus_All(t *testing.T) {
	for _, tc := range corpus {
		t.Run(tc.name, func(t *testing.T) {
			// ── Parse ─────────────────────────────────────────────────────────
			// Verify the fixture loads and parses without error.
			t.Run("Parse", func(t *testing.T) {
				pkgs, err := loadPackages([]string{"./testdata/corpus/" + tc.dir}, slog.Default())
				if err != nil {
					t.Fatalf("loadPackages: %v", err)
				}
				if _, err = parseSnapshot(pkgs, tc.name, ParseOpts{}); err != nil {
					t.Fatalf("parseSnapshot(%q): %v", tc.name, err)
				}
			})

			// ── Generate ──────────────────────────────────────────────────────
			// Run the generator and capture the output source bytes for Compile.
			var generatedSrc []byte
			t.Run("Generate", func(t *testing.T) {
				outPath := filepath.Join(t.TempDir(), "delta.go")
				cfg := Config{
					InputPkgs:     []string{"./testdata/corpus/" + tc.dir},
					TargetStructs: []string{tc.name},
					OutPath:       outPath,
				}
				if err := New(cfg).Run(); err != nil {
					t.Fatalf("Run(): %v", err)
				}
				if _, err := os.Stat(outPath); err != nil {
					t.Fatalf("expected output file %q: %v", outPath, err)
				}
				src, err := os.ReadFile(outPath)
				if err != nil {
					t.Fatalf("reading output: %v", err)
				}
				generatedSrc = src
			})

			// ── Compile ───────────────────────────────────────────────────────
			// Build the generated source in an isolated module.
			t.Run("Compile", func(t *testing.T) {
				if generatedSrc == nil {
					t.Skip("Generate subtest did not produce output")
				}
				compileCheckCorpus(t, tc.dir, tc.pkg, generatedSrc)
			})
		})
	}
}

// compileCheckCorpus writes the corpus fixture and its generated delta source
// into an isolated temp module and runs go build to verify compilation.
//
// Steps:
//  1. Create a temp directory via t.TempDir().
//  2. Derive the module root (two levels above the package directory).
//  3. Copy the fixture .go source from testdata/corpus/<dir>/ as snapshot.go.
//  4. Write the generated delta source as delta.go; assert it is gofmt-clean.
//  5. Write go.mod with a replace directive pointing at the local module root.
//  6. Copy go.sum from the module root.
//  7. Run go build -mod=mod ./... in the temp directory.
func compileCheckCorpus(t *testing.T, dir, pkgName string, generatedSrc []byte) {
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

	runBuildCmd(t, tmpDir, "go", "build", "-mod=mod", "./...")
}
