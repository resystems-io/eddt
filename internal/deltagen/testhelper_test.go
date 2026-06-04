package deltagen

import (
	"go/ast"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// runOpts configures a single isolated-module compile-and-run test.
type runOpts struct {
	// pkgName is the Go module name (e.g. "atomic_all").
	pkgName string

	// snapshotSrc is the content of snapshot.go when the fixture is inline.
	// Mutually exclusive with fixtureDir.
	snapshotSrc string

	// fixtureDir is a testdata/corpus/<dir> path; all .go files there are
	// written as snapshot.go. Mutually exclusive with snapshotSrc.
	fixtureDir string

	// generatedSrc is the content of delta.go (must not be nil).
	generatedSrc []byte

	// extraFiles maps file name → content for additional files to write
	// (e.g. test source files).
	extraFiles map[string]string

	// runArgs are passed to "go test -mod=mod -count=1 <runArgs...>".
	// Typically []string{"./..."} or []string{"-run", "TestFoo", "./..."}.
	runArgs []string
}

// runEmittedInModule creates an isolated temp module, writes snapshot.go
// (from opts.snapshotSrc or loaded from opts.fixtureDir), writes delta.go
// (and asserts it is gofmt-clean), writes opts.extraFiles, writes go.mod,
// copies go.sum, then runs go test with opts.runArgs.
func runEmittedInModule(t *testing.T, opts runOpts) {
	t.Helper()

	tmpDir := t.TempDir()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	moduleRoot := filepath.Clean(filepath.Join(wd, "..", ".."))

	// Write snapshot.go from inline source or from disk.
	if opts.snapshotSrc != "" {
		if err := os.WriteFile(filepath.Join(tmpDir, "snapshot.go"), []byte(opts.snapshotSrc), 0644); err != nil {
			t.Fatalf("write snapshot.go: %v", err)
		}
	} else if opts.fixtureDir != "" {
		entries, err := os.ReadDir(opts.fixtureDir)
		if err != nil {
			t.Fatalf("readdir %s: %v", opts.fixtureDir, err)
		}
		wrote := false
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".go") {
				src, err := os.ReadFile(filepath.Join(opts.fixtureDir, e.Name()))
				if err != nil {
					t.Fatalf("read fixture %s: %v", e.Name(), err)
				}
				if err := os.WriteFile(filepath.Join(tmpDir, "snapshot.go"), src, 0644); err != nil {
					t.Fatalf("write snapshot.go: %v", err)
				}
				wrote = true
				break
			}
		}
		if !wrote {
			t.Fatalf("no .go file found in %s", opts.fixtureDir)
		}
	}

	// Write delta.go and assert gofmt-clean.
	deltaPath := filepath.Join(tmpDir, "delta.go")
	if err := os.WriteFile(deltaPath, opts.generatedSrc, 0644); err != nil {
		t.Fatalf("write delta.go: %v", err)
	}
	assertGofmtClean(t, deltaPath)

	// Write extra files (test sources, helper files, etc.).
	for name, content := range opts.extraFiles {
		if err := os.WriteFile(filepath.Join(tmpDir, name), []byte(content), 0644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	// Write go.mod with replace directive.
	modContent := "module " + opts.pkgName + "\n\ngo 1.25.0\n\nrequire go.resystems.io/eddt v0.0.0\n\nreplace go.resystems.io/eddt => " + moduleRoot + "\n"
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

	args := append([]string{"test", "-mod=mod", "-count=1"}, opts.runArgs...)
	runBuildCmd(t, tmpDir, "go", args...)
}

// assertDeltaShape asserts that the named struct in f contains all wantFields.
// It fatals immediately if the struct is absent. The returned field-name slice
// can be used for subsequent absent-field or ordering assertions.
func assertDeltaShape(t *testing.T, f *ast.File, structName string, wantFields []string) []string {
	t.Helper()
	decl := findStructDecl(f, structName)
	if decl == nil {
		t.Fatalf("%s type not found in generated file", structName)
		return nil
	}
	fields := structFieldNames(decl)
	for _, want := range wantFields {
		if !contains(fields, want) {
			t.Errorf("field %q missing from %s; fields: %v", want, structName, fields)
		}
	}
	return fields
}

// assertHasMethods asserts that every name in names is a method on recvType in f.
func assertHasMethods(t *testing.T, f *ast.File, recvType string, names []string) {
	t.Helper()
	for _, name := range names {
		if findMethodDecl(f, recvType, name) == nil {
			t.Errorf("method %s.%s not found in generated file", recvType, name)
		}
	}
}
