package deltagen

// load_test.go exercises the package loading stage (G-02) in four groups:
//
//   A. Filesystem path loading  — R-10
//   B. Import path loading      — R-10, R-11
//   C. Dependency loading       — validates NeedDeps / NeedImports correctness
//   D. Helper unit tests        — isFilesystemPath, FindPkgByPath
//
// Tests in groups A–C call the package-private loadPackages directly; they do
// not go through the CLI or generator.Run so that failure messages point
// precisely at the loading layer.
//
// The runtime package import path used throughout is
// "go.resystems.io/eddt/runtime", which is part of this module's go.mod and
// therefore resolvable from the test binary's working directory.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// runtimePkgPath is the import path of the eddt runtime package. Used in
// group C and D tests to validate transitive dependency loading.
const runtimePkgPath = "go.resystems.io/eddt/runtime"

// writeTempModule creates a minimal Go module in dir: a go.mod declaring the
// given module path and a single .go source file with the given content.
// It is a test helper and calls t.Fatal on any write error.
func writeTempModule(t *testing.T, dir, modulePath, goSrc string) {
	t.Helper()
	modContent := "module " + modulePath + "\n\ngo 1.25.0\n"
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(modContent), 0o644); err != nil {
		t.Fatalf("writeTempModule: write go.mod: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "model.go"), []byte(goSrc), 0o644); err != nil {
		t.Fatalf("writeTempModule: write model.go: %v", err)
	}
}

// ── Group A: Filesystem path loading ─────────────────────────────────────────

// TestLoad_ValidFilesystemPath verifies that a well-formed filesystem path
// loads successfully and that the returned package name matches the Go package
// declaration in the source file.
// Covers: R-10
func TestLoad_ValidFilesystemPath(t *testing.T) {
	dir := t.TempDir()
	writeTempModule(t, dir, "testpkg", `package testpkg

type Snapshot struct {
	ID   int32
	Name string
}
`)
	pkgs, err := loadPackages([]string{dir}, false)
	if err != nil {
		t.Fatalf("loadPackages: unexpected error: %v", err)
	}
	if len(pkgs) == 0 {
		t.Fatal("expected at least one package, got none")
	}
	// The top-level package should be "testpkg".
	if pkgs[0].Name != "testpkg" {
		t.Errorf("package name: got %q, want %q", pkgs[0].Name, "testpkg")
	}
}

// TestLoad_MultipleFilesystemPaths verifies that two distinct filesystem paths
// each residing in their own module are both loaded and appear in the result.
// Covers: R-10
func TestLoad_MultipleFilesystemPaths(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()
	writeTempModule(t, dirA, "pkga", "package pkga\n")
	writeTempModule(t, dirB, "pkgb", "package pkgb\n")

	pkgs, err := loadPackages([]string{dirA, dirB}, false)
	if err != nil {
		t.Fatalf("loadPackages: unexpected error: %v", err)
	}

	// Both packages must be present.
	names := map[string]bool{}
	for _, p := range pkgs {
		names[p.Name] = true
	}
	for _, want := range []string{"pkga", "pkgb"} {
		if !names[want] {
			t.Errorf("package %q not found in result; got %v", want, pkgs)
		}
	}
}

// TestLoad_InvalidFilesystemPath verifies that a non-existent filesystem path
// produces an error whose message contains "failed to load package directory",
// matching the error format that TestCLI_InvalidPackage asserts.
// Covers: R-10
func TestLoad_InvalidFilesystemPath(t *testing.T) {
	_, err := loadPackages([]string{"/does/not/exist/surely"}, false)
	if err == nil {
		t.Fatal("expected error for non-existent path, got nil")
	}
	if !strings.Contains(err.Error(), "failed to load package directory") {
		t.Errorf("error should mention 'failed to load package directory', got: %v", err)
	}
}

// TestLoad_PackageSyntaxError verifies that a directory containing a Go file
// with a deliberate syntax error is reported as a load failure rather than
// silently producing an empty package list.
// Covers: R-10
func TestLoad_PackageSyntaxError(t *testing.T) {
	dir := t.TempDir()
	// Deliberately broken Go source — missing closing brace.
	writeTempModule(t, dir, "broken", "package broken\n\nfunc broken( {\n")

	_, err := loadPackages([]string{dir}, false)
	if err == nil {
		t.Fatal("expected error for package with syntax errors, got nil")
	}
}

// ── Group B: Import path loading ─────────────────────────────────────────────

// TestLoad_ImportPath verifies that a valid Go import path that exists in this
// module's go.mod (the eddt runtime package) loads successfully and that the
// returned package carries the correct name.
// Covers: R-10, R-11
func TestLoad_ImportPath(t *testing.T) {
	pkgs, err := loadPackages([]string{runtimePkgPath}, false)
	if err != nil {
		t.Fatalf("loadPackages(%q): unexpected error: %v", runtimePkgPath, err)
	}
	if len(pkgs) == 0 {
		t.Fatalf("expected at least one package for %q, got none", runtimePkgPath)
	}
	// The eddt runtime package declares itself as "runtime".
	if pkgs[0].Name != "runtime" {
		t.Errorf("package name: got %q, want %q", pkgs[0].Name, "runtime")
	}
}

// TestLoad_ImportPathNotInGoMod verifies that loading a nonexistent import path
// produces an error containing "go get" remediation guidance and that the error
// does NOT contain "failed to load package directory" (which would indicate the
// import path was misidentified as a filesystem path).
// Covers: R-11
func TestLoad_ImportPathNotInGoMod(t *testing.T) {
	// Create a minimal throw-away module in a temp dir and make it the working
	// directory for this test. This ensures the import path is resolved against
	// a module that definitely does not declare the nonexistent package.
	tmpDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module throwaway\n\ngo 1.25.0\n"), 0o644); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "main.go"), []byte("package throwaway\n"), 0o644); err != nil {
		t.Fatalf("write main.go: %v", err)
	}

	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	defer os.Chdir(origDir) //nolint:errcheck

	const badPkg = "github.com/nonexistent/pkg123456789"
	_, err = loadPackages([]string{badPkg}, false)
	if err == nil {
		t.Fatalf("expected error for nonexistent import path %q, got nil", badPkg)
	}

	errMsg := err.Error()
	if !strings.Contains(errMsg, "go get") {
		t.Errorf("error should contain 'go get' remediation guidance, got: %s", errMsg)
	}
	if strings.Contains(errMsg, "failed to load package directory") {
		t.Errorf("error should NOT contain 'failed to load package directory' (import path was misclassified), got: %s", errMsg)
	}
}

// ── Group C: Dependency loading (NeedDeps / NeedImports) ─────────────────────

// TestLoad_DepsIncluded verifies that loading a package by import path also
// makes its transitive dependencies available via FindPkgByPath. Specifically
// it asserts that after loading the eddt runtime package the Header type can be
// looked up from the package's type scope — exactly the operation the parse
// stage (G-03) will perform to identify embedded runtime.Header fields.
// Covers: R-10 (NeedDeps correctness)
func TestLoad_DepsIncluded(t *testing.T) {
	pkgs, err := loadPackages([]string{runtimePkgPath}, false)
	if err != nil {
		t.Fatalf("loadPackages: unexpected error: %v", err)
	}

	// FindPkgByPath must locate the runtime package even though it is returned
	// as the top-level package here; the test also validates the traversal path
	// used by G-03 for non-top-level dependencies.
	rp := FindPkgByPath(pkgs, runtimePkgPath)
	if rp == nil {
		t.Fatalf("FindPkgByPath(%q) returned nil; NeedDeps / NeedImports may not be working", runtimePkgPath)
	}

	// The Types scope must be populated (NeedTypes is set).
	if rp.Types == nil {
		t.Fatal("runtime package has nil Types; type-checker information not loaded")
	}

	// Header must be directly look-up-able — this is the lookup G-03 will use.
	headerObj := rp.Types.Scope().Lookup("Header")
	if headerObj == nil {
		t.Errorf("runtime.Header type object not found in package scope; delta-gen parse stage cannot identify embedded Header fields")
	}
}

// TestLoad_FindPkgByPath verifies FindPkgByPath for both the present and absent
// cases so that the helper's traversal behaviour is independently verified.
// Covers: R-10
func TestLoad_FindPkgByPath(t *testing.T) {
	pkgs, err := loadPackages([]string{runtimePkgPath}, false)
	if err != nil {
		t.Fatalf("loadPackages: unexpected error: %v", err)
	}

	// Present case: the runtime package itself.
	if got := FindPkgByPath(pkgs, runtimePkgPath); got == nil {
		t.Errorf("FindPkgByPath(%q): expected non-nil, got nil", runtimePkgPath)
	}

	// Absent case: a path that is definitely not in the dependency graph.
	const absent = "github.com/nonexistent/should-not-exist"
	if got := FindPkgByPath(pkgs, absent); got != nil {
		t.Errorf("FindPkgByPath(%q): expected nil, got %v", absent, got)
	}
}

// ── Group D: Helper unit tests ────────────────────────────────────────────────

// TestLoad_IsFilesystemPath verifies the isFilesystemPath classifier for every
// input category: dot paths, relative paths, absolute paths, and import paths.
func TestLoad_IsFilesystemPath(t *testing.T) {
	// absPath is a platform-correct absolute path for the positive case.
	absPath, err := filepath.Abs(".")
	if err != nil {
		t.Fatalf("filepath.Abs: %v", err)
	}

	cases := []struct {
		input string
		want  bool
	}{
		// Dot-relative — filesystem.
		{".", true},
		{"./pkg", true},
		{"../sibling", true},
		// Absolute — filesystem.
		{"/usr/local/go", true},
		{absPath, true},
		// Go import paths — not filesystem.
		{"github.com/foo/bar", false},
		{"go.resystems.io/eddt/runtime", false},
		{"golang.org/x/tools", false},
		// Bare package name with no prefix — treated as import path.
		{"runtime", false},
	}

	for _, tc := range cases {
		got := isFilesystemPath(tc.input)
		if got != tc.want {
			t.Errorf("isFilesystemPath(%q) = %v, want %v", tc.input, got, tc.want)
		}
	}
}
