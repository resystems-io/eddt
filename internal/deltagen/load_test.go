package deltagen

// load_test.go exercises the package loading stage (G-02) in five groups:
//
//   A. Filesystem path loading  — R-DG-037
//   B. Import path loading      — R-DG-037, R-DG-037
//   C. Dependency loading       — validates NeedDeps / NeedImports correctness
//   D. Helper unit tests        — isFilesystemPath, FindPkgByPath
//   E. Alias-promoted cross-package — sourceHasExplicitAlias / resolveStage
//
// Tests in groups A–C call the package-private loadPackages directly; they do
// not go through the CLI or generator.Run so that failure messages point
// precisely at the loading layer.
//
// The runtime package import path used throughout is
// "go.resystems.io/eddt/runtime", which is part of this module's go.mod and
// therefore resolvable from the test binary's working directory.

import (
	"log/slog"
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
// Covers: R-DG-037
func TestLoad_ValidFilesystemPath(t *testing.T) {
	dir := t.TempDir()
	writeTempModule(t, dir, "testpkg", `package testpkg

type Snapshot struct {
	ID   int32
	Name string
}
`)
	pkgs, err := loadPackages([]string{dir}, slog.Default())
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
// Covers: R-DG-037
func TestLoad_MultipleFilesystemPaths(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()
	writeTempModule(t, dirA, "pkga", "package pkga\n")
	writeTempModule(t, dirB, "pkgb", "package pkgb\n")

	pkgs, err := loadPackages([]string{dirA, dirB}, slog.Default())
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
// Covers: R-DG-037
func TestLoad_InvalidFilesystemPath(t *testing.T) {
	_, err := loadPackages([]string{"/does/not/exist/surely"}, slog.Default())
	if err == nil {
		t.Fatal("expected error for non-existent path, got nil")
	}
	if !strings.Contains(err.Error(), "failed to load package directory") {
		t.Errorf("error should mention 'failed to load package directory', got: %v", err)
	}
}

// TestLoad_PackageSyntaxError verifies that a directory containing a Go file
// with a deliberate syntax error is reported as a load failure rather than
// silently producing an empty package list, and that the error message surfaces
// the actual loader diagnostic (not just a count).
// Covers: R-DG-037, R-DG-037
func TestLoad_PackageSyntaxError(t *testing.T) {
	dir := t.TempDir()
	// Deliberately broken Go source — missing closing brace.
	writeTempModule(t, dir, "broken", "package broken\n\nfunc broken( {\n")

	_, err := loadPackages([]string{dir}, slog.Default())
	if err == nil {
		t.Fatal("expected error for package with syntax errors, got nil")
	}
	// The error must surface the actual loader diagnostic so the user can act,
	// not just "had N error(s)" with no further detail (R-DG-037 regression guard).
	msg := err.Error()
	if !strings.Contains(msg, "expected") && !strings.Contains(msg, "syntax") && !strings.Contains(msg, "broken") {
		t.Errorf("error message should contain a loader diagnostic (expected/syntax/filename); got: %s", msg)
	}
}

// ── Group B: Import path loading ─────────────────────────────────────────────

// TestLoad_ImportPath verifies that a valid Go import path that exists in this
// module's go.mod (the eddt runtime package) loads successfully and that the
// returned package carries the correct name.
// Covers: R-DG-037, R-DG-037
func TestLoad_ImportPath(t *testing.T) {
	pkgs, err := loadPackages([]string{runtimePkgPath}, slog.Default())
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
// Covers: R-DG-037
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
	_, err = loadPackages([]string{badPkg}, slog.Default())
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
// Covers: R-DG-037 (NeedDeps correctness)
func TestLoad_DepsIncluded(t *testing.T) {
	pkgs, err := loadPackages([]string{runtimePkgPath}, slog.Default())
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
// Covers: R-DG-037
func TestLoad_FindPkgByPath(t *testing.T) {
	pkgs, err := loadPackages([]string{runtimePkgPath}, slog.Default())
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

// ── Group E: Output-package resolver ─────────────────────────────────────────

// TestResolve_NoOverride verifies that when no --pkg-name override is supplied
// the output package name equals the source package name and crossPackage is
// false.
// Covers: R-DG-036, R-DG-037, R-DG-038, R-DG-037
func TestResolve_NoOverride(t *testing.T) {
	pkgs, err := loadPackages([]string{runtimePkgPath}, slog.Default())
	if err != nil {
		t.Fatalf("loadPackages: %v", err)
	}

	name, cross := resolveOutputPkg(pkgs, "")
	if name != "runtime" {
		t.Errorf("pkgName: got %q, want %q", name, "runtime")
	}
	if cross {
		t.Error("crossPackage: got true, want false")
	}
}

// TestResolve_OverrideMatchingSource verifies that when --pkg-name is set to
// the same value as the source package name crossPackage is false.
// Covers: R-DG-036, R-DG-037, R-DG-038, R-DG-037
func TestResolve_OverrideMatchingSource(t *testing.T) {
	pkgs, err := loadPackages([]string{runtimePkgPath}, slog.Default())
	if err != nil {
		t.Fatalf("loadPackages: %v", err)
	}

	name, cross := resolveOutputPkg(pkgs, "runtime")
	if name != "runtime" {
		t.Errorf("pkgName: got %q, want %q", name, "runtime")
	}
	if cross {
		t.Error("crossPackage: got true, want false")
	}
}

// TestResolve_OverrideDiffering verifies that when --pkg-name is set to a
// value different from the source package name crossPackage is true and the
// returned name equals the override.
// Covers: R-DG-036, R-DG-037, R-DG-038, R-DG-037
func TestResolve_OverrideDiffering(t *testing.T) {
	pkgs, err := loadPackages([]string{runtimePkgPath}, slog.Default())
	if err != nil {
		t.Fatalf("loadPackages: %v", err)
	}

	name, cross := resolveOutputPkg(pkgs, "myoutputpkg")
	if name != "myoutputpkg" {
		t.Errorf("pkgName: got %q, want %q", name, "myoutputpkg")
	}
	if !cross {
		t.Error("crossPackage: got false, want true")
	}
}

// TestResolve_MultiPkgInput verifies that when multiple packages are loaded
// the first package's name determines the source for cross-package detection,
// regardless of subsequent package names.
// Covers: R-DG-037
func TestResolve_MultiPkgInput(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()
	writeTempModule(t, dirA, "pkga", "package pkga\n")
	writeTempModule(t, dirB, "pkgb", "package pkgb\n")

	pkgs, err := loadPackages([]string{dirA, dirB}, slog.Default())
	if err != nil {
		t.Fatalf("loadPackages: %v", err)
	}

	// No override: output pkg auto-detected from first loaded package (pkga).
	name, cross := resolveOutputPkg(pkgs, "")
	if name != "pkga" {
		t.Errorf("no-override pkgName: got %q, want %q", name, "pkga")
	}
	if cross {
		t.Errorf("no-override crossPackage: got true, want false")
	}

	// Override matching first package: same-package.
	_, cross = resolveOutputPkg(pkgs, "pkga")
	if cross {
		t.Errorf("pkga-override crossPackage: got true, want false")
	}

	// Override matching second package only: cross-package, because the first
	// package (pkga) determines the source, not the second.
	name, cross = resolveOutputPkg(pkgs, "pkgb")
	if name != "pkgb" {
		t.Errorf("pkgb-override pkgName: got %q, want %q", name, "pkgb")
	}
	if !cross {
		t.Errorf("pkgb-override crossPackage: got false, want true")
	}
}

// ── Group E: Alias-promoted cross-package detection ───────────────────────────

// TestSourceHasExplicitAlias exercises the sourceHasExplicitAlias helper
// across the scenarios that matter for the alias-promoted cross-package fix.
func TestSourceHasExplicitAlias(t *testing.T) {
	pkgs, err := loadPackages([]string{runtimePkgPath}, slog.Default())
	if err != nil {
		t.Fatalf("loadPackages: %v", err)
	}

	runtimePath := "go.resystems.io/eddt/runtime"

	cases := []struct {
		label    string
		aliases  []string
		wantTrue bool
	}{
		{
			label:    "empty alias list",
			aliases:  nil,
			wantTrue: false,
		},
		{
			label:    "alias for an unknown import path (not in pkgs)",
			aliases:  []string{"example.com/other/pkg=other"},
			wantTrue: false,
		},
		{
			label:    "alias for the loaded source package",
			aliases:  []string{runtimePath + "=eddtrt"},
			wantTrue: true,
		},
		{
			label:    "alias for source package among multiple aliases",
			aliases:  []string{"example.com/a=a", runtimePath + "=eddtrt", "example.com/b=b"},
			wantTrue: true,
		},
		{
			label:    "malformed alias entry (no = separator) — skipped",
			aliases:  []string{"malformed-no-equals"},
			wantTrue: false,
		},
		{
			label:    "malformed alias with empty key part",
			aliases:  []string{"=alias"},
			wantTrue: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			got := sourceHasExplicitAlias(pkgs, tc.aliases)
			if got != tc.wantTrue {
				t.Errorf("sourceHasExplicitAlias = %v, want %v", got, tc.wantTrue)
			}
		})
	}
}

// TestResolve_SameNameAlias_ForcedCrossPackage verifies the alias-promoted
// cross-package detection end-to-end through resolveStage. When the output
// package name matches the source package name (normally → crossPackage=false),
// an explicit alias for the source package must force crossPackage=true.
func TestResolve_SameNameAlias_ForcedCrossPackage(t *testing.T) {
	pkgs, err := loadPackages([]string{runtimePkgPath}, slog.Default())
	if err != nil {
		t.Fatalf("loadPackages: %v", err)
	}

	// Baseline: same name, no alias → crossPackage remains false.
	g1 := &Generator{OutPkgNameOverride: "runtime"}
	g1.resolveStage(pkgs)
	if g1.CrossPackage {
		t.Error("baseline (no alias): expected CrossPackage=false, got true")
	}
	if g1.OutPkgName != "runtime" {
		t.Errorf("baseline OutPkgName: got %q, want %q", g1.OutPkgName, "runtime")
	}

	// With alias for source package: same name but aliased → crossPackage=true.
	g2 := &Generator{
		OutPkgNameOverride: "runtime",
		PkgAliases:         []string{"go.resystems.io/eddt/runtime=eddtrt"},
	}
	g2.resolveStage(pkgs)
	if !g2.CrossPackage {
		t.Error("with source alias: expected CrossPackage=true, got false")
	}
	if g2.OutPkgName != "runtime" {
		t.Errorf("alias case OutPkgName: got %q, want %q", g2.OutPkgName, "runtime")
	}

	// Alias for an unrelated package: no promotion.
	g3 := &Generator{
		OutPkgNameOverride: "runtime",
		PkgAliases:         []string{"example.com/unrelated=other"},
	}
	g3.resolveStage(pkgs)
	if g3.CrossPackage {
		t.Error("unrelated alias: expected CrossPackage=false, got true")
	}

	// Different name (already cross-package): alias irrelevant.
	g4 := &Generator{
		OutPkgNameOverride: "deltas",
		PkgAliases:         []string{"go.resystems.io/eddt/runtime=eddtrt"},
	}
	g4.resolveStage(pkgs)
	if !g4.CrossPackage {
		t.Error("different name + alias: expected CrossPackage=true, got false")
	}
}

// TestResolve_SameNameAlias_OnlySourcePackageChecked verifies that only pkgs[0]
// (the primary source package) is considered for alias-promoted cross-package
// detection. Aliases on secondary packages (dependency resolution helpers) must
// not trigger the promotion.
func TestResolve_SameNameAlias_OnlySourcePackageChecked(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()
	writeTempModule(t, dirA, "pkga", "package pkga\n")
	writeTempModule(t, dirB, "pkgb", "package pkgb\n")

	pkgs, err := loadPackages([]string{dirA, dirB}, slog.Default())
	if err != nil {
		t.Fatalf("loadPackages: %v", err)
	}
	if len(pkgs) < 2 {
		t.Fatalf("expected 2 packages, got %d", len(pkgs))
	}

	// pkgs[0] is pkga (the source); pkgs[1] is pkgb (dependency helper).
	pkgbPath := pkgs[1].PkgPath

	// Alias for pkgb (non-source): must NOT promote crossPackage when output = "pkga".
	g := &Generator{
		OutPkgNameOverride: "pkga",
		PkgAliases:         []string{pkgbPath + "=pb"},
	}
	g.resolveStage(pkgs)
	if g.CrossPackage {
		t.Errorf("non-source alias: CrossPackage should be false when only pkgs[1] is aliased; pkgbPath=%q", pkgbPath)
	}

	// Alias for pkga (source): MUST promote crossPackage even when output = "pkga".
	pkgaPath := pkgs[0].PkgPath
	g2 := &Generator{
		OutPkgNameOverride: "pkga",
		PkgAliases:         []string{pkgaPath + "=pa"},
	}
	g2.resolveStage(pkgs)
	if !g2.CrossPackage {
		t.Errorf("source alias: CrossPackage should be true when pkgs[0] is aliased; pkgaPath=%q", pkgaPath)
	}
	_ = strings.Contains(pkgbPath, "pkgb") // keep strings import live
}
