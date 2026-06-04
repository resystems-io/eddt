package deltagen

// resolve_test.go exercises the resolve stage (R-DG-036, R-DG-037, R-DG-038):
// output-package-name resolution and cross-package-mode detection, including
// the alias-promoted case where the output and source packages share a short
// name but differ in import path.
//
// Tests call the package-private resolveOutputPkg / sourceHasExplicitAlias and
// the Generator.resolveStage method directly, reusing the package-level test
// helpers (loadPackages, runtimePkgPath, writeTempModule) defined alongside the
// load-stage tests.

import (
	"log/slog"
	"testing"
)

// ── Output-package resolver ─────────────────────────────────────────

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

// ── Alias-promoted cross-package detection ───────────────────────────

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
}
