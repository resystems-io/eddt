package deltagen

// resolve.go implements the resolve stage of the delta-gen pipeline: deciding
// the output package name and whether emission is cross-package, from the
// loaded packages and the --pkg-name / --pkg-alias flags.
//
// The resolve stage runs after load (it reads the source package's name and
// alias set from the loaded packages) and before parse (which consumes the
// resulting cross-package flag to drop unexported fields and omit method
// wrappers). The resolveStage method that drives it lives in generator.go,
// alongside the other stage methods; the helpers below carry the logic.
//
// # Exported surface
//
// Both symbols are package-private, consumed by generator.go's resolveStage.

import (
	"strings"

	"golang.org/x/tools/go/packages"
)

// resolveOutputPkg determines the output package name and whether the generator
// is in cross-package mode (R-DG-012, R-DG-013, R-DG-019).
//
// If outPkgNameOverride is empty the output package defaults to the name of the
// first loaded source package and crossPackage is false. If an override is given
// it becomes the output package name; crossPackage is true when the override
// differs from the source package name.
//
// The first loaded package determines the source name because delta-gen processes
// one Snapshot type per invocation; additional --pkg arguments exist only to
// resolve cross-package type references in the source, not to define independent
// output packages.
func resolveOutputPkg(pkgs []*packages.Package, outPkgNameOverride string) (pkgName string, crossPackage bool) {
	// Determine source package name from the first loaded package. Guard against
	// an empty slice even though loadPackages should never return one without error.
	srcPkgName := ""
	if len(pkgs) > 0 {
		srcPkgName = pkgs[0].Name
	}

	// No override: output package = source package; always same-package mode.
	if outPkgNameOverride == "" {
		return srcPkgName, false
	}

	// Override provided: cross-package when the names differ.
	return outPkgNameOverride, outPkgNameOverride != srcPkgName
}

// sourceHasExplicitAlias reports whether pkgs[0] (the primary source package)
// has an explicit alias in rawAliases. This is used in resolveStage to promote
// crossPackage to true when the source and output packages share a short name
// but differ in import path: an alias for the primary source package is
// definitive proof that the user intends it to be a foreign import.
//
// Only pkgs[0] is checked because resolveOutputPkg also uses only pkgs[0] to
// determine the source package name for cross-package detection. Additional
// packages in pkgs (loaded as dependency resolution helpers via --pkg) are
// handled by the type-qualifier closure in template.go and do not affect the
// cross-package flag.
//
// rawAliases is the raw slice of "importpath=alias" strings from --pkg-alias
// (same format as PkgAliases on Config). Malformed entries (no "=") are
// skipped silently, consistent with parsePkgAliases in template.go.
func sourceHasExplicitAlias(pkgs []*packages.Package, rawAliases []string) bool {
	if len(pkgs) == 0 {
		return false
	}
	sourcePath := pkgs[0].PkgPath
	for _, entry := range rawAliases {
		key, _, ok := strings.Cut(entry, "=")
		if !ok || key == "" {
			continue
		}
		if key == sourcePath {
			return true
		}
	}
	return false
}
