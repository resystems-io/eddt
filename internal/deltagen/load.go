package deltagen

// load.go implements the first stage of the delta-gen pipeline: resolving the
// user-supplied --pkg arguments into fully type-checked *packages.Package values.
//
// # Two-phase loading
//
// delta-gen accepts two kinds of package references:
//
//   - Filesystem paths (starting with ".", "..", "/", or any absolute path) are
//     loaded one at a time with packages.Config.Dir set to that path, which
//     allows each path to belong to its own Go module. This mirrors the approach
//     taken by arrow-writer-gen and arrow-reader-gen via gencommon.
//
//   - Go import paths (everything else, e.g. "go.resystems.io/eddt/runtime") are
//     batched into a single packages.Load call resolved from the invoking module's
//     go.mod. They must already appear in go.mod; if not, formatImportPathErrors
//     returns an error containing "go get" remediation guidance (R-DG-037).
//
// # Why NeedDeps is required
//
// The parse stage identifies the embedded runtime.Header field by type
// identity — not by name — to be robust against aliased imports. For that
// comparison it needs the *types.TypeName object for runtime.Header, which lives
// in the go.resystems.io/eddt/runtime package. That package is a dependency of
// any Snapshot-containing package, not a top-level input, so it only appears in
// the loaded set when packages.NeedDeps is requested.
//
// NeedImports must accompany NeedDeps: without it the Imports graph is not
// populated and packages.Visit cannot traverse transitive dependencies.
//
// # Exported surface
//
// Only FindPkgByPath is exported. All other symbols are package-private and
// consumed by generator.go's Run method and by the parse and emit stages.

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"

	"golang.org/x/tools/go/packages"
)

// loadMode is the set of package facts requested from the type checker for
// every package loaded by delta-gen.
//
// NeedName / NeedFiles / NeedSyntax provide the package identity and AST needed
// by the parse stage to walk struct declarations.
//
// NeedTypes / NeedTypesInfo give the type checker's resolved type objects and
// expression→type map, used to identify runtime.Header by type identity and to
// inspect field types and struct tags.
//
// NeedImports populates Package.Imports so the dependency graph is traversable.
// NeedDeps loads the complete transitive closure of each input package with the
// same fact set, ensuring go.resystems.io/eddt/runtime (and its Header type) is
// accessible via FindPkgByPath even though it is not a top-level --pkg argument.
const loadMode = packages.NeedName | packages.NeedFiles | packages.NeedSyntax |
	packages.NeedTypes | packages.NeedTypesInfo |
	packages.NeedImports | packages.NeedDeps

// isFilesystemPath reports whether s is a filesystem path rather than a Go
// import path. It follows the convention from "go help packages": paths
// beginning with ".", "..", or "/" are filesystem paths, as are absolute paths
// according to the host OS.
func isFilesystemPath(s string) bool {
	return s == "." ||
		strings.HasPrefix(s, "./") ||
		strings.HasPrefix(s, "../") ||
		strings.HasPrefix(s, "/") ||
		filepath.IsAbs(s)
}

// collectPackageErrors counts the total number of errors reported across all
// loaded packages (including transitive dependencies visited by packages.Visit).
// It does not print anything; callers format their own error messages.
func collectPackageErrors(pkgs []*packages.Package) int {
	count := 0
	packages.Visit(pkgs, nil, func(p *packages.Package) {
		count += len(p.Errors)
	})
	return count
}

// formatPackageErrors collects the per-package error messages from pkgs into a
// single error. Used by both the filesystem-path and import-path load branches
// so callers always see the actual loader complaints rather than just a count.
func formatPackageErrors(pkgs []*packages.Package) error {
	var msgs []string
	packages.Visit(pkgs, nil, func(p *packages.Package) {
		for _, e := range p.Errors {
			msgs = append(msgs, e.Msg)
		}
	})
	return fmt.Errorf("%s", strings.Join(msgs, "\n  "))
}

// formatImportPathErrors builds a human-readable error from the errors attached
// to a set of packages that failed to load via import path. When an error
// message matches a "missing module" pattern it appends a "go get <path>"
// remediation hint so the caller can act immediately.
func formatImportPathErrors(pkgs []*packages.Package) error {
	var msgs []string
	for _, pkg := range pkgs {
		for _, e := range pkg.Errors {
			msg := e.Msg
			// Recognise the two most common missing-module patterns emitted by
			// the Go toolchain and append actionable guidance.
			if strings.Contains(msg, "no required module provides") ||
				strings.Contains(msg, "could not import") {
				msg += fmt.Sprintf(
					"\n\n  To add it to your module's dependencies, run:"+
						"\n    go get %s\n\n  Then re-run the generator.",
					pkg.PkgPath)
			}
			msgs = append(msgs, msg)
		}
	}
	return fmt.Errorf("failed to load import paths:\n  %s", strings.Join(msgs, "\n  "))
}

// loadPackages resolves inputPkgs into a set of fully type-checked packages.
// The returned slice contains only the top-level packages; their transitive
// dependencies are reachable via FindPkgByPath, which uses packages.Visit to
// walk the full Imports graph.
//
// Filesystem paths and import paths are handled in two separate phases so that
// each filesystem path can live in its own Go module.
func loadPackages(inputPkgs []string, log *slog.Logger) ([]*packages.Package, error) {
	var all []*packages.Package
	var importPaths []string

	// Filesystem paths: load each directory separately.
	// Each path gets its own Config.Dir so that separate Go modules are resolved
	// relative to the correct root. A single call covering multiple separate
	// modules would fail because go/packages expects a single module root.
	for _, input := range inputPkgs {
		if !isFilesystemPath(input) {
			// Defer import paths to the batched phase below.
			importPaths = append(importPaths, input)
			continue
		}

		cfg := &packages.Config{Mode: loadMode, Dir: input}
		pkgs, err := packages.Load(cfg, ".")
		if err != nil {
			// Wrap the raw error with the path so the user knows which argument failed.
			return nil, fmt.Errorf("failed to load package directory %q: %w", input, err)
		}
		if n := collectPackageErrors(pkgs); n > 0 {
			return nil, fmt.Errorf("package loading had %d error(s) in %q:\n%w", n, input, formatPackageErrors(pkgs))
		}

		for _, p := range pkgs {
			log.Info("loaded package", "name", p.Name, "path", p.PkgPath)
		}
		all = append(all, pkgs...)
	}

	// Import paths: load all in a single batched call.
	// All import paths are resolved against the go.mod of the current working
	// directory (the invoking module). Batching is safe here because they all
	// share a single module context.
	if len(importPaths) > 0 {
		cfg := &packages.Config{Mode: loadMode}
		pkgs, err := packages.Load(cfg, importPaths...)
		if err != nil {
			return nil, fmt.Errorf("failed to load import paths %v: %w", importPaths, err)
		}
		if n := collectPackageErrors(pkgs); n > 0 {
			// Use the structured error formatter to attach "go get" hints.
			return nil, formatImportPathErrors(pkgs)
		}

		for _, p := range pkgs {
			log.Info("loaded package", "name", p.Name, "path", p.PkgPath)
		}
		all = append(all, pkgs...)
	}

	return all, nil
}

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

// FindPkgByPath returns the first package in the full transitive closure of
// pkgs whose import path equals pkgPath, or nil if no such package is found.
//
// packages.Visit is used rather than a plain range over pkgs because with
// NeedDeps the top-level slice contains only the packages named on the command
// line; their dependencies — including go.resystems.io/eddt/runtime — are
// reachable only by walking the Imports graph. A plain range would miss them.
func FindPkgByPath(pkgs []*packages.Package, pkgPath string) *packages.Package {
	var found *packages.Package
	packages.Visit(pkgs, func(p *packages.Package) bool {
		if p.PkgPath == pkgPath {
			found = p
			// Return false to stop visiting this subtree; the outer loop in
			// packages.Visit continues with remaining top-level packages unless
			// we also signal completion. We rely on the nil check in the post
			// function to halt early once found is set.
			return false
		}
		// Continue visiting children only if we have not found the target yet.
		return found == nil
	}, nil)
	return found
}
