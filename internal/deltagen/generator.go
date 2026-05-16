// Package deltagen implements the delta-gen code generator. It reads an EDDT
// Snapshot struct annotated with eddt:"entity.key" and delta.* struct tags and
// emits the companion Delta type together with Apply, Diff, Coalesce, and
// EntityID methods.
//
// The generator pipeline has three stages, each delivered by its own item in
// the implementation plan:
//
//   - Load  (G-02): resolve the input package(s) with golang.org/x/tools/go/packages.
//   - Parse (G-03/G-04): walk the loaded types to find the Snapshot struct, its
//     embedded runtime.Header, its entity.key field, and its payload fields.
//   - Emit  (Phase 4): render the Delta type and method bodies via text/template.
package deltagen

import "fmt"

// Generator holds the configuration for a single delta-gen invocation.
type Generator struct {
	// InputPkgs is the list of Go import paths or filesystem paths that contain
	// the target Snapshot struct. Corresponds to the --pkg flag.
	InputPkgs []string

	// TargetStructs is the list of Snapshot struct names to generate delta types
	// for. Corresponds to the --structs flag.
	TargetStructs []string

	// OutPath is the filesystem path of the file to write. Corresponds to --out.
	OutPath string

	// Verbose enables progress logging to stdout. Corresponds to --verbose.
	Verbose bool

	// PkgAliases is the raw list of "importpath=alias" mappings. Corresponds to
	// the --pkg-alias flag. Parsed into a map by the emit stage.
	PkgAliases []string

	// Version is the short VCS revision embedded in the generated file header.
	// Set by the caller (cmd/delta-gen) from debug.BuildInfo; may be empty.
	Version string
}

// NewGenerator constructs a Generator with the supplied parameters.
func NewGenerator(inputPkgs, targetStructs []string, outPath string, verbose bool, pkgAliases []string) *Generator {
	return &Generator{
		InputPkgs:     inputPkgs,
		TargetStructs: targetStructs,
		OutPath:       outPath,
		Verbose:       verbose,
		PkgAliases:    pkgAliases,
	}
}

// Run executes the full generation pipeline for each target struct.
//
// The pipeline has three stages; each is implemented in its own file:
//
//   - Load  (load.go, G-02):   resolve --pkg arguments into type-checked packages.
//   - Parse (parse.go, G-03/G-04): find the Snapshot struct, its embedded
//     runtime.Header, its entity.key field, and classify payload fields.
//   - Emit  (template.go, Phase 4): render the Delta type and Apply / Diff /
//     Coalesce / EntityID method bodies via text/template.
func (g *Generator) Run(outPkgNameOverride string) error {
	// Stage 1 — Load: resolve all --pkg arguments into *packages.Package values.
	// Filesystem paths and Go import paths are handled separately; see load.go
	// for the two-phase loading strategy and the rationale for NeedDeps.
	pkgs, err := loadPackages(g.InputPkgs, g.Verbose)
	if err != nil {
		return err
	}

	if g.Verbose {
		fmt.Printf("Loaded %d top-level package(s)\n", len(pkgs))
	}

	// Stage 2 — Parse: not yet implemented (G-03 / G-04).
	_ = pkgs
	return fmt.Errorf("delta-gen: parse stage not yet implemented")
}
