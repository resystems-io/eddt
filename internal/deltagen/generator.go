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
// The pipeline stages and the items that implement them:
//
//   - Load:  G-02 (internal/deltagen/load.go)   — golang.org/x/tools/go/packages
//   - Parse: G-03 (internal/deltagen/parse.go)  — Snapshot / Header / payload fields
//           G-04 (internal/deltagen/parse.go)  — entity.key field + type validation
//   - Emit:  EM-01..EM-05 (internal/deltagen/template.go) — Delta type + methods
//
// Until those stages are implemented this function returns an error.
func (g *Generator) Run(outPkgNameOverride string) error {
	return fmt.Errorf("delta-gen: not yet implemented")
}
