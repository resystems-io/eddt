// Package deltagen implements the delta-gen code generator. It reads an EDDT
// Snapshot struct annotated with eddt:"entity.key" and delta.* struct tags and
// emits the companion Delta type together with the package-level functions
// Apply, Diff, Coalesce, and EntityID. When the output package matches the
// source package, optional ergonomic method wrappers are also emitted (E-12).
//
// The generator pipeline has four stages, each delivered by its own item in
// the implementation plan:
//
//   - Load    (G-02): resolve the input package(s) with golang.org/x/tools/go/packages.
//   - Resolve (G-05): determine the output package name and cross-package mode.
//   - Parse   (G-03 / G-07 / G-04): walk the loaded types to find the Snapshot
//     struct, its embedded runtime.Header, its entity.key field, and its
//     payload fields. Driven by a single `parseSnapshot(pkgs, name, ParseOpts{...})`
//     call per target struct; the entity-key field is surfaced via
//     ParsedSnapshot.KeyVar and excluded from Fields.
//   - Tag    (Phase 3): parse and validate eddt: tag values on payload fields.
//   - Emit    (Phase 4): render the Delta type and function bodies via text/template.
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

	// OutPkgName is the resolved output package name — from --pkg-name when
	// supplied, otherwise auto-detected from the first source package. Set by
	// Run after the load stage; zero-value before Run is called.
	OutPkgName string

	// CrossPackage is true when OutPkgName differs from the source package name.
	// When true, the parse stage excludes unexported fields and the emit stage
	// omits ergonomic method wrappers (E-12). Set by Run after the load stage.
	CrossPackage bool
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
// The pipeline has four stages; each is implemented in its own file:
//
//   - Load    (load.go, G-02):           resolve --pkg arguments into type-checked packages.
//   - Resolve (load.go, G-05):           determine output package name and cross-package mode.
//   - Parse   (parse.go, G-03 / G-07 / G-04):
//     a single `parseSnapshot(pkgs, name, ParseOpts{...})` call per target
//     struct identifies the embedded runtime.Header, the entity.key field,
//     and classifies payload fields. The key is surfaced via
//     ParsedSnapshot.KeyVar and excluded from Fields.
//   - Tag    (tag.go, Phase 3):          parse and validate eddt: tag values.
//   - Emit    (template.go, Phase 4):    render the Delta type and Apply / Diff /
//     Coalesce / EntityID function bodies via text/template; emit method wrappers
//     when CrossPackage is false.
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

	// Stage 1.5 — Resolve: determine output package name and cross-package mode.
	// CrossPackage is true when --pkg-name differs from the source package name;
	// downstream stages use it to exclude unexported fields and omit method wrappers.
	g.OutPkgName, g.CrossPackage = resolveOutputPkg(pkgs, outPkgNameOverride)
	if g.Verbose {
		if g.CrossPackage {
			fmt.Printf("Cross-package mode: output %q differs from source %q\n",
				g.OutPkgName, pkgs[0].Name)
		} else {
			fmt.Printf("Output package: %q\n", g.OutPkgName)
		}
	}

	// Stage 2 — Parse: resolve each target struct into a ParsedSnapshot
	// (G-03 / G-07 / G-04). The ParsedSnapshot carries HeaderVar, KeyVar,
	// and the payload Fields ready for tag handling and emission.
	opts := ParseOpts{
		CrossPackage: g.CrossPackage,
		// KeyFieldOverride is populated by G-06 from g.KeyFields[structName]
		// in a later refinement; the empty value selects tag-based discovery.
	}
	for _, structName := range g.TargetStructs {
		_, err := parseSnapshot(pkgs, structName, opts)
		if err != nil {
			return err
		}
		if g.Verbose {
			fmt.Printf("Parsed struct %q\n", structName)
		}
	}

	// Stage 3 — Tag handling (Phase 3): not yet implemented.
	return fmt.Errorf("delta-gen: tag parser not yet implemented")
}
