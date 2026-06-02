// generator.go implements the Generator type, its configuration (Config), and
// the four-stage pipeline (load, resolve, parse, emit) that drives code generation.
package deltagen

import (
	"log/slog"
	"os"
	"sync"

	"golang.org/x/tools/go/packages"
)

// Config holds every caller-supplied input for a single delta-gen invocation.
// Pass it to New to obtain a Generator ready to Run. Derived state (OutPkgName,
// CrossPackage) is computed by Run and is not part of Config.
type Config struct {
	// InputPkgs is the list of Go import paths or filesystem paths that contain
	// the target Snapshot struct. Corresponds to the --pkg flag.
	InputPkgs []string

	// TargetStructs is the list of Snapshot struct names to generate delta types
	// for. Corresponds to the --structs flag.
	TargetStructs []string

	// OutPath is the filesystem path of the file to write. Corresponds to --out.
	OutPath string

	// PkgAliases is the raw list of "importpath=alias" mappings. Corresponds to
	// the --pkg-alias flag. Parsed into a map by the emit stage.
	PkgAliases []string

	// Version is the short VCS revision embedded in the generated file header.
	// Set from debug.BuildInfo by the CLI layer; may be empty.
	Version string

	// KeyFields maps Snapshot struct names to the field name that identifies
	// the entity-key field, bypassing the eddt:"entity.key" tag scan. An absent
	// or empty entry selects tag-based discovery for that struct.
	KeyFields map[string]string

	// Log is the structured logger for progress and warning output. When nil,
	// a package-level default (Warn level, text handler, stderr) is used.
	Log *slog.Logger

	// OutPkgNameOverride is the caller-supplied --pkg-name value. When non-empty
	// it overrides the auto-detected source package name in the output file.
	OutPkgNameOverride string

	// Standalone enables runtime-independent code generation. When true:
	//   - Snapshot structs may omit the embedded runtime.Header field.
	//   - Generated *_delta.go files import nothing from go.resystems.io/eddt/runtime.
	//   - Apply, Diff, and Coalesce are pure functions with no error return.
	//   - A companion file (StandaloneTypesFile) is emitted with local
	//     equivalents of EntityID, FieldDelta[T], FieldDeltaOp, and hash helpers.
	// Mutually exclusive with embedding runtime.Header in any target Snapshot.
	Standalone bool

	// StandaloneHash selects the EntityID hash algorithm for standalone mode.
	// Accepted values: "blake2b" (default — blake2b-256 via golang.org/x/crypto,
	// EntityID-compatible with eddt/runtime) and "sha256" (stdlib crypto/sha256,
	// different EntityID values). Ignored when Standalone is false.
	StandaloneHash string

	// StandaloneTypesFile is the filename of the generated companion local-types
	// file (default "delta_types.go"). Resolved relative to the output file's
	// directory. Ignored when Standalone is false.
	StandaloneTypesFile string
}

// Generator holds the configuration for a single delta-gen invocation.
type Generator struct {
	// Input fields — set from Config by New.
	InputPkgs           []string
	TargetStructs       []string
	OutPath             string
	PkgAliases          []string
	Version             string
	KeyFields           map[string]string
	Log                 *slog.Logger
	OutPkgNameOverride  string
	Standalone          bool
	StandaloneHash      string
	StandaloneTypesFile string

	// Derived state — populated by Run after the load/resolve stages.

	// OutPkgName is the resolved output package name (from OutPkgNameOverride
	// or auto-detected from the first source package).
	OutPkgName string

	// CrossPackage is true when OutPkgName differs from the source package name.
	// When true, the parse stage excludes unexported fields and the emit stage
	// omits ergonomic method wrappers (R-DG-012, R-DG-013, R-DG-019).
	CrossPackage bool
}

// New constructs a Generator from the supplied Config. Input fields are copied
// from cfg; derived state (OutPkgName, CrossPackage) is left zero and populated
// by Run.
func New(cfg Config) *Generator {
	return &Generator{
		InputPkgs:           cfg.InputPkgs,
		TargetStructs:       cfg.TargetStructs,
		OutPath:             cfg.OutPath,
		PkgAliases:          cfg.PkgAliases,
		Version:             cfg.Version,
		KeyFields:           cfg.KeyFields,
		Log:                 cfg.Log,
		OutPkgNameOverride:  cfg.OutPkgNameOverride,
		Standalone:          cfg.Standalone,
		StandaloneHash:      cfg.StandaloneHash,
		StandaloneTypesFile: cfg.StandaloneTypesFile,
	}
}

// defaultLog is the fallback logger: Warn level, text handler, stderr.
var defaultLog = sync.OnceValue(func() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
})

// log returns g.Log when set, otherwise the package-level default.
func (g *Generator) log() *slog.Logger {
	if g.Log != nil {
		return g.Log
	}
	return defaultLog()
}

// Run executes the full generation pipeline for each target struct using the
// Config fields set by New. OutPkgNameOverride drives the resolve stage;
// OutPkgName and CrossPackage are written to the Generator on return.
//
// The pipeline has four stages; each is implemented in its own file:
//
//   - Load    (load.go):    resolve --pkg arguments into type-checked packages.
//   - Resolve (load.go):    determine output package name and cross-package mode.
//   - Parse   (parse.go):   a single parseSnapshot call per target struct
//     identifies the embedded runtime.Header, the entity.key field, and
//     classifies payload fields. The key is surfaced via ParsedSnapshot.KeyVar
//     and excluded from Fields. Per-struct key-field overrides are carried via
//     KeyFields.
//   - Tag    (tag.go):      parse and validate eddt: tag values.
//   - Emit    (template.go): render the TDelta struct (R-DG-015) and the
//     Apply/Diff/Coalesce/EntityID function bodies.
func (g *Generator) Run() error {
	pkgs, err := g.loadStage()
	if err != nil {
		return err
	}

	g.resolveStage(pkgs)

	snapshots, err := g.parseStage(pkgs)
	if err != nil {
		return err
	}

	return g.emitStage(snapshots)
}

// loadStage resolves all --pkg arguments into type-checked *packages.Package
// values using the two-phase loading strategy in load.go.
func (g *Generator) loadStage() ([]*packages.Package, error) {
	pkgs, err := loadPackages(g.InputPkgs, g.log())
	if err != nil {
		return nil, err
	}
	g.log().Info("loaded packages", "count", len(pkgs))
	return pkgs, nil
}

// resolveStage determines the output package name and cross-package mode.
// CrossPackage is true when --pkg-name differs from the source package name,
// OR when any source package has an explicit --pkg-alias entry. The second
// condition handles the "same short name, different import path" scenario (e.g.
// source go.example.com/foo/mme and output package both named "mme"), where
// the alias is definitive evidence that the packages are distinct.
// Downstream stages use CrossPackage to exclude unexported fields and omit
// method wrappers.
func (g *Generator) resolveStage(pkgs []*packages.Package) {
	g.OutPkgName, g.CrossPackage = resolveOutputPkg(pkgs, g.OutPkgNameOverride)

	// Promote to cross-package if any source package has an explicit alias.
	// sourceHasExplicitAlias is in load.go (same package).
	if !g.CrossPackage && sourceHasExplicitAlias(pkgs, g.PkgAliases) {
		g.CrossPackage = true
	}

	if g.CrossPackage {
		g.log().Info("cross-package mode", "output_pkg", g.OutPkgName, "source_pkg", pkgs[0].Name)
	} else {
		g.log().Info("output package", "name", g.OutPkgName)
	}
}

// parseStage resolves each target struct into a ParsedSnapshot. KeyFieldOverride
// is populated from g.KeyFields per struct; an absent entry selects tag-based
// discovery. A Warn is emitted when a CLI override supersedes an entity.key tag.
func (g *Generator) parseStage(pkgs []*packages.Package) ([]*ParsedSnapshot, error) {
	snapshots := make([]*ParsedSnapshot, 0, len(g.TargetStructs))
	for _, structName := range g.TargetStructs {
		opts := ParseOpts{
			CrossPackage:     g.CrossPackage,
			KeyFieldOverride: g.KeyFields[structName],
			Standalone:       g.Standalone,
		}
		ps, err := parseSnapshot(pkgs, structName, opts)
		if err != nil {
			return nil, err
		}

		// Conflict warning: when --key-field overrides a tagged entity.key field,
		// the tagged field falls back into ps.Fields (R-DG-010). Detect and
		// warn unconditionally so the Snapshot author is informed.
		if g.KeyFields[structName] != "" {
			for _, f := range ps.Fields {
				if f.Tag.Kind == TagKindEntityKey {
					g.log().Warn("key-field override supersedes entity.key tag",
						"struct", structName,
						"override", g.KeyFields[structName],
						"tag_field", f.Name)
					break
				}
			}
		}

		g.log().Info("parsed struct", "name", structName)
		snapshots = append(snapshots, ps)
	}
	return snapshots, nil
}

// emitStage renders the Delta type and associated functions for each snapshot.
// R-DG-015 emits the TDelta struct (embedded runtime.Header + per-field atomic
// Set<Name> declarations) via the text/template pipeline in template.go.
// Apply, Diff, Coalesce, and EntityID bodies land in R-DG-012, R-DG-013, R-DG-014, R-DG-034.
// In standalone mode, the companion local-types file is also emitted after the
// delta file (see template_standalone.go).
func (g *Generator) emitStage(snapshots []*ParsedSnapshot) error {
	if err := executeEmit(snapshots, g); err != nil {
		return err
	}
	if g.Standalone {
		return emitStandaloneTypes(g)
	}
	return nil
}
