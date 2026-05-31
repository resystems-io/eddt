package deltagen

// template_standalone.go implements the standalone-mode code-emission stage.
//
// Standalone mode (--standalone) generates runtime-independent delta code:
// Snapshot structs need not embed runtime.Header, Apply/Diff/Coalesce are pure
// functions with no error return, and no symbols from go.resystems.io/eddt/runtime
// appear in the generated file. A companion delta_types.go (or custom name) is
// emitted alongside the *_delta.go with local equivalents of EntityID, FieldDelta[T],
// FieldDeltaOp, and the hash helpers needed by EntityID().
//
// # Integration with the existing template pipeline
//
// The standalone sub-templates are parsed into the shared deltaTemplate object
// at package init time (via init()). This means all sub-templates defined in
// template.go (nestedTypeDecl, applyField, diffField, fieldDeclsRange, mapWrapper,
// sliceWrapper, etc.) are visible to and reused by the standaloneMain template.
// The only new templates defined here are those that differ in the standalone mode:
// the top-level standaloneMain body and the Apply/Diff/Coalesce/EntityID function
// and method wrapper variants.
//
// # Companion file
//
// emitStandaloneTypes writes the companion local-types file. Its content is
// controlled by standaloneTypesTemplateStr, which is selected based on the
// --standalone-hash flag (blake2b or sha256). The file is only written when
// --standalone is active and the destination file either does not exist or
// already carries a "DO NOT EDIT" marker from a prior run.

import (
	"bytes"
	_ "embed"
	"fmt"
	"os"
	"path/filepath"
	"text/template"
)

// ── Standalone delta-file templates ──────────────────────────────────────────

// standaloneTemplateFS holds the additional named sub-templates for standalone
// mode, loaded from the sibling template_standalone.go.tmpl file via //go:embed.
// These are parsed into deltaTemplate at init time, making them available
// via ExecuteTemplate("standaloneMain", ...).
//
// The standaloneMain template mirrors the structure of the root "delta" template
// in template.go but differs in three ways:
//  1. The TDelta struct omits the runtime.Header embedding.
//  2. Apply, Diff, Coalesce, and EntityID use standalone variants (pure functions,
//     no error return, local EntityID type).
//  3. The import block is conditional to handle the empty-imports case (no
//     runtime import, and some simple snapshots may have no foreign types at all).
//
//go:embed template_standalone.go.tmpl
var standaloneTemplateFS string

// init registers the standalone sub-templates with the shared deltaTemplate so
// ExecuteTemplate("standaloneMain", ...) and the standalone function sub-templates
// are available at generation time.
func init() {
	template.Must(deltaTemplate.Parse(standaloneTemplateFS))
}

// ── Companion local-types file ────────────────────────────────────────────────

// standaloneTypesData is the view type for the companion delta_types.go template.
type standaloneTypesData struct {
	Version     string
	PackageName string
	UseSHA256   bool // true when --standalone-hash sha256
}

// standaloneTypesBlake2bFS is the companion file template for blake2b mode,
// loaded from template_standalone_blake2b.go.tmpl via //go:embed.
// The produced EntityID values are byte-identical to runtime.EntityID for the
// same key fields (same algorithm, same encoding).
//
//go:embed template_standalone_blake2b.go.tmpl
var standaloneTypesBlake2bFS string

// standaloneTypesSHA256FS is the companion file template for sha256 mode,
// loaded from template_standalone_sha256.go.tmpl via //go:embed.
// NOTE: EntityID values produced here differ from eddt/runtime EntityID values.
//
//go:embed template_standalone_sha256.go.tmpl
var standaloneTypesSHA256FS string
var (
	standaloneTypesBlake2bTemplate = template.Must(
		template.New("standalone-types-blake2b").Parse(standaloneTypesBlake2bFS))
	standaloneTypesSHA256Template = template.Must(
		template.New("standalone-types-sha256").Parse(standaloneTypesSHA256FS))
)

// emitStandaloneTypes writes the companion local-types file (default delta_types.go)
// for standalone mode. It is called by Generator.emitStage after the *_delta.go
// file has been written. The output directory is derived from g.OutPath.
//
// If the target file already exists and does NOT carry a "DO NOT EDIT" header
// (indicating a hand-written file), the function warns and skips rather than
// overwriting. On subsequent generator runs the file is overwritten in place.
func emitStandaloneTypes(g *Generator) error {
	outDir := filepath.Dir(g.OutPath)
	typesName := g.StandaloneTypesFile
	if typesName == "" {
		typesName = "delta_types.go"
	}
	typesPath := filepath.Join(outDir, typesName)

	// Guard against overwriting hand-written files.
	if existing, err := os.ReadFile(typesPath); err == nil {
		if !bytes.Contains(existing, []byte("DO NOT EDIT")) {
			g.log().Warn("standalone-types: skipping — file exists without DO NOT EDIT marker",
				"path", typesPath)
			return nil
		}
	}

	data := standaloneTypesData{
		Version:     g.Version,
		PackageName: g.OutPkgName,
		UseSHA256:   g.StandaloneHash == "sha256",
	}

	tmpl := standaloneTypesBlake2bTemplate
	if data.UseSHA256 {
		tmpl = standaloneTypesSHA256Template
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return fmt.Errorf("delta-gen: standalone types template execution failed: %w", err)
	}

	return writeFormattedGo(typesPath, &buf)
}
