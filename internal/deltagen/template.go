package deltagen

// template.go implements the code-emission stage (Phase 4) for delta-gen.
// It provides text/template-driven generation of the TDelta companion struct
// (EM-01) and, in later Phase-4 items, the Apply, Diff, Coalesce, and
// EntityID function bodies (EM-02..EM-05).
//
// # Architecture
//
// View construction is separated from template execution:
//
//   - templateData / snapshotView / fieldView / importSpec are stable view
//     types shared by all Phase-4 items.
//   - buildImports constructs the import set and a types.Qualifier closure;
//     the qualifier side-effects the import set as types.TypeString encounters
//     foreign-package references during view construction.
//   - buildSnapshotView translates one ParsedSnapshot into a snapshotView;
//     it applies suppression (omit/retired) and the Phase-5 sentinel
//     (delta.nested) before rendering each field's Delta-side type via
//     types.TypeString.
//   - executeEmit orchestrates build → execute → go/format → WriteFile,
//     called by generator.go's emitStage.
//
// # Extending for EM-02..EM-05
//
// Add new fields to fieldView and snapshotView, new named sub-templates under
// deltaTemplateStr, and new rendering logic in buildSnapshotView.  The
// templateData shape, buildImports, and executeEmit pipeline are intended to
// remain stable across all Phase-4 items.

import (
	"bytes"
	"fmt"
	"go/format"
	"go/types"
	"os"
	"sort"
	"strings"
	"text/template"
)

// runtimeImportPath is the canonical import path for go.resystems.io/eddt/runtime.
// Always included in the generated file's import block (embedded Header).
const runtimeImportPath = "go.resystems.io/eddt/runtime"

// ── View types ────────────────────────────────────────────────────────────────

// templateData is the top-level input to the delta template. Fields are stable
// across EM-01..EM-05 so sub-templates added in later items can reuse them.
type templateData struct {
	// Version is the CLI --version string embedded in the generated file header.
	Version string

	// PackageName is the resolved output package name (e.g. "model" or "deltas").
	PackageName string

	// Imports is the ordered set of import specs; runtime is always first,
	// followed by additional packages sorted by path.
	Imports []importSpec

	// Snapshots holds one view per --structs entry in declaration order.
	Snapshots []snapshotView
}

// importSpec describes one entry in the generated import block.
type importSpec struct {
	// Alias is the local import alias (e.g. "eddt"); empty means use the
	// package's own name (Go's default behaviour).
	Alias string

	// Path is the full import path (e.g. "go.resystems.io/eddt/runtime").
	Path string
}

// snapshotView is the template's per-Snapshot view.
type snapshotView struct {
	// Name is the source struct name (e.g. "UESessionSnapshot").
	Name string

	// DeltaName is the emitted companion type name (e.g. "UESessionSnapshotDelta").
	DeltaName string

	// Qualifier is the package-qualifier prefix used in doc comments when the
	// generated file is in a different package than the source (e.g. "model.").
	// Empty for same-package output.
	Qualifier string

	// KeyName is the source field name of the entity-key field (ps.KeyVar.Name()).
	// Used in Apply to emit "result.<KeyName> = s.<KeyName>" (EM-02).
	KeyName string

	// EmitMethod is true when the output package matches the source package
	// (E-12). When true, the template emits a same-package method wrapper that
	// delegates to the package-level Apply function (EM-02).
	EmitMethod bool

	// Fields is the ordered list of payload fields in source declaration order
	// (excluding the entity-key field extracted into KeyName). Suppressed fields
	// (delta.omit / delta.retired) are included with Suppressed: true so the
	// Apply template can emit result.F = s.F propagation assignments (EM-02).
	Fields []fieldView
}

// fieldView is the template's per-field view of one payload field in TDelta.
type fieldView struct {
	// Name is the source field name (e.g. "Address").
	Name string

	// DeltaName is the emitted Delta-side field name (e.g. "SetAddress").
	// Empty when Suppressed is true.
	DeltaName string

	// DeltaType is the rendered Go type string (e.g. "*string", "**int32").
	// Empty when Suppressed is true.
	DeltaType string

	// Suppressed is true for delta.omit and delta.retired fields. The
	// Delta-side field is absent from TDelta but Apply still propagates the
	// source value via result.F = s.F (EM-02).
	Suppressed bool
}

// ── Template ─────────────────────────────────────────────────────────────────

// deltaTemplateStr is the text/template source for the generated Delta file.
// EM-02 scope: type declarations (EM-01) + Apply function and method wrapper.
// Named sub-templates for Diff, Coalesce, and EntityID are added by EM-03..EM-05.
//
// Sub-template inventory:
//   - applyFunc:  package-level Apply function (always emitted).
//   - applyField: per-field Apply contribution (atomic or suppressed).
//   - applyMethod: same-package method wrapper delegating to Apply (E-12).
//
// The dict FuncMap helper enables multi-value pipelines to sub-templates
// (writer-gen pattern); it is registered up-front so later items do not need
// to re-thread the template wiring.
const deltaTemplateStr = `// Code generated by delta-gen{{if .Version}} ({{.Version}}){{end}}. DO NOT EDIT.
package {{.PackageName}}

import (
{{- range .Imports}}
	{{if .Alias}}{{.Alias}} {{end}}"{{.Path}}"
{{- end}}
)
{{range .Snapshots}}
// {{.DeltaName}} is the Delta companion type for {{.Qualifier}}{{.Name}}.
// Each payload field is expressed as an optional pointer: a nil value means
// "no change" for that field when Apply is called.
type {{.DeltaName}} struct {
	runtime.Header
{{- range .Fields}}{{if not .Suppressed}}
	{{.DeltaName}} {{.DeltaType}}
{{- end}}{{end}}
}
{{template "applyFunc" .}}
{{if .EmitMethod}}{{template "applyMethod" .}}
{{end}}{{end -}}

{{define "applyFunc"}}
// Apply produces the Snapshot that results from applying d to s.
// It is a pure function; chain-envelope validations live in
// runtime.HeaderAfterApply and are propagated to the caller as a
// non-nil error. See delta-gen-spec.md §6.4 / §7.1 (Errata E-19).
func Apply(s {{.Qualifier}}{{.Name}}, d {{.DeltaName}}) ({{.Qualifier}}{{.Name}}, error) {
	var result {{.Qualifier}}{{.Name}}
	hdr, err := runtime.HeaderAfterApply(s.Header, d.Header)
	if err != nil {
		return result, err
	}
	result.Header = hdr
	result.{{.KeyName}} = s.{{.KeyName}}
{{range .Fields}}	{{template "applyField" .}}
{{end}}	return result, nil
}
{{end -}}

{{define "applyField"}}{{if .Suppressed}}result.{{.Name}} = s.{{.Name}}{{else}}if d.{{.DeltaName}} != nil { result.{{.Name}} = *d.{{.DeltaName}} } else { result.{{.Name}} = s.{{.Name}} }{{end}}{{end -}}

{{define "applyMethod"}}
// Apply is an ergonomic same-package wrapper that delegates to the
// package-level Apply function (E-12).
func (s {{.Name}}) Apply(d {{.DeltaName}}) ({{.Name}}, error) {
	return Apply(s, d)
}
{{end}}`

// deltaTemplate is the parsed and compiled template; compiled once at init.
var deltaTemplate = template.Must(
	template.New("delta").
		Funcs(template.FuncMap{
			// dict constructs a map[string]any from alternating key/value pairs,
			// enabling multi-argument pipelines into named sub-templates.
			"dict": func(pairs ...any) (map[string]any, error) {
				if len(pairs)%2 != 0 {
					return nil, fmt.Errorf("dict: odd number of arguments")
				}
				m := make(map[string]any, len(pairs)/2)
				for i := 0; i < len(pairs); i += 2 {
					key, ok := pairs[i].(string)
					if !ok {
						return nil, fmt.Errorf("dict: key at position %d is not a string", i)
					}
					m[key] = pairs[i+1]
				}
				return m, nil
			},
		}).
		Parse(deltaTemplateStr),
)

// ── Emit options ──────────────────────────────────────────────────────────────

// emitOpts groups options derived from Generator fields that are needed during
// view construction and import resolution.
type emitOpts struct {
	// crossPackage is true when the output package differs from the source
	// package (E-12), requiring type-reference qualification.
	crossPackage bool

	// aliases maps import path → caller-supplied local alias (from --pkg-alias).
	aliases map[string]string
}

// parsePkgAliases converts the raw --pkg-alias "importpath=alias" slice to a
// map.  Entries without "=" are silently skipped (the load stage validates the
// format; by the time emit runs the flag has already been accepted).
func parsePkgAliases(raw []string) map[string]string {
	m := make(map[string]string, len(raw))
	for _, entry := range raw {
		idx := strings.Index(entry, "=")
		if idx < 0 {
			continue
		}
		m[entry[:idx]] = entry[idx+1:]
	}
	return m
}

// ── Import / qualifier construction ──────────────────────────────────────────

// buildImports returns a types.Qualifier closure and a lazy import-list getter.
//
// The qualifier is a side-effecting function: every foreign *types.Package that
// types.TypeString encounters while rendering a field type is recorded in an
// internal map.  The caller must complete all type-string rendering (i.e. call
// buildSnapshotView for every snapshot) before calling getImports(), so that
// the full set of required imports is captured.
//
// The runtime package is pre-seeded (always required for the embedded Header).
// In cross-package mode the source packages are also pre-seeded.
func buildImports(
	snapshots []*ParsedSnapshot,
	opts emitOpts,
) (qualifier types.Qualifier, getImports func() []importSpec) {
	// recorded maps import-path → importSpec; populated eagerly for runtime and
	// cross-pkg sources, and lazily by the qualifier closure for foreign types.
	recorded := map[string]importSpec{
		runtimeImportPath: {Path: runtimeImportPath},
	}

	// In cross-package mode pre-seed the source package import(s).
	if opts.crossPackage {
		for _, ps := range snapshots {
			alias := opts.aliases[ps.PkgPath]
			recorded[ps.PkgPath] = importSpec{Alias: alias, Path: ps.PkgPath}
		}
	}

	// localPkgPath identifies the source package (same for all snapshots in a
	// single generator run; needed to suppress the qualifier for local types).
	localPkgPath := ""
	if len(snapshots) > 0 {
		localPkgPath = snapshots[0].PkgPath
	}

	qual := func(pkg *types.Package) string {
		path := pkg.Path()

		// Same-package types need no qualifier.
		if !opts.crossPackage && path == localPkgPath {
			return ""
		}

		// runtime uses its own package name; it is pre-seeded, not re-recorded.
		if path == runtimeImportPath {
			return "runtime"
		}

		// Use caller-supplied alias if provided; otherwise the package short name.
		alias := opts.aliases[path]
		name := alias
		if name == "" {
			name = pkg.Name()
		}

		// Record the import so getImports() can include it.
		recorded[path] = importSpec{Alias: alias, Path: path}
		return name
	}

	getImports = func() []importSpec {
		list := make([]importSpec, 0, len(recorded))
		for _, spec := range recorded {
			list = append(list, spec)
		}
		// Deterministic order: runtime first, then alphabetical by path.
		sort.Slice(list, func(i, j int) bool {
			if list[i].Path == runtimeImportPath {
				return true
			}
			if list[j].Path == runtimeImportPath {
				return false
			}
			return list[i].Path < list[j].Path
		})
		return list
	}

	return qual, getImports
}

// ── View construction ─────────────────────────────────────────────────────────

// buildSnapshotView constructs the template view for one ParsedSnapshot.
//
// delta.nested on any shape returns an explicit error directing the caller
// to Phase 5. (delta.clearable is already rejected upstream by T-01.)
//
// Suppressed fields (delta.omit / delta.retired) are included in sv.Fields
// with Suppressed: true so the Apply template can emit result.F = s.F
// propagation assignments (EM-02). The Delta-side type declaration template
// gates on {{not .Suppressed}} to keep them out of TDelta.
//
// Each admitted field's DeltaType is rendered via types.TypeString on a
// single pointer-wrap of the source GoType:
//
//	scalar T        → *T
//	pointer *T      → **T
//	struct value T  → *T
//	slice []T       → *[]T      (atomic per E-15)
//	map[K]V         → *map[K]V  (atomic per E-16)
//
// The caller must pass a qualifier derived from buildImports so that foreign
// packages are recorded as a side effect of type rendering.
func buildSnapshotView(ps *ParsedSnapshot, qualifier types.Qualifier) (snapshotView, error) {
	sv := snapshotView{
		Name:      ps.Name,
		DeltaName: ps.Name + "Delta",
		KeyName:   ps.KeyVar.Name(),
	}

	for _, f := range ps.Fields {
		// Phase-5 sentinel: delta.nested requires compositional emission (N-01/N-03/N-04).
		if f.Tag.Kind == TagKindNested {
			return snapshotView{}, fmt.Errorf(
				"field %s.%s: delta.nested emission is not yet implemented (Phase 5)",
				ps.Name, f.Name)
		}

		// Presence-axis: omit/retired fields are suppressed on the Delta side
		// but still appear in Fields so the Apply template emits result.F = s.F.
		if f.Tag.Kind == TagKindOmit || f.Tag.Kind == TagKindRetired {
			sv.Fields = append(sv.Fields, fieldView{Name: f.Name, Suppressed: true})
			continue
		}

		// TagKindNone and TagKindCommutative both emit as atomic (§9.5).
		// Render the Delta-side type as *<GoType>; one pointer wrap covers all shapes.
		deltaType := types.TypeString(types.NewPointer(f.GoType), qualifier)

		sv.Fields = append(sv.Fields, fieldView{
			Name:      f.Name,
			DeltaName: "Set" + f.Name,
			DeltaType: deltaType,
		})
	}

	return sv, nil
}

// ── Emit orchestration ────────────────────────────────────────────────────────

// executeEmit runs the full Phase-4 emit pipeline:
//
//  1. Parse --pkg-alias entries and derive emitOpts.
//  2. Build the qualifier / import-recorder via buildImports.
//  3. Translate each ParsedSnapshot to a snapshotView via buildSnapshotView
//     (this side-effects the qualifier to record foreign packages).
//  4. Materialise the import list via getImports().
//  5. Execute deltaTemplate into a buffer.
//  6. Format the buffer with go/format.Source (syntax errors include the raw
//     source for debuggability, mirroring the arrow-writer-gen pattern).
//  7. Write the formatted result to g.OutPath.
func executeEmit(snapshots []*ParsedSnapshot, g *Generator) error {
	opts := emitOpts{
		crossPackage: g.CrossPackage,
		aliases:      parsePkgAliases(g.PkgAliases),
	}

	// Step 2: build the qualifier and import-recorder.
	qualifier, getImports := buildImports(snapshots, opts)

	// Step 3: translate each snapshot into a template view.
	views := make([]snapshotView, 0, len(snapshots))
	for _, ps := range snapshots {
		sv, err := buildSnapshotView(ps, qualifier)
		if err != nil {
			return err
		}

		// Set the Qualifier for the doc comment in cross-package mode.
		if g.CrossPackage {
			sv.Qualifier = ps.PkgName + "."
			if alias := opts.aliases[ps.PkgPath]; alias != "" {
				sv.Qualifier = alias + "."
			}
		}

		// EmitMethod gates the same-package method wrapper (E-12, EM-02).
		sv.EmitMethod = !g.CrossPackage

		views = append(views, sv)
	}

	// Step 4: materialise imports after all type strings have been rendered.
	data := templateData{
		Version:     g.Version,
		PackageName: g.OutPkgName,
		Imports:     getImports(),
		Snapshots:   views,
	}

	// Step 5: execute template.
	var buf bytes.Buffer
	if err := deltaTemplate.Execute(&buf, data); err != nil {
		return fmt.Errorf("delta-gen: template execution failed: %w", err)
	}

	// Step 6: format the generated source; wrap errors with raw output for debugging.
	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		return fmt.Errorf(
			"delta-gen: generated source is not valid Go: %w\n--- raw source ---\n%s",
			err, buf.String())
	}

	// Step 7: write to the output file.
	if err := os.WriteFile(g.OutPath, formatted, 0644); err != nil {
		return fmt.Errorf("delta-gen: writing output file %q: %w", g.OutPath, err)
	}

	return nil
}
