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
//     foreign-package references during view construction. The returned
//     recordExtra closure allows callers to inject additional imports (e.g.
//     "reflect") after view construction is complete.
//   - buildSnapshotView translates one ParsedSnapshot into a snapshotView;
//     it applies suppression (omit/retired) and the Phase-5 sentinel
//     (delta.nested) before rendering each field's Delta-side type via
//     types.TypeString. Sets UseReflectEq per field and NeedsReflect per
//     snapshot for the conditional reflect-import logic (EM-03).
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
	"reflect"
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

// nestedTypeView is the template's view for one delta.nested companion type.
// Emitted before the parent snapshotView's TDelta declaration.
type nestedTypeView struct {
	// Name is the source type name (e.g. "Inner").
	Name string

	// DeltaName is the companion type name (e.g. "InnerDelta").
	DeltaName string

	// ApplyFuncName is the package-level function name (e.g. "ApplyInner").
	ApplyFuncName string

	// DiffFuncName is the package-level function name (e.g. "DiffInner").
	DiffFuncName string

	// Fields is the ordered list of payload fields (suppressed included).
	Fields []fieldView

	// DiffFields is the subset of Fields with a Delta-side representation.
	DiffFields []fieldView

	// NeedsReflect is true when at least one DiffField uses reflect.DeepEqual.
	// Propagated to snapshotView.NeedsReflect so the "reflect" import is injected.
	NeedsReflect bool

	// EmitMethod is true in same-package mode; gates method wrapper emission.
	EmitMethod bool
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

	// KeyTypeName is the bare (unqualified) type name of the entity-key field,
	// e.g. "UEKey", "IMSI", or "string" for a raw-basic key (EM-05).
	KeyTypeName string

	// KeyQualifier is the package-qualifier prefix for the key type in cross-
	// package mode (e.g. "model."). Empty in same-package mode or when the key
	// type is an unnamed basic (e.g. raw string). Set alongside Qualifier in
	// executeEmit (EM-05, E-12).
	KeyQualifier string

	// KeyHashLines is the ordered list of runtime.Write* call strings for the
	// EntityID function body (EM-05). One line for a scalar key; one line per
	// exported sub-field in source order for a struct key.
	KeyHashLines []string

	// EmitEntityIDMethod is true when the EntityID method wrapper should be
	// emitted on the key type: same-package mode AND the key type is a named
	// type (Go forbids methods on unnamed basic types). When false, only the
	// package-level EntityID function is emitted (EM-05, R-24, E-12).
	EmitEntityIDMethod bool

	// EmitMethod is true when the output package matches the source package
	// (E-12). When true, the template emits same-package method wrappers that
	// delegate to the package-level Apply, Diff, and Coalesce functions
	// (EM-02, EM-03, EM-04).
	EmitMethod bool

	// NeedsReflect is true when at least one DiffFields entry uses
	// reflect.DeepEqual for its comparison (EM-03). executeEmit uses this to
	// inject a "reflect" import only when needed.
	NeedsReflect bool

	// Fields is the ordered list of payload fields in source declaration order
	// (excluding the entity-key field extracted into KeyName). Suppressed fields
	// (delta.omit / delta.retired) are included with Suppressed: true so the
	// Apply template can emit result.F = s.F propagation assignments (EM-02).
	Fields []fieldView

	// DiffFields is the subset of Fields that have a Delta-side representation
	// (i.e. non-suppressed fields). The Diff template iterates DiffFields so
	// that suppressed fields produce no body line (EM-03).
	DiffFields []fieldView

	// NestedTypes holds companion views for delta.nested struct-value fields,
	// in bottom-up order (deepest companion type first). Emitted before the
	// parent TDelta declaration so forward references are avoided (N-01).
	NestedTypes []nestedTypeView
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
	// source value via result.F = s.F (EM-02). Suppressed fields are excluded
	// from DiffFields and therefore produce no Diff body line (EM-03).
	Suppressed bool

	// UseReflectEq is true when the Diff template must use reflect.DeepEqual
	// rather than != to compare this field's values (EM-03). Set for all
	// non-scalar shapes: pointer, struct value, slice, map.
	UseReflectEq bool

	// IsNested is true for delta.nested struct-value fields (N-01). When true,
	// DeltaName equals the source field name (no "Set" prefix), DeltaType is the
	// companion Delta type name (e.g. "InnerDelta"), and no pointer-wrap is used.
	IsNested bool

	// NestedFuncName is the package-level Apply function to call for cross-package
	// nested fields (e.g. "ApplyInner"). Empty in same-package mode, where the
	// method wrapper is used instead (s.F.Apply(d.F)).
	NestedFuncName string

	// NestedDiffFuncName is the package-level Diff function for cross-package
	// nested fields (e.g. "DiffInner"). Empty in same-package mode.
	NestedDiffFuncName string
}

// ── Template ─────────────────────────────────────────────────────────────────

// deltaTemplateStr is the text/template source for the generated Delta file.
// EM-02 scope: type declarations (EM-01) + Apply function and method wrapper.
// EM-03 scope: Diff function and method wrapper.
// EM-04 scope: Coalesce function and method wrapper.
// EM-05 scope: EntityID function and method wrapper on the key type.
// N-01 scope:  companion Delta types and Apply/Diff for delta.nested struct fields.
//
// Sub-template inventory:
//   - applyFunc:         package-level Apply function (always emitted).
//   - applyField:        per-field Apply contribution (atomic, suppressed, or nested).
//   - applyMethod:       same-package method wrapper delegating to Apply (E-12).
//   - diffFunc:          package-level Diff function (always emitted).
//   - diffField:         per-field Diff contribution (non-suppressed fields only).
//   - diffMethod:        same-package method wrapper delegating to Diff (E-12).
//   - coalesceFunc:      package-level Coalesce function (always emitted).
//   - coalesceMethod:    same-package method wrapper delegating to Coalesce (E-12).
//   - entityIDFunc:      package-level EntityID function (always emitted, EM-05).
//   - entityIDMethod:    same-package method wrapper on the key type (E-12, EM-05);
//     emitted only when the key type is a named type (EmitEntityIDMethod).
//   - nestedTypeDecl:    companion Delta struct for a delta.nested type (N-01).
//   - nestedApplyFunc:   package-level ApplyU function for a nested type (N-01).
//   - nestedApplyMethod: same-package method wrapper func (u U) Apply(...) (N-01).
//   - nestedApplyField:  per-field body line for nestedApplyFunc (uses u. receiver).
//   - nestedDiffFunc:    package-level DiffU function for a nested type (N-01).
//   - nestedDiffMethod:  same-package method wrapper func (u U) Diff(...) (N-01).
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
{{range .Snapshots}}{{range .NestedTypes}}{{template "nestedTypeDecl" .}}{{template "nestedApplyFunc" .}}{{if .EmitMethod}}{{template "nestedApplyMethod" .}}{{end}}{{template "nestedDiffFunc" .}}{{if .EmitMethod}}{{template "nestedDiffMethod" .}}{{end}}
{{end}}
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
{{end}}{{template "diffFunc" .}}
{{if .EmitMethod}}{{template "diffMethod" .}}
{{end}}{{template "coalesceFunc" .}}
{{if .EmitMethod}}{{template "coalesceMethod" .}}
{{end}}{{template "entityIDFunc" .}}
{{if .EmitEntityIDMethod}}{{template "entityIDMethod" .}}
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

{{define "applyField"}}{{if .Suppressed}}result.{{.Name}} = s.{{.Name}}{{else if .IsNested}}{{if .NestedFuncName}}result.{{.Name}} = {{.NestedFuncName}}(s.{{.Name}}, d.{{.DeltaName}}){{else}}result.{{.Name}} = s.{{.Name}}.Apply(d.{{.DeltaName}}){{end}}{{else}}if d.{{.DeltaName}} != nil { result.{{.Name}} = *d.{{.DeltaName}} } else { result.{{.Name}} = s.{{.Name}} }{{end}}{{end -}}

{{define "applyMethod"}}
// Apply is an ergonomic same-package wrapper that delegates to the
// package-level Apply function (E-12).
func (s {{.Name}}) Apply(d {{.DeltaName}}) ({{.Name}}, error) {
	return Apply(s, d)
}
{{end -}}

{{define "diffFunc"}}
// Diff produces the minimal Delta d such that Apply(a, d) payload-equals b.
// It is a pure function; chain-envelope validations live in
// runtime.HeaderForDiff and are propagated to the caller as a non-nil error.
// See delta-gen-spec.md §6.5 / §7.2 (Errata E-20).
func Diff(a, b {{.Qualifier}}{{.Name}}) ({{.DeltaName}}, error) {
	hdr, err := runtime.HeaderForDiff(a.Header, b.Header)
	if err != nil {
		return {{.DeltaName}}{}, err
	}
	d := {{.DeltaName}}{Header: hdr}
{{range .DiffFields}}	{{template "diffField" .}}
{{end}}	return d, nil
}
{{end -}}

{{define "diffField"}}{{if .IsNested}}{{if .NestedDiffFuncName}}d.{{.DeltaName}} = {{.NestedDiffFuncName}}(a.{{.Name}}, b.{{.Name}}){{else}}d.{{.DeltaName}} = a.{{.Name}}.Diff(b.{{.Name}}){{end}}{{else if .UseReflectEq}}if !reflect.DeepEqual(a.{{.Name}}, b.{{.Name}}) { d.{{.DeltaName}} = &b.{{.Name}} }{{else}}if a.{{.Name}} != b.{{.Name}} { d.{{.DeltaName}} = &b.{{.Name}} }{{end}}{{end -}}

{{define "diffMethod"}}
// Diff is an ergonomic same-package wrapper that delegates to the
// package-level Diff function (E-12).
func (a {{.Name}}) Diff(b {{.Name}}) ({{.DeltaName}}, error) {
	return Diff(a, b)
}
{{end -}}

{{define "coalesceFunc"}}
// Coalesce folds a slice of TDeltas into s by iterated Apply. It is a pure
// function; chain-envelope validations propagate from the first failing Apply
// step. An empty slice returns (s, nil) without any runtime call. See
// delta-gen-spec.md §7.3 / §8.3 (Errata E-21, E-19).
func Coalesce(s {{.Qualifier}}{{.Name}}, ds []{{.DeltaName}}) ({{.Qualifier}}{{.Name}}, error) {
	result := s
	for _, d := range ds {
		var err error
		result, err = Apply(result, d)
		if err != nil {
			return {{.Qualifier}}{{.Name}}{}, err
		}
	}
	return result, nil
}
{{end -}}

{{define "coalesceMethod"}}
// Coalesce is an ergonomic same-package wrapper that delegates to the
// package-level Coalesce function (E-12).
func (s {{.Name}}) Coalesce(ds []{{.DeltaName}}) ({{.Name}}, error) {
	return Coalesce(s, ds)
}
{{end -}}

{{define "entityIDFunc"}}
// EntityID returns the deterministic content-hash EntityID of k. The hash is
// Blake2b-256 over the canonical encoding of k's fields (E-10, RT-02). It is
// a pure function: same input → same output forever.
func EntityID(k {{.KeyQualifier}}{{.KeyTypeName}}) runtime.EntityID {
	h := runtime.NewHash()
{{- range .KeyHashLines}}
	{{.}}
{{- end}}
	return runtime.Finalise(h)
}
{{end -}}

{{define "entityIDMethod"}}
// EntityID is an ergonomic same-package wrapper that delegates to the
// package-level EntityID function (E-12).
func (k {{.KeyTypeName}}) EntityID() runtime.EntityID {
	return EntityID(k)
}
{{end}}
{{define "nestedTypeDecl"}}
// {{.DeltaName}} is the Delta companion type for delta.nested fields of
// type {{.Name}}. It is generated by delta-gen and must not be edited.
type {{.DeltaName}} struct {
{{- range .Fields}}{{if not .Suppressed}}
	{{.DeltaName}} {{.DeltaType}}
{{- end}}{{end}}
}
{{end -}}

{{define "nestedApplyFunc"}}
// {{.ApplyFuncName}} produces the {{.Name}} that results from applying d to u.
// It is a pure function with no chain-envelope validation (delta-gen spec §4.3).
func {{.ApplyFuncName}}(u {{.Name}}, d {{.DeltaName}}) {{.Name}} {
	result := u
{{range .Fields}}	{{template "nestedApplyField" .}}
{{end}}	return result
}
{{end -}}

{{define "nestedApplyMethod"}}
// Apply is an ergonomic same-package wrapper (E-12).
func (u {{.Name}}) Apply(d {{.DeltaName}}) {{.Name}} { return {{.ApplyFuncName}}(u, d) }
{{end -}}

{{define "nestedApplyField"}}{{if .Suppressed}}result.{{.Name}} = u.{{.Name}}{{else if .IsNested}}{{if .NestedFuncName}}result.{{.Name}} = {{.NestedFuncName}}(u.{{.Name}}, d.{{.DeltaName}}){{else}}result.{{.Name}} = u.{{.Name}}.Apply(d.{{.DeltaName}}){{end}}{{else}}if d.{{.DeltaName}} != nil { result.{{.Name}} = *d.{{.DeltaName}} } else { result.{{.Name}} = u.{{.Name}} }{{end}}{{end -}}

{{define "nestedDiffFunc"}}
// {{.DiffFuncName}} produces the minimal {{.DeltaName}} such that {{.ApplyFuncName}}(a, d)
// payload-equals b. Pure function, no chain-envelope validation (delta-gen spec §4.3).
func {{.DiffFuncName}}(a, b {{.Name}}) {{.DeltaName}} {
	d := {{.DeltaName}}{}
{{range .DiffFields}}	{{template "diffField" .}}
{{end}}	return d
}
{{end -}}

{{define "nestedDiffMethod"}}
// Diff is an ergonomic same-package wrapper (E-12).
func (u {{.Name}}) Diff(other {{.Name}}) {{.DeltaName}} { return {{.DiffFuncName}}(u, other) }
{{end -}}`

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

// buildImports returns a types.Qualifier closure, a lazy import-list getter,
// and a recordExtra closure for injecting additional imports after view
// construction is complete (e.g. "reflect" when Diff needs reflect.DeepEqual).
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
) (qualifier types.Qualifier, getImports func() []importSpec, recordExtra func(string)) {
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

	recordExtra = func(path string) {
		if _, exists := recorded[path]; !exists {
			recorded[path] = importSpec{Path: path}
		}
	}

	return qual, getImports, recordExtra
}

// ── View construction ─────────────────────────────────────────────────────────

// buildNestedTypeView constructs the template view for one delta.nested
// companion type U. It recursively visits any delta.nested sub-fields of U,
// collecting their companion views in bottom-up order (deepest first) so that
// forward references are avoided in the generated output. The visited set
// prevents duplicate emission when multiple fields share the same nested type
// (N-01 req 09). Returns (view, additional companion views from deeper nesting, error).
func buildNestedTypeView(
	typeName string,
	st *types.Struct,
	qualifier types.Qualifier,
	emitMethod bool,
	visited map[string]bool,
) (nestedTypeView, []nestedTypeView, error) {
	nv := nestedTypeView{
		Name:          typeName,
		DeltaName:     typeName + "Delta",
		ApplyFuncName: "Apply" + typeName,
		DiffFuncName:  "Diff" + typeName,
		EmitMethod:    emitMethod,
	}

	var additional []nestedTypeView

	for i := 0; i < st.NumFields(); i++ {
		field := st.Field(i)

		// Skip unexported fields in cross-package mode (emitMethod == false means cross-pkg).
		if !emitMethod && !field.Exported() {
			continue
		}

		rawTag := reflect.StructTag(st.Tag(i)).Get("eddt")
		tag, err := parseTag(rawTag)
		if err != nil {
			return nestedTypeView{}, nil, fmt.Errorf(
				"nested type %s field %s: parsing eddt:%q: %w", typeName, field.Name(), rawTag, err)
		}

		// Suppressed fields: propagated in Apply, absent from TDelta and Diff.
		if tag.Kind == TagKindOmit || tag.Kind == TagKindRetired {
			nv.Fields = append(nv.Fields, fieldView{Name: field.Name(), Suppressed: true})
			continue
		}

		shape, err := classifyShape(field.Type())
		if err != nil {
			return nestedTypeView{}, nil, fmt.Errorf(
				"nested type %s field %s: %w", typeName, field.Name(), err)
		}

		if tag.Kind == TagKindNested {
			if shape != ShapeStructValue {
				return nestedTypeView{}, nil, fmt.Errorf(
					"nested type %s field %s: delta.nested on slice/map shapes is not yet implemented (N-03/N-04)",
					typeName, field.Name())
			}
			named, ok := field.Type().(*types.Named)
			if !ok {
				return nestedTypeView{}, nil, fmt.Errorf(
					"nested type %s field %s: delta.nested requires a named type",
					typeName, field.Name())
			}
			subTypeName := named.Obj().Name()
			nestedFuncName, nestedDiffFuncName := "", ""
			if !emitMethod {
				nestedFuncName = "Apply" + subTypeName
				nestedDiffFuncName = "Diff" + subTypeName
			}
			fv := fieldView{
				Name:               field.Name(),
				DeltaName:          field.Name(),
				DeltaType:          subTypeName + "Delta",
				IsNested:           true,
				NestedFuncName:     nestedFuncName,
				NestedDiffFuncName: nestedDiffFuncName,
			}
			nv.Fields = append(nv.Fields, fv)
			nv.DiffFields = append(nv.DiffFields, fv)

			if !visited[subTypeName] {
				visited[subTypeName] = true
				subSt, _ := named.Underlying().(*types.Struct)
				subView, subExtra, err := buildNestedTypeView(subTypeName, subSt, qualifier, emitMethod, visited)
				if err != nil {
					return nestedTypeView{}, nil, err
				}
				if subView.NeedsReflect {
					nv.NeedsReflect = true
				}
				additional = append(additional, subExtra...)
				additional = append(additional, subView)
			}
			continue
		}

		// Atomic field (TagKindNone or TagKindCommutative): pointer-wrap the source type.
		deltaType := types.TypeString(types.NewPointer(field.Type()), qualifier)
		fv := fieldView{
			Name:      field.Name(),
			DeltaName: "Set" + field.Name(),
			DeltaType: deltaType,
		}
		if shape != ShapeScalar {
			fv.UseReflectEq = true
			nv.NeedsReflect = true
		}
		nv.Fields = append(nv.Fields, fv)
		nv.DiffFields = append(nv.DiffFields, fv)
	}

	return nv, additional, nil
}

// buildSnapshotView constructs the template view for one ParsedSnapshot.
//
// delta.nested on struct-value shapes triggers N-01 compositional emission:
// a companion UDelta type + ApplyU/DiffU functions are collected in
// sv.NestedTypes (bottom-up order). delta.nested on slice/map shapes
// returns a sentinel error referencing N-03/N-04. (delta.clearable is
// rejected upstream by T-01.)
//
// emitMethod gates same-package method wrappers (E-12): pass true for
// same-package output, false for cross-package.
//
// Suppressed fields (delta.omit / delta.retired) are included in sv.Fields
// with Suppressed: true so the Apply template can emit result.F = s.F
// propagation assignments (EM-02). The Delta-side type declaration template
// gates on {{not .Suppressed}} to keep them out of TDelta.
//
// Each admitted atomic field's DeltaType is rendered via types.TypeString on a
// single pointer-wrap of the source GoType:
//
//	scalar T        → *T
//	pointer *T      → **T
//	struct value T  → *T      (atomic, untagged)
//	slice []T       → *[]T    (atomic per E-15)
//	map[K]V         → *map[K]V (atomic per E-16)
//
// The caller must pass a qualifier derived from buildImports so that foreign
// packages are recorded as a side effect of type rendering.
func buildSnapshotView(ps *ParsedSnapshot, qualifier types.Qualifier, emitMethod bool) (snapshotView, error) {
	sv := snapshotView{
		Name:      ps.Name,
		DeltaName: ps.Name + "Delta",
		KeyName:   ps.KeyVar.Name(),
	}

	// Resolve the key type name and hash lines (EM-05).
	switch kt := ps.KeyVar.Type().(type) {
	case *types.Named:
		sv.KeyTypeName = kt.Obj().Name()
	default:
		sv.KeyTypeName = types.TypeString(ps.KeyVar.Type(), nil)
	}
	hashLines, err := buildKeyHashLines(ps.KeyVar.Type(), ps.KeyShape)
	if err != nil {
		return snapshotView{}, err
	}
	sv.KeyHashLines = hashLines

	visited := make(map[string]bool) // dedup set for nested companion types (N-01 req 09)

	for _, f := range ps.Fields {
		// Presence-axis: omit/retired fields are suppressed on the Delta side
		// but still appear in Fields so the Apply template emits result.F = s.F.
		if f.Tag.Kind == TagKindOmit || f.Tag.Kind == TagKindRetired {
			sv.Fields = append(sv.Fields, fieldView{Name: f.Name, Suppressed: true})
			continue
		}

		// N-01: delta.nested struct-value fields emit a companion type.
		// delta.nested on slice/map shapes remains a Phase-5 sentinel (N-03/N-04).
		if f.Tag.Kind == TagKindNested {
			if f.Shape != ShapeStructValue {
				return snapshotView{}, fmt.Errorf(
					"field %s.%s: delta.nested on slice/map shapes is not yet implemented (N-03/N-04)",
					ps.Name, f.Name)
			}
			named, ok := f.GoType.(*types.Named)
			if !ok {
				return snapshotView{}, fmt.Errorf(
					"field %s.%s: delta.nested requires a named type (anonymous struct types are not supported)",
					ps.Name, f.Name)
			}
			subTypeName := named.Obj().Name()
			nestedFuncName, nestedDiffFuncName := "", ""
			if !emitMethod {
				nestedFuncName = "Apply" + subTypeName
				nestedDiffFuncName = "Diff" + subTypeName
			}
			fv := fieldView{
				Name:               f.Name,
				DeltaName:          f.Name,
				DeltaType:          subTypeName + "Delta",
				IsNested:           true,
				NestedFuncName:     nestedFuncName,
				NestedDiffFuncName: nestedDiffFuncName,
			}
			sv.Fields = append(sv.Fields, fv)
			sv.DiffFields = append(sv.DiffFields, fv)

			if !visited[subTypeName] {
				visited[subTypeName] = true
				subSt, _ := named.Underlying().(*types.Struct)
				subView, subExtra, err := buildNestedTypeView(subTypeName, subSt, qualifier, emitMethod, visited)
				if err != nil {
					return snapshotView{}, fmt.Errorf("field %s.%s: %w", ps.Name, f.Name, err)
				}
				if subView.NeedsReflect {
					sv.NeedsReflect = true
				}
				sv.NestedTypes = append(sv.NestedTypes, subExtra...)
				sv.NestedTypes = append(sv.NestedTypes, subView)
			}
			continue
		}

		// TagKindNone and TagKindCommutative both emit as atomic (§9.5).
		// Render the Delta-side type as *<GoType>; one pointer wrap covers all shapes.
		deltaType := types.TypeString(types.NewPointer(f.GoType), qualifier)

		fv := fieldView{
			Name:      f.Name,
			DeltaName: "Set" + f.Name,
			DeltaType: deltaType,
		}
		// Non-scalar shapes require reflect.DeepEqual for Diff comparisons (EM-03):
		// pointer identity != value equality, and slice/map have no == operator.
		if f.Shape != ShapeScalar {
			fv.UseReflectEq = true
			sv.NeedsReflect = true
		}
		sv.Fields = append(sv.Fields, fv)
		// DiffFields excludes suppressed fields so the Diff template emits no body
		// line for them (EM-03).
		sv.DiffFields = append(sv.DiffFields, fv)
	}

	return sv, nil
}

// ── Emit orchestration ────────────────────────────────────────────────────────

// executeEmit runs the full Phase-4 emit pipeline:
//
//  1. Parse --pkg-alias entries and derive emitOpts.
//  2. Build the qualifier, import-recorder, and extra-import injector via
//     buildImports.
//  3. Translate each ParsedSnapshot to a snapshotView via buildSnapshotView
//     (this side-effects the qualifier to record foreign packages).
//  4. Inject the "reflect" import if any view has NeedsReflect set (EM-03),
//     then materialise the import list via getImports().
//  5. Execute deltaTemplate into a buffer.
//  6. Format the buffer with go/format.Source (syntax errors include the raw
//     source for debuggability, mirroring the arrow-writer-gen pattern).
//  7. Write the formatted result to g.OutPath.
func executeEmit(snapshots []*ParsedSnapshot, g *Generator) error {
	opts := emitOpts{
		crossPackage: g.CrossPackage,
		aliases:      parsePkgAliases(g.PkgAliases),
	}

	// Step 2: build the qualifier, import-recorder, and extra-import injector.
	qualifier, getImports, recordExtra := buildImports(snapshots, opts)

	// emitMethod gates same-package method wrappers (E-12); precomputed once.
	emitMethod := !g.CrossPackage

	// Step 3: translate each snapshot into a template view.
	views := make([]snapshotView, 0, len(snapshots))
	for _, ps := range snapshots {
		sv, err := buildSnapshotView(ps, qualifier, emitMethod)
		if err != nil {
			return err
		}

		// Set the Qualifier for the doc comment in cross-package mode.
		if g.CrossPackage {
			sv.Qualifier = ps.PkgName + "."
			if alias := opts.aliases[ps.PkgPath]; alias != "" {
				sv.Qualifier = alias + "."
			}
			// Key type also lives in the source package → same qualifier prefix.
			if _, isNamed := ps.KeyVar.Type().(*types.Named); isNamed {
				sv.KeyQualifier = sv.Qualifier
			}
		}

		// EmitMethod gates the same-package method wrappers (E-12, EM-02..EM-04).
		sv.EmitMethod = emitMethod

		// EmitEntityIDMethod additionally requires the key type to be a named
		// type: Go forbids defining methods on unnamed basic types (EM-05, R-24).
		_, isNamed := ps.KeyVar.Type().(*types.Named)
		sv.EmitEntityIDMethod = sv.EmitMethod && isNamed

		views = append(views, sv)
	}

	// Step 4: inject the "reflect" import if any Diff field uses reflect.DeepEqual,
	// then materialise the import list. The check must run after all views are
	// built so that NeedsReflect is fully populated across all target structs.
	for _, sv := range views {
		if sv.NeedsReflect {
			recordExtra("reflect")
			break
		}
	}

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
