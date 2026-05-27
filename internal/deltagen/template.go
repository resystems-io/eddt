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
//     types.TypeString. Sets UseReflectEq via types.Comparable per field
//     and NeedsReflect per snapshot for the conditional reflect-import
//     logic (EM-03). Only non-comparable types (slice, map, complex structs)
//     trigger reflect; comparable types including pointers use !=.
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
// nestedKind distinguishes between struct-companion types (nestedKindStruct,
// the default) and the net-new map/slice wrapper types introduced for CL-05..07
// clearable emission (nestedKindMapWrapper, nestedKindSliceWrapper). The zero
// value (nestedKindStruct) preserves byte-identical emission for all existing
// nested struct companions.
type nestedKind int

const (
	nestedKindStruct       nestedKind = iota // existing N-01 struct companion
	nestedKindMapWrapper                     // CL-05: <X>MapDelta wrapper for clearable map field
	nestedKindSliceWrapper                   // CL-05: <X>SliceDelta wrapper for clearable slice field
)

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

	// Kind discriminates struct companions (default 0) from clearable wrappers.
	// Non-zero kinds skip the struct-companion Apply/Diff func templates.
	Kind nestedKind

	// IsMapWrapper / IsSliceWrapper are derived from Kind for use in templates.
	IsMapWrapper   bool
	IsSliceWrapper bool

	// Wrapper payload fields: populated only for nestedKindMapWrapper and
	// nestedKindSliceWrapper (CL-05..07). The algorithm mirrors the existing
	// applyMapField / diffMapField (and applySliceField / diffSliceField) logic
	// but parameterised over the wrapper struct rather than the parent's sibling fields.

	// WrapperUpdatedName is "Updated<Field>" (map) or "Added<Field>" (slice).
	WrapperUpdatedName string
	// WrapperRemovedName is "Removed<Field>" in both shapes.
	WrapperRemovedName string
	// WrapperMapType is the rendered map type string (e.g. "map[string]string").
	WrapperMapType string
	// WrapperMapKeyType is the rendered map key type string (e.g. "string").
	WrapperMapKeyType string
	// WrapperSliceType is the rendered slice type string (e.g. "[]string").
	WrapperSliceType string
	// WrapperSliceElem is the rendered element type string (e.g. "string").
	WrapperSliceElem string
	// WrapperUseReflectEq is true when the map value / slice elem is not comparable.
	WrapperUseReflectEq bool
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
	// because the field's Go type is not comparable (e.g. slice, map, or a struct
	// containing a slice/map). Scalars and simple structs use != directly.
	// Pointer fields (*T) are handled separately via IsPointer regardless of
	// types.Comparable — they always use nil-equivalence + dereferenced comparison.
	UseReflectEq bool

	// IsPointer is true for ShapePointer (*T) fields. Diff emits a
	// nil-equivalence + dereferenced-value comparison rather than pointer identity
	// (implements R-27, resolves E-02). PointeeUseReflectEq controls whether the
	// pointee comparison uses == or reflect.DeepEqual.
	IsPointer bool

	// PointeeUseReflectEq is true when the pointee type T of a *T field is not
	// comparable (e.g. a struct containing a slice), so the deref comparison must
	// use reflect.DeepEqual(*a.X, *b.X). Only meaningful when IsPointer is true.
	PointeeUseReflectEq bool

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

	// IsMapNested is true for delta.nested map[K]V fields (N-03). When true,
	// DeltaName = "Updated"+Name, DeltaType = rendered map type (no pointer wrap),
	// MapRemovedName = "Removed"+Name, MapKeyType = rendered K type.
	IsMapNested bool

	// MapRemovedName is the Delta-side field name for the removed-keys slice
	// (e.g. "RemovedTags" for a source field named Tags). Only set when IsMapNested.
	MapRemovedName string

	// MapKeyType is the rendered Go type string for the map key K
	// (e.g. "string"). Used to declare the RemovedX []K field. Only when IsMapNested.
	MapKeyType string

	// MapValueUseReflectEq is true when the map value type V is not comparable
	// (e.g. a struct containing a slice/map) and Diff must use reflect.DeepEqual
	// for value comparison. Set by !types.Comparable(mapT.Elem()). Only when IsMapNested.
	MapValueUseReflectEq bool

	// IsSliceNested is true for delta.nested []T fields (N-04, E-15 set-diff semantics).
	// DeltaName = "Added"+Name, SliceRemovedName = "Removed"+Name, DeltaType = []T.
	IsSliceNested bool

	// SliceRemovedName is the Delta-side field name for removed elements
	// (e.g. "RemovedNames" for a source field named Names). Only when IsSliceNested.
	SliceRemovedName string

	// SliceElemType is the rendered Go element type string (e.g. "string", "Tag").
	// Used as the map key type in the O(n) comparable-element path. Only when IsSliceNested.
	SliceElemType string

	// SliceElemUseReflectEq is true when the slice element type is not comparable
	// (§5.2) and the O(n²) reflect.DeepEqual fallback must be used instead of the
	// O(n) map[T]struct{} set path. Set by !types.Comparable(sliceT.Elem()). Only when IsSliceNested.
	SliceElemUseReflectEq bool

	// ── Clearable-envelope fields (CL-05..07, E-17/E-23) ─────────────────────
	//
	// IsClearable is true for delta.nested+delta.clearable fields. When true
	// the parent Delta carries `X runtime.FieldDelta[ClearableInner]` (single
	// field, no sibling fields). IsNested / IsMapNested / IsSliceNested are false.
	IsClearable bool

	// ClearableInner is the T_inner type name used in runtime.FieldDelta[T_inner]:
	// "FooDelta" for struct, "<X>MapDelta" for map, "<X>SliceDelta" for slice.
	ClearableInner string

	// ClearableIsStruct is true when the inner shape is a named struct type.
	// Drives the Diff template's equality and zero-composite predicates.
	ClearableIsStruct bool

	// ClearableZeroComposite is the Go expression for the zero value of the
	// composite field: qualified struct literal (e.g. "Foo{}" / "model.Foo{}")
	// for struct; "nil" for map and slice.
	ClearableZeroComposite string

	// ClearableApplyFunc / ClearableDiffFunc are the package-level function
	// names for the inner Apply/Diff (always func form, both modes, so the
	// Op-switch template is mode-agnostic).
	ClearableApplyFunc string
	ClearableDiffFunc  string

	// ClearableStructEqReflect is true when the struct type is not comparable
	// (e.g. it contains a slice/map), so the Diff predicate must use
	// reflect.DeepEqual instead of == for equality and zero-composite detection.
	ClearableStructEqReflect bool
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
//   - nestedTypeDecl:      companion Delta struct for a delta.nested type (N-01).
//   - nestedApplyFunc:     package-level ApplyU function for a nested type (N-01).
//   - nestedApplyMethod:   same-package method wrapper func (u U) Apply(...) (N-01).
//   - nestedApplyField:    per-field body line for nestedApplyFunc (uses u. receiver).
//   - nestedDiffFunc:      package-level DiffU function for a nested type (N-01).
//   - nestedDiffMethod:    same-package method wrapper func (u U) Diff(...) (N-01).
//   - applyMapField:         apply body block for a delta.nested map field (N-03, uses s.).
//   - nestedApplyMapField:   apply body block for a map field inside a nested type (N-03, uses u.).
//   - diffMapField:          diff body block for a delta.nested map field (N-03, E-16).
//   - applySliceField:       apply body block for a delta.nested slice field (N-04, uses s.).
//   - nestedApplySliceField: apply body block for a slice field inside a nested type (N-04, uses u.).
//   - diffSliceField:        diff body block for a delta.nested slice field (N-04, E-15).
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
{{range .Snapshots}}{{range .NestedTypes}}{{if .IsMapWrapper}}{{template "mapWrapper" .}}{{else if .IsSliceWrapper}}{{template "sliceWrapper" .}}{{else}}{{template "nestedTypeDecl" .}}{{template "nestedApplyFunc" .}}{{if .EmitMethod}}{{template "nestedApplyMethod" .}}{{end}}{{template "nestedDiffFunc" .}}{{if .EmitMethod}}{{template "nestedDiffMethod" .}}{{end}}{{end}}
{{end}}
// {{.DeltaName}} is the Delta companion type for {{.Qualifier}}{{.Name}}.
// Each payload field is expressed as an optional pointer: a nil value means
// "no change" for that field when Apply is called.
type {{.DeltaName}} struct {
	runtime.Header
{{- range .Fields}}{{if not .Suppressed}}{{if .IsSliceNested}}
	{{.DeltaName}} {{.DeltaType}}
	{{.SliceRemovedName}} {{.DeltaType}}
{{- else if .IsMapNested}}
	{{.DeltaName}} {{.DeltaType}}
	{{.MapRemovedName}} []{{.MapKeyType}}
{{- else}}
	{{.DeltaName}} {{.DeltaType}}
{{- end}}{{end}}{{end}}
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

{{define "applyField"}}{{if .Suppressed}}result.{{.Name}} = s.{{.Name}}{{else if .IsClearable}}{{template "applyClearableField" .}}{{else if .IsNested}}{{if .NestedFuncName}}result.{{.Name}} = {{.NestedFuncName}}(s.{{.Name}}, d.{{.DeltaName}}){{else}}result.{{.Name}} = s.{{.Name}}.Apply(d.{{.DeltaName}}){{end}}{{else if .IsSliceNested}}{{template "applySliceField" .}}{{else if .IsMapNested}}{{template "applyMapField" .}}{{else}}if d.{{.DeltaName}} != nil { result.{{.Name}} = *d.{{.DeltaName}} } else { result.{{.Name}} = s.{{.Name}} }{{end}}{{end -}}

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

{{define "diffField"}}{{if .IsClearable}}{{template "diffClearableField" .}}{{else if .IsSliceNested}}{{template "diffSliceField" .}}{{else if .IsMapNested}}{{template "diffMapField" .}}{{else if .IsNested}}{{if .NestedDiffFuncName}}d.{{.DeltaName}} = {{.NestedDiffFuncName}}(a.{{.Name}}, b.{{.Name}}){{else}}d.{{.DeltaName}} = a.{{.Name}}.Diff(b.{{.Name}}){{end}}{{else if .IsPointer}}if !((a.{{.Name}} == nil && b.{{.Name}} == nil) || (a.{{.Name}} != nil && b.{{.Name}} != nil && {{if .PointeeUseReflectEq}}reflect.DeepEqual(*a.{{.Name}}, *b.{{.Name}}){{else}}*a.{{.Name}} == *b.{{.Name}}{{end}})) { d.{{.DeltaName}} = &b.{{.Name}} }{{else if .UseReflectEq}}if !reflect.DeepEqual(a.{{.Name}}, b.{{.Name}}) { d.{{.DeltaName}} = &b.{{.Name}} }{{else}}if a.{{.Name}} != b.{{.Name}} { d.{{.DeltaName}} = &b.{{.Name}} }{{end}}{{end -}}

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
// EntityID creates a hash for the key field: {{.KeyName}}
//
// It returns the deterministic content-hash EntityID of k. The hash is
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
// EntityID creates a hash for the key field: {{.KeyName}}
//
// It is an ergonomic same-package wrapper that delegates to the
// package-level EntityID function (E-12).
func (k {{.KeyTypeName}}) EntityID() runtime.EntityID {
	return EntityID(k)
}
{{end}}
{{define "nestedTypeDecl"}}
// {{.DeltaName}} is the Delta companion type for delta.nested fields of
// type {{.Name}}. It is generated by delta-gen and must not be edited.
type {{.DeltaName}} struct {
{{- range .Fields}}{{if not .Suppressed}}{{if .IsSliceNested}}
	{{.DeltaName}} {{.DeltaType}}
	{{.SliceRemovedName}} {{.DeltaType}}
{{- else if .IsMapNested}}
	{{.DeltaName}} {{.DeltaType}}
	{{.MapRemovedName}} []{{.MapKeyType}}
{{- else}}
	{{.DeltaName}} {{.DeltaType}}
{{- end}}{{end}}{{end}}
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

{{define "nestedApplyField"}}{{if .Suppressed}}result.{{.Name}} = u.{{.Name}}{{else if .IsNested}}{{if .NestedFuncName}}result.{{.Name}} = {{.NestedFuncName}}(u.{{.Name}}, d.{{.DeltaName}}){{else}}result.{{.Name}} = u.{{.Name}}.Apply(d.{{.DeltaName}}){{end}}{{else if .IsSliceNested}}{{template "nestedApplySliceField" .}}{{else if .IsMapNested}}{{template "nestedApplyMapField" .}}{{else}}if d.{{.DeltaName}} != nil { result.{{.Name}} = *d.{{.DeltaName}} } else { result.{{.Name}} = u.{{.Name}} }{{end}}{{end -}}

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
{{end -}}

{{define "applyMapField"}}
// apply delta.nested map field {{.Name}} (N-03):
// 1. copy source map, 2. delete removed keys, 3. upsert updated entries.
{
	__m := make({{.DeltaType}}, len(s.{{.Name}}))
	for __k, __v := range s.{{.Name}} { __m[__k] = __v }
	for _, __k := range d.{{.MapRemovedName}} { delete(__m, __k) }
	for __k, __v := range d.{{.DeltaName}} { __m[__k] = __v }
	result.{{.Name}} = __m
}{{end -}}

{{define "nestedApplyMapField"}}
// apply delta.nested map field {{.Name}} (N-03):
// 1. copy source map, 2. delete removed keys, 3. upsert updated entries.
{
	__m := make({{.DeltaType}}, len(u.{{.Name}}))
	for __k, __v := range u.{{.Name}} { __m[__k] = __v }
	for _, __k := range d.{{.MapRemovedName}} { delete(__m, __k) }
	for __k, __v := range d.{{.DeltaName}} { __m[__k] = __v }
	result.{{.Name}} = __m
}{{end -}}

{{define "diffMapField"}}
// diff delta.nested map field {{.Name}} (N-03, E-16 upsert semantics):
// RemovedX = keys in a absent from b; UpdatedX = entries in b absent or changed vs a.
// A value-changed entry appears in UpdatedX only, never in RemovedX.
{
	var __removed []{{.MapKeyType}}
	for __k := range a.{{.Name}} {
		if _, __ok := b.{{.Name}}[__k]; !__ok {
			__removed = append(__removed, __k)
		}
	}
	if len(__removed) > 0 { d.{{.MapRemovedName}} = __removed }
	var __updated {{.DeltaType}}
	for __k, __v := range b.{{.Name}} {
		if __av, __ok := a.{{.Name}}[__k]; !__ok || {{if .MapValueUseReflectEq}}!reflect.DeepEqual(__av, __v){{else}}__av != __v{{end}} {
			if __updated == nil { __updated = make({{.DeltaType}}) }
			__updated[__k] = __v
		}
	}
	if __updated != nil { d.{{.DeltaName}} = __updated }
}{{end -}}

{{define "applySliceField"}}
// apply delta.nested slice field {{.Name}} (N-04, E-15 set-diff semantics):
// 1. filter removed elements, 2. append added elements (E-03 survivor order).
{{- if .SliceElemUseReflectEq}}
// Element type is not comparable: O(n²) reflect.DeepEqual fallback (§5.2).
{
	__src := s.{{.Name}}
	if len(d.{{.SliceRemovedName}}) > 0 {
		__out := make({{.DeltaType}}, 0, len(__src))
		for _, __v := range __src {
			__keep := true
			for _, __r := range d.{{.SliceRemovedName}} {
				if reflect.DeepEqual(__r, __v) { __keep = false; break }
			}
			if __keep { __out = append(__out, __v) }
		}
		__src = __out
	}
	result.{{.Name}} = append(__src, d.{{.DeltaName}}...)
}
{{- else}}
// Element type is comparable: O(n) map[T]struct{} membership set.
{
	__src := s.{{.Name}}
	if len(d.{{.SliceRemovedName}}) > 0 {
		__rem := make(map[{{.SliceElemType}}]struct{}, len(d.{{.SliceRemovedName}}))
		for _, __r := range d.{{.SliceRemovedName}} { __rem[__r] = struct{}{} }
		__out := make({{.DeltaType}}, 0, len(__src))
		for _, __v := range __src {
			if _, __ok := __rem[__v]; !__ok { __out = append(__out, __v) }
		}
		__src = __out
	}
	result.{{.Name}} = append(__src, d.{{.DeltaName}}...)
}
{{- end}}{{end -}}

{{define "nestedApplySliceField"}}
// apply delta.nested slice field {{.Name}} inside a nested type (N-04, E-15):
{{- if .SliceElemUseReflectEq}}
{
	__src := u.{{.Name}}
	if len(d.{{.SliceRemovedName}}) > 0 {
		__out := make({{.DeltaType}}, 0, len(__src))
		for _, __v := range __src {
			__keep := true
			for _, __r := range d.{{.SliceRemovedName}} {
				if reflect.DeepEqual(__r, __v) { __keep = false; break }
			}
			if __keep { __out = append(__out, __v) }
		}
		__src = __out
	}
	result.{{.Name}} = append(__src, d.{{.DeltaName}}...)
}
{{- else}}
{
	__src := u.{{.Name}}
	if len(d.{{.SliceRemovedName}}) > 0 {
		__rem := make(map[{{.SliceElemType}}]struct{}, len(d.{{.SliceRemovedName}}))
		for _, __r := range d.{{.SliceRemovedName}} { __rem[__r] = struct{}{} }
		__out := make({{.DeltaType}}, 0, len(__src))
		for _, __v := range __src {
			if _, __ok := __rem[__v]; !__ok { __out = append(__out, __v) }
		}
		__src = __out
	}
	result.{{.Name}} = append(__src, d.{{.DeltaName}}...)
}
{{- end}}{{end -}}

{{define "diffSliceField"}}
// diff delta.nested slice field {{.Name}} (N-04, E-15 set-diff semantics):
// AddedX = b.X ∖ a.X; RemovedX = a.X ∖ b.X.
{{- if .SliceElemUseReflectEq}}
// Element type is not comparable: O(n²) reflect.DeepEqual fallback (§5.2).
{
	var __added {{.DeltaType}}
	for _, __v := range b.{{.Name}} {
		__found := false
		for _, __av := range a.{{.Name}} {
			if reflect.DeepEqual(__av, __v) { __found = true; break }
		}
		if !__found { __added = append(__added, __v) }
	}
	if len(__added) > 0 { d.{{.DeltaName}} = __added }
	var __removed {{.DeltaType}}
	for _, __v := range a.{{.Name}} {
		__found := false
		for _, __bv := range b.{{.Name}} {
			if reflect.DeepEqual(__bv, __v) { __found = true; break }
		}
		if !__found { __removed = append(__removed, __v) }
	}
	if len(__removed) > 0 { d.{{.SliceRemovedName}} = __removed }
}
{{- else}}
// Element type is comparable: O(n) map[T]struct{} membership sets.
{
	__aset := make(map[{{.SliceElemType}}]struct{}, len(a.{{.Name}}))
	for _, __v := range a.{{.Name}} { __aset[__v] = struct{}{} }
	var __added {{.DeltaType}}
	for _, __v := range b.{{.Name}} {
		if _, __ok := __aset[__v]; !__ok { __added = append(__added, __v) }
	}
	if len(__added) > 0 { d.{{.DeltaName}} = __added }
	__bset := make(map[{{.SliceElemType}}]struct{}, len(b.{{.Name}}))
	for _, __v := range b.{{.Name}} { __bset[__v] = struct{}{} }
	var __removed {{.DeltaType}}
	for _, __v := range a.{{.Name}} {
		if _, __ok := __bset[__v]; !__ok { __removed = append(__removed, __v) }
	}
	if len(__removed) > 0 { d.{{.SliceRemovedName}} = __removed }
}
{{- end}}{{end -}}

{{define "applyClearableField"}}switch d.{{.DeltaName}}.Op {
case runtime.OpRetract:
	result.{{.Name}} = {{.ClearableZeroComposite}}
case runtime.OpAssert:
	result.{{.Name}} = {{.ClearableApplyFunc}}(s.{{.Name}}, d.{{.DeltaName}}.Value)
default:
	result.{{.Name}} = s.{{.Name}}
}{{end -}}

{{define "diffClearableField"}}{{if .ClearableIsStruct}}if {{if .ClearableStructEqReflect}}!reflect.DeepEqual(a.{{.Name}}, b.{{.Name}}){{else}}a.{{.Name}} != b.{{.Name}}{{end}} {
	if {{if .ClearableStructEqReflect}}reflect.DeepEqual(b.{{.Name}}, {{.ClearableZeroComposite}}){{else}}b.{{.Name}} == ({{.ClearableZeroComposite}}){{end}} {
		d.{{.DeltaName}} = runtime.FieldDelta[{{.ClearableInner}}]{Op: runtime.OpRetract}
	} else {
		d.{{.DeltaName}} = runtime.FieldDelta[{{.ClearableInner}}]{Op: runtime.OpAssert, Value: {{.ClearableDiffFunc}}(a.{{.Name}}, b.{{.Name}})}
	}
}{{else}}{
	__inner := {{.ClearableDiffFunc}}(a.{{.Name}}, b.{{.Name}})
	if !__inner.IsEmpty() {
		if len(b.{{.Name}}) == 0 {
			d.{{.DeltaName}} = runtime.FieldDelta[{{.ClearableInner}}]{Op: runtime.OpRetract}
		} else {
			d.{{.DeltaName}} = runtime.FieldDelta[{{.ClearableInner}}]{Op: runtime.OpAssert, Value: __inner}
		}
	}
}{{end}}{{end -}}

{{define "mapWrapper"}}
// {{.DeltaName}} is the clearable-envelope inner type for a delta.nested+delta.clearable
// map field. It carries the per-entry upsert/remove delta (E-16/E-17).
type {{.DeltaName}} struct {
	// {{.WrapperUpdatedName}} contains entries to upsert (add or overwrite).
	{{.WrapperUpdatedName}} {{.WrapperMapType}}
	// {{.WrapperRemovedName}} contains keys to delete.
	{{.WrapperRemovedName}} []{{.WrapperMapKeyType}}
}

// IsEmpty reports whether the delta has no changes.
// Used by the Diff three-branch predicate to decide between OpIgnore and OpAssert/OpRetract.
func (w {{.DeltaName}}) IsEmpty() bool {
	return len(w.{{.WrapperUpdatedName}}) == 0 && len(w.{{.WrapperRemovedName}}) == 0
}

// {{.ApplyFuncName}} applies w to prior, returning the resulting map (N-03).
func {{.ApplyFuncName}}(prior {{.WrapperMapType}}, w {{.DeltaName}}) {{.WrapperMapType}} {
	__m := make({{.WrapperMapType}}, len(prior))
	for __k, __v := range prior { __m[__k] = __v }
	for _, __k := range w.{{.WrapperRemovedName}} { delete(__m, __k) }
	for __k, __v := range w.{{.WrapperUpdatedName}} { __m[__k] = __v }
	return __m
}

// {{.DiffFuncName}} computes the minimal {{.DeltaName}} such that {{.ApplyFuncName}}(a, d) value-equals b (N-03, E-16).
func {{.DiffFuncName}}(a, b {{.WrapperMapType}}) {{.DeltaName}} {
	var w {{.DeltaName}}
	for __k := range a {
		if _, __ok := b[__k]; !__ok {
			w.{{.WrapperRemovedName}} = append(w.{{.WrapperRemovedName}}, __k)
		}
	}
	for __k, __v := range b {
		if __av, __ok := a[__k]; !__ok || {{if .WrapperUseReflectEq}}!reflect.DeepEqual(__av, __v){{else}}__av != __v{{end}} {
			if w.{{.WrapperUpdatedName}} == nil { w.{{.WrapperUpdatedName}} = make({{.WrapperMapType}}) }
			w.{{.WrapperUpdatedName}}[__k] = __v
		}
	}
	return w
}
{{end -}}

{{define "sliceWrapper"}}
// {{.DeltaName}} is the clearable-envelope inner type for a delta.nested+delta.clearable
// slice field. It carries the set-difference delta (E-15/E-17).
type {{.DeltaName}} struct {
	// {{.WrapperUpdatedName}} contains elements to add (present in b, absent in a).
	{{.WrapperUpdatedName}} {{.WrapperSliceType}}
	// {{.WrapperRemovedName}} contains elements to remove (present in a, absent in b).
	{{.WrapperRemovedName}} {{.WrapperSliceType}}
}

// IsEmpty reports whether the delta has no changes.
func (w {{.DeltaName}}) IsEmpty() bool {
	return len(w.{{.WrapperUpdatedName}}) == 0 && len(w.{{.WrapperRemovedName}}) == 0
}
{{if .WrapperUseReflectEq}}// {{.ApplyFuncName}} applies w to prior (N-04, E-15); O(n²) reflect.DeepEqual fallback (§5.2).
func {{.ApplyFuncName}}(prior {{.WrapperSliceType}}, w {{.DeltaName}}) {{.WrapperSliceType}} {
	__src := prior
	if len(w.{{.WrapperRemovedName}}) > 0 {
		__out := make({{.WrapperSliceType}}, 0, len(__src))
		for _, __v := range __src {
			__keep := true
			for _, __r := range w.{{.WrapperRemovedName}} {
				if reflect.DeepEqual(__r, __v) { __keep = false; break }
			}
			if __keep { __out = append(__out, __v) }
		}
		__src = __out
	}
	return append(__src, w.{{.WrapperUpdatedName}}...)
}
// {{.DiffFuncName}} computes the minimal {{.DeltaName}} such that {{.ApplyFuncName}}(a, d) set-equals b (N-04, E-15); O(n²).
func {{.DiffFuncName}}(a, b {{.WrapperSliceType}}) {{.DeltaName}} {
	var w {{.DeltaName}}
	for _, __v := range b {
		__found := false
		for _, __av := range a {
			if reflect.DeepEqual(__av, __v) { __found = true; break }
		}
		if !__found { w.{{.WrapperUpdatedName}} = append(w.{{.WrapperUpdatedName}}, __v) }
	}
	for _, __v := range a {
		__found := false
		for _, __bv := range b {
			if reflect.DeepEqual(__bv, __v) { __found = true; break }
		}
		if !__found { w.{{.WrapperRemovedName}} = append(w.{{.WrapperRemovedName}}, __v) }
	}
	return w
}
{{- else}}// {{.ApplyFuncName}} applies w to prior (N-04, E-15); O(n) map[T]struct{} membership set.
func {{.ApplyFuncName}}(prior {{.WrapperSliceType}}, w {{.DeltaName}}) {{.WrapperSliceType}} {
	__src := prior
	if len(w.{{.WrapperRemovedName}}) > 0 {
		__rem := make(map[{{.WrapperSliceElem}}]struct{}, len(w.{{.WrapperRemovedName}}))
		for _, __r := range w.{{.WrapperRemovedName}} { __rem[__r] = struct{}{} }
		__out := make({{.WrapperSliceType}}, 0, len(__src))
		for _, __v := range __src {
			if _, __ok := __rem[__v]; !__ok { __out = append(__out, __v) }
		}
		__src = __out
	}
	return append(__src, w.{{.WrapperUpdatedName}}...)
}
// {{.DiffFuncName}} computes the minimal {{.DeltaName}} such that {{.ApplyFuncName}}(a, d) set-equals b (N-04, E-15); O(n).
func {{.DiffFuncName}}(a, b {{.WrapperSliceType}}) {{.DeltaName}} {
	var w {{.DeltaName}}
	__aset := make(map[{{.WrapperSliceElem}}]struct{}, len(a))
	for _, __v := range a { __aset[__v] = struct{}{} }
	for _, __v := range b {
		if _, __ok := __aset[__v]; !__ok { w.{{.WrapperUpdatedName}} = append(w.{{.WrapperUpdatedName}}, __v) }
	}
	__bset := make(map[{{.WrapperSliceElem}}]struct{}, len(b))
	for _, __v := range b { __bset[__v] = struct{}{} }
	for _, __v := range a {
		if _, __ok := __bset[__v]; !__ok { w.{{.WrapperRemovedName}} = append(w.{{.WrapperRemovedName}}, __v) }
	}
	return w
}
{{- end}}
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
// forward references are avoided in the generated output.
//
// visited prevents duplicate emission when multiple fields share the same
// nested type (N-01 req 09). inPath tracks the active DFS ancestry chain; an
// entry already in inPath signals a cycle and returns an error (N-02 §3.3.2).
//
// Returns (view, additional companion views from deeper nesting, error).
func buildNestedTypeView(
	typeName string,
	qualifiedTypeName string,
	st *types.Struct,
	qualifier types.Qualifier,
	emitMethod bool,
	visited map[string]bool,
	inPath map[string]bool,
) (nestedTypeView, []nestedTypeView, error) {
	nv := nestedTypeView{
		Name:          qualifiedTypeName, // qualified in cross-package mode (E-12)
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
			if tag.Clearable {
				return nestedTypeView{}, nil, fmt.Errorf(
					"nested type %s field %s: eddt:\"delta.clearable\" inside a delta.nested type is not yet supported",
					typeName, field.Name())
			}
			if shape == ShapeSlice {
				goType := field.Type()
				sliceT := goType.Underlying().(*types.Slice)
				sliceStr := types.TypeString(goType, qualifier)
				elemStr := types.TypeString(sliceT.Elem(), qualifier)
				fv := fieldView{
					Name:                  field.Name(),
					DeltaName:             "Added" + field.Name(),
					DeltaType:             sliceStr,
					IsSliceNested:         true,
					SliceRemovedName:      "Removed" + field.Name(),
					SliceElemType:         elemStr,
					SliceElemUseReflectEq: !types.Comparable(sliceT.Elem()),
				}
				nv.Fields = append(nv.Fields, fv)
				nv.DiffFields = append(nv.DiffFields, fv)
				if fv.SliceElemUseReflectEq {
					nv.NeedsReflect = true
				}
				continue
			}
			if shape == ShapeMap {
				mapT := field.Type().Underlying().(*types.Map)
				keyStr := types.TypeString(mapT.Key(), qualifier)
				mapStr := types.TypeString(field.Type(), qualifier)
				fv := fieldView{
					Name:                 field.Name(),
					DeltaName:            "Updated" + field.Name(),
					DeltaType:            mapStr,
					IsMapNested:          true,
					MapRemovedName:       "Removed" + field.Name(),
					MapKeyType:           keyStr,
					MapValueUseReflectEq: !types.Comparable(mapT.Elem()),
				}
				nv.Fields = append(nv.Fields, fv)
				nv.DiffFields = append(nv.DiffFields, fv)
				if fv.MapValueUseReflectEq {
					nv.NeedsReflect = true
				}
				continue
			}
			named, ok := field.Type().(*types.Named)
			if !ok {
				return nestedTypeView{}, nil, fmt.Errorf(
					"nested type %s field %s: delta.nested requires a named type",
					typeName, field.Name())
			}
			subTypeName := named.Obj().Name()
			qualifiedSubTypeName := types.TypeString(named, qualifier)
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

			if inPath[subTypeName] {
				return nestedTypeView{}, nil, fmt.Errorf(
					"delta.nested type chain forms a cycle at %s (§3.3.2)", subTypeName)
			}
			if !visited[subTypeName] {
				visited[subTypeName] = true
				inPath[subTypeName] = true
				subSt, _ := named.Underlying().(*types.Struct)
				subView, subExtra, err := buildNestedTypeView(subTypeName, qualifiedSubTypeName, subSt, qualifier, emitMethod, visited, inPath)
				delete(inPath, subTypeName)
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
		// ShapePointer (*T) uses nil-equivalence + dereferenced-value comparison (R-27,
		// E-02). Other non-comparable types use reflect.DeepEqual; scalars and simple
		// structs use != directly.
		if shape == ShapePointer {
			fv.IsPointer = true
			pointeeT := field.Type().Underlying().(*types.Pointer).Elem()
			if !types.Comparable(pointeeT) {
				fv.PointeeUseReflectEq = true
				nv.NeedsReflect = true
			}
		} else if !types.Comparable(field.Type()) {
			fv.UseReflectEq = true
			nv.NeedsReflect = true
		}
		nv.Fields = append(nv.Fields, fv)
		nv.DiffFields = append(nv.DiffFields, fv)
	}

	return nv, additional, nil
}

// buildClearableFieldView handles the delta.nested+delta.clearable case (CL-05..07,
// E-17/E-23). It mutates sv in place, appending to sv.Fields, sv.DiffFields, and
// sv.NestedTypes as appropriate. The three shapes diverge in inner type and wrapper:
//   - struct: inner = <SubType>Delta; reuses N-01 recursion for dedup.
//   - map: inner = <FieldName>MapDelta; appends a nestedKindMapWrapper view.
//   - slice: inner = <FieldName>SliceDelta; appends a nestedKindSliceWrapper view.
//
// Validation (Clearable ⟹ Nested, Nested ⟹ composite) is already done upstream.
func buildClearableFieldView(
	f ParsedField,
	sv *snapshotView,
	qualifier types.Qualifier,
	emitMethod bool,
	visited map[string]bool,
	inPath map[string]bool,
) error {
	switch f.Shape {
	case ShapeSlice:
		sliceT := f.GoType.Underlying().(*types.Slice)
		sliceStr := types.TypeString(f.GoType, qualifier)
		elemStr := types.TypeString(sliceT.Elem(), qualifier)
		useReflect := !types.Comparable(sliceT.Elem())
		innerName := f.Name + "SliceDelta"
		fv := fieldView{
			Name:                   f.Name,
			DeltaName:              f.Name,
			DeltaType:              "runtime.FieldDelta[" + innerName + "]",
			IsClearable:            true,
			ClearableInner:         innerName,
			ClearableIsStruct:      false,
			ClearableZeroComposite: "nil",
			ClearableApplyFunc:     "Apply" + innerName,
			ClearableDiffFunc:      "Diff" + innerName,
		}
		sv.Fields = append(sv.Fields, fv)
		sv.DiffFields = append(sv.DiffFields, fv)
		if useReflect {
			sv.NeedsReflect = true
		}
		sv.NestedTypes = append(sv.NestedTypes, nestedTypeView{
			Kind:                nestedKindSliceWrapper,
			IsSliceWrapper:      true,
			DeltaName:           innerName,
			ApplyFuncName:       "Apply" + innerName,
			DiffFuncName:        "Diff" + innerName,
			WrapperUpdatedName:  "Added" + f.Name,
			WrapperRemovedName:  "Removed" + f.Name,
			WrapperSliceType:    sliceStr,
			WrapperSliceElem:    elemStr,
			WrapperUseReflectEq: useReflect,
		})

	case ShapeMap:
		mapT := f.GoType.Underlying().(*types.Map)
		keyStr := types.TypeString(mapT.Key(), qualifier)
		mapStr := types.TypeString(f.GoType, qualifier)
		useReflect := !types.Comparable(mapT.Elem())
		innerName := f.Name + "MapDelta"
		fv := fieldView{
			Name:                   f.Name,
			DeltaName:              f.Name,
			DeltaType:              "runtime.FieldDelta[" + innerName + "]",
			IsClearable:            true,
			ClearableInner:         innerName,
			ClearableIsStruct:      false,
			ClearableZeroComposite: "nil",
			ClearableApplyFunc:     "Apply" + innerName,
			ClearableDiffFunc:      "Diff" + innerName,
		}
		sv.Fields = append(sv.Fields, fv)
		sv.DiffFields = append(sv.DiffFields, fv)
		if useReflect {
			sv.NeedsReflect = true
		}
		sv.NestedTypes = append(sv.NestedTypes, nestedTypeView{
			Kind:                nestedKindMapWrapper,
			IsMapWrapper:        true,
			DeltaName:           innerName,
			ApplyFuncName:       "Apply" + innerName,
			DiffFuncName:        "Diff" + innerName,
			WrapperUpdatedName:  "Updated" + f.Name,
			WrapperRemovedName:  "Removed" + f.Name,
			WrapperMapType:      mapStr,
			WrapperMapKeyType:   keyStr,
			WrapperUseReflectEq: useReflect,
		})

	default: // struct-value shape
		named, ok := f.GoType.(*types.Named)
		if !ok {
			return fmt.Errorf(
				"field %s: delta.nested+delta.clearable requires a named struct type", f.Name)
		}
		subTypeName := named.Obj().Name()
		qualifiedSub := types.TypeString(named, qualifier)
		eqReflect := !types.Comparable(f.GoType)
		fv := fieldView{
			Name:                     f.Name,
			DeltaName:                f.Name,
			DeltaType:                "runtime.FieldDelta[" + subTypeName + "Delta]",
			IsClearable:              true,
			ClearableInner:           subTypeName + "Delta",
			ClearableIsStruct:        true,
			ClearableZeroComposite:   qualifiedSub + "{}",
			ClearableApplyFunc:       "Apply" + subTypeName,
			ClearableDiffFunc:        "Diff" + subTypeName,
			ClearableStructEqReflect: eqReflect,
		}
		sv.Fields = append(sv.Fields, fv)
		sv.DiffFields = append(sv.DiffFields, fv)
		if eqReflect {
			sv.NeedsReflect = true
		}
		// Funnel through N-01 recursion so FooDelta/ApplyFoo/DiffFoo are emitted
		// exactly once (deduped vs any plain delta.nested use of the same type).
		if inPath[subTypeName] {
			return fmt.Errorf(
				"field %s: delta.nested+delta.clearable type chain forms a cycle at %s (§3.3.2)",
				f.Name, subTypeName)
		}
		if !visited[subTypeName] {
			visited[subTypeName] = true
			inPath[subTypeName] = true
			subSt, _ := named.Underlying().(*types.Struct)
			subView, subExtra, err := buildNestedTypeView(subTypeName, qualifiedSub, subSt, qualifier, emitMethod, visited, inPath)
			delete(inPath, subTypeName)
			if err != nil {
				return fmt.Errorf("field %s: %w", f.Name, err)
			}
			if subView.NeedsReflect {
				sv.NeedsReflect = true
			}
			sv.NestedTypes = append(sv.NestedTypes, subExtra...)
			sv.NestedTypes = append(sv.NestedTypes, subView)
		}
	}
	return nil
}

// buildSnapshotView constructs the template view for one ParsedSnapshot.
//
// delta.nested on struct-value shapes triggers N-01 compositional emission:
// a companion UDelta type + ApplyU/DiffU functions are collected in
// sv.NestedTypes (bottom-up order). delta.nested on slice/map shapes
// returns a sentinel error referencing N-03/N-04. (delta.clearable is
// handled in buildClearableFieldView, CL-05..07.)
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
	inPath := make(map[string]bool)  // active DFS ancestry chain for cycle detection (N-02)

	for _, f := range ps.Fields {
		// Presence-axis: omit/retired fields are suppressed on the Delta side
		// but still appear in Fields so the Apply template emits result.F = s.F.
		if f.Tag.Kind == TagKindOmit || f.Tag.Kind == TagKindRetired {
			sv.Fields = append(sv.Fields, fieldView{Name: f.Name, Suppressed: true})
			continue
		}

		// N-01/N-03/N-04: delta.nested struct-value → companion type (N-01);
		// map → UpdatedX/RemovedX encoding (N-03); slice → AddedX/RemovedX set-diff (N-04).
		// CL-05..07: delta.nested+delta.clearable → FieldDelta[<inner>] envelope.
		if f.Tag.Kind == TagKindNested {
			if f.Tag.Clearable {
				if err := buildClearableFieldView(f, &sv, qualifier, emitMethod, visited, inPath); err != nil {
					return snapshotView{}, err
				}
				continue
			}
			if f.Shape == ShapeSlice {
				sliceT := f.GoType.Underlying().(*types.Slice)
				sliceStr := types.TypeString(f.GoType, qualifier)
				elemStr := types.TypeString(sliceT.Elem(), qualifier)
				fv := fieldView{
					Name:                  f.Name,
					DeltaName:             "Added" + f.Name,
					DeltaType:             sliceStr,
					IsSliceNested:         true,
					SliceRemovedName:      "Removed" + f.Name,
					SliceElemType:         elemStr,
					SliceElemUseReflectEq: !types.Comparable(sliceT.Elem()),
				}
				sv.Fields = append(sv.Fields, fv)
				sv.DiffFields = append(sv.DiffFields, fv)
				if fv.SliceElemUseReflectEq {
					sv.NeedsReflect = true
				}
				continue
			}
			if f.Shape == ShapeMap {
				mapT := f.GoType.Underlying().(*types.Map)
				keyStr := types.TypeString(mapT.Key(), qualifier)
				mapStr := types.TypeString(f.GoType, qualifier)
				fv := fieldView{
					Name:                 f.Name,
					DeltaName:            "Updated" + f.Name,
					DeltaType:            mapStr,
					IsMapNested:          true,
					MapRemovedName:       "Removed" + f.Name,
					MapKeyType:           keyStr,
					MapValueUseReflectEq: !types.Comparable(mapT.Elem()),
				}
				sv.Fields = append(sv.Fields, fv)
				sv.DiffFields = append(sv.DiffFields, fv)
				if fv.MapValueUseReflectEq {
					sv.NeedsReflect = true
				}
				continue
			}
			named, ok := f.GoType.(*types.Named)
			if !ok {
				return snapshotView{}, fmt.Errorf(
					"field %s.%s: delta.nested requires a named type (anonymous struct types are not supported)",
					ps.Name, f.Name)
			}
			subTypeName := named.Obj().Name()
			qualifiedSubTypeName := types.TypeString(named, qualifier)
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

			if inPath[subTypeName] {
				return snapshotView{}, fmt.Errorf(
					"field %s.%s: delta.nested type chain forms a cycle at %s (§3.3.2)",
					ps.Name, f.Name, subTypeName)
			}
			if !visited[subTypeName] {
				visited[subTypeName] = true
				inPath[subTypeName] = true
				subSt, _ := named.Underlying().(*types.Struct)
				subView, subExtra, err := buildNestedTypeView(subTypeName, qualifiedSubTypeName, subSt, qualifier, emitMethod, visited, inPath)
				delete(inPath, subTypeName)
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
		// ShapePointer (*T) uses nil-equivalence + dereferenced-value comparison (R-27,
		// E-02). Other non-comparable types use reflect.DeepEqual; scalars and simple
		// structs use != directly.
		if f.Shape == ShapePointer {
			fv.IsPointer = true
			pointeeT := f.GoType.Underlying().(*types.Pointer).Elem()
			if !types.Comparable(pointeeT) {
				fv.PointeeUseReflectEq = true
				sv.NeedsReflect = true
			}
		} else if !types.Comparable(f.GoType) {
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
