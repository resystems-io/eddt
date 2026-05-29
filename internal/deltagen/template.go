package deltagen

// template.go implements the code-emission stage (Phase 4) for delta-gen.
// It provides text/template-driven generation of the TDelta companion struct
// (R-DG-015) and, in later Phase-4 items, the Apply, Diff, Coalesce, and
// EntityID function bodies (R-DG-012, R-DG-013, R-DG-014, R-DG-034).
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
//     logic (R-DG-012, R-DG-013). Only non-comparable types (slice, map, complex structs)
//     trigger reflect; comparable types including pointers use !=.
//   - executeEmit orchestrates build → execute → go/format → WriteFile,
//     called by generator.go's emitStage.
//
// # Extending for R-DG-012, R-DG-013, R-DG-014, R-DG-034
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

// Prefix / suffix constants for generated field and function names. Using these
// ensures every site agrees on spelling and makes mechanical renames safe.
const (
	prefixSet     = "Set"
	prefixAdded   = "Added"
	prefixUpdated = "Updated"
	prefixRemoved = "Removed"
	prefixApply   = "Apply"
	prefixDiff    = "Diff"
	suffixDelta   = "Delta"
)

// fieldViewShape is a discriminator for the five mutually exclusive rendering
// paths on a fieldView. Using a single typed constant instead of five parallel
// booleans (IsClearable/IsSliceNested/IsMapNested/IsNested/IsPointer) eliminates
// the invalid "two trues at once" state and collapses parallel template ladders
// to a single {{if eq .Shape "..."}} chain.
type fieldViewShape string

const (
	fieldShapeAtomic    fieldViewShape = "atomic"      // untagged / commutative; SetX *T field
	fieldShapePointer   fieldViewShape = "pointer"     // *T atomic; nil-equivalence + deref comparison
	fieldShapeNested    fieldViewShape = "nested"      // delta.nested struct companion
	fieldShapeSlice     fieldViewShape = "sliceNested" // delta.nested []T set-diff
	fieldShapeMap       fieldViewShape = "mapNested"   // delta.nested map[K]V upsert/remove
	fieldShapeClearable fieldViewShape = "clearable"   // delta.nested+delta.clearable FieldDelta[T]
)

// ── View types ────────────────────────────────────────────────────────────────

// templateData is the top-level input to the delta template. Fields are stable
// across R-DG-015 through R-DG-034 so sub-templates added in later items can reuse them.
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
// the default) and the net-new map/slice wrapper types introduced for R-DG-016..07
// clearable emission (nestedKindMapWrapper, nestedKindSliceWrapper). The zero
// value (nestedKindStruct) preserves byte-identical emission for all existing
// nested struct companions.
type nestedKind int

const (
	nestedKindStruct       nestedKind = iota // existing R-DG-016 struct companion
	nestedKindMapWrapper                     // R-DG-016: <X>MapDelta wrapper for clearable map field
	nestedKindSliceWrapper                   // R-DG-016: <X>SliceDelta wrapper for clearable slice field
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
	// nestedKindSliceWrapper (R-DG-016..07). The algorithm mirrors the existing
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

	// SourceField is the source Snapshot field name that triggered this wrapper
	// type's emission (e.g. "Tags"). Populated only for nestedKindMapWrapper and
	// nestedKindSliceWrapper; empty for nestedKindStruct companions.
	SourceField string

	// SourceParent is the source Snapshot type name that owns SourceField
	// (e.g. "ClearableCompositeSnapshot"). Populated alongside SourceField.
	SourceParent string
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
	// Used in Apply to emit "result.<KeyName> = s.<KeyName>" (R-DG-012, R-DG-013).
	KeyName string

	// KeyTypeName is the bare (unqualified) type name of the entity-key field,
	// e.g. "UEKey", "IMSI", or "string" for a raw-basic key (R-DG-034).
	KeyTypeName string

	// KeyQualifier is the package-qualifier prefix for the key type in cross-
	// package mode (e.g. "model."). Empty in same-package mode or when the key
	// type is an unnamed basic (e.g. raw string). Set alongside Qualifier in
	// executeEmit (R-DG-034, R-DG-012, R-DG-013, R-DG-019).
	KeyQualifier string

	// KeyHashLines is the ordered list of runtime.Write* call strings for the
	// EntityID function body (R-DG-034). One line for a scalar key; one line per
	// exported sub-field in source order for a struct key.
	KeyHashLines []string

	// EmitEntityIDMethod is true when the EntityID method wrapper should be
	// emitted on the key type: same-package mode AND the key type is a named
	// type (Go forbids methods on unnamed basic types). When false, only the
	// package-level EntityID function is emitted (R-DG-034, R-DG-012, R-DG-014, R-DG-012, R-DG-013, R-DG-019).
	EmitEntityIDMethod bool

	// EmitMethod is true when the output package matches the source package
	// (R-DG-012, R-DG-013, R-DG-019). When true, the template emits same-package method wrappers that
	// delegate to the package-level Apply, Diff, and Coalesce functions
	// (R-DG-012, R-DG-013, R-DG-012, R-DG-013, R-DG-012, R-DG-013).
	EmitMethod bool

	// NeedsReflect is true when at least one DiffFields entry uses
	// reflect.DeepEqual for its comparison (R-DG-012, R-DG-013). executeEmit uses this to
	// inject a "reflect" import only when needed.
	NeedsReflect bool

	// Fields is the ordered list of payload fields in source declaration order
	// (excluding the entity-key field extracted into KeyName). Suppressed fields
	// (delta.omit / delta.retired) are included with Suppressed: true so the
	// Apply template can emit result.F = s.F propagation assignments (R-DG-012, R-DG-013).
	Fields []fieldView

	// DiffFields is the subset of Fields that have a Delta-side representation
	// (i.e. non-suppressed fields). The Diff template iterates DiffFields so
	// that suppressed fields produce no body line (R-DG-012, R-DG-013).
	DiffFields []fieldView

	// NestedTypes holds companion views for delta.nested struct-value fields,
	// in bottom-up order (deepest companion type first). Emitted before the
	// parent TDelta declaration so forward references are avoided (R-DG-016).
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

	// Shape is the discriminator for the five mutually exclusive rendering paths.
	// The zero value (empty string) applies to suppressed fields where the shape
	// is irrelevant (gated out by {{if not .Suppressed}} in all templates).
	Shape fieldViewShape

	// Suppressed is true for delta.omit and delta.retired fields. The
	// Delta-side field is absent from TDelta but Apply still propagates the
	// source value via result.F = s.F (R-DG-012, R-DG-013). Suppressed fields are excluded
	// from DiffFields and therefore produce no Diff body line (R-DG-012, R-DG-013).
	Suppressed bool

	// UseReflectEq is true when the Diff template must use reflect.DeepEqual
	// because the field's Go type is not comparable (e.g. slice, map, or a struct
	// containing a slice/map). Scalars and simple structs use != directly.
	// Pointer fields (*T) are handled separately via Shape==fieldShapePointer regardless
	// of types.Comparable — they always use nil-equivalence + dereferenced comparison.
	UseReflectEq bool

	// PointeeUseReflectEq is true when the pointee type T of a *T field is not
	// comparable (e.g. a struct containing a slice), so the deref comparison must
	// use reflect.DeepEqual(*a.X, *b.X). Only meaningful when Shape==fieldShapePointer.
	PointeeUseReflectEq bool

	// NestedFuncName is the package-level Apply function to call for cross-package
	// nested fields (e.g. "ApplyInner"). Empty in same-package mode, where the
	// method wrapper is used instead (s.F.Apply(d.F)). Only when Shape==fieldShapeNested.
	NestedFuncName string

	// NestedDiffFuncName is the package-level Diff function for cross-package
	// nested fields (e.g. "DiffInner"). Empty in same-package mode. Only when Shape==fieldShapeNested.
	NestedDiffFuncName string

	// MapRemovedName is the Delta-side field name for the removed-keys slice
	// (e.g. "RemovedTags" for a source field named Tags). Only when Shape==fieldShapeMap.
	MapRemovedName string

	// MapKeyType is the rendered Go type string for the map key K
	// (e.g. "string"). Used to declare the RemovedX []K field. Only when Shape==fieldShapeMap.
	MapKeyType string

	// MapValueUseReflectEq is true when the map value type V is not comparable
	// (e.g. a struct containing a slice/map) and Diff must use reflect.DeepEqual
	// for value comparison. Set by !types.Comparable(mapT.Elem()). Only when Shape==fieldShapeMap.
	MapValueUseReflectEq bool

	// SliceRemovedName is the Delta-side field name for removed elements
	// (e.g. "RemovedNames" for a source field named Names). Only when Shape==fieldShapeSlice.
	SliceRemovedName string

	// SliceElemType is the rendered Go element type string (e.g. "string", "Tag").
	// Used as the map key type in the O(n) comparable-element path. Only when Shape==fieldShapeSlice.
	SliceElemType string

	// SliceElemUseReflectEq is true when the slice element type is not comparable
	// (§5.2) and the O(n²) reflect.DeepEqual fallback must be used instead of the
	// O(n) map[T]struct{} set path. Set by !types.Comparable(sliceT.Elem()). Only when Shape==fieldShapeSlice.
	SliceElemUseReflectEq bool

	// ── Clearable-envelope fields (R-DG-016..07, R-DG-007, R-DG-016/R-DG-007) ─────────────────────
	//
	// The following fields are only meaningful when Shape==fieldShapeClearable.
	// The parent Delta carries `X runtime.FieldDelta[ClearableInner]` (single field).

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

	// SourceTypeStr is the rendered Go type string of the source Snapshot field
	// (e.g. "string", "[]Tag", "map[string]string", "Address"). Used in generated
	// doc comments to link each emitted delta field back to its origin.
	SourceTypeStr string
}

// ── Template ─────────────────────────────────────────────────────────────────

// deltaTemplateStr is the text/template source for the generated Delta file.
// R-DG-012, R-DG-013 scope: type declarations (R-DG-015) + Apply function and method wrapper.
// R-DG-012, R-DG-013 scope: Diff function and method wrapper.
// R-DG-012, R-DG-013 scope: Coalesce function and method wrapper.
// R-DG-034 scope: EntityID function and method wrapper on the key type.
// R-DG-016 scope:  companion Delta types and Apply/Diff for delta.nested struct fields.
//
// Sub-template inventory:
//   - applyFunc:         package-level Apply function (always emitted).
//   - applyField:        per-field Apply contribution (atomic, suppressed, or nested).
//   - applyMethod:       same-package method wrapper delegating to Apply (R-DG-012, R-DG-013, R-DG-019).
//   - diffFunc:          package-level Diff function (always emitted).
//   - diffField:         per-field Diff contribution (non-suppressed fields only).
//   - diffMethod:        same-package method wrapper delegating to Diff (R-DG-012, R-DG-013, R-DG-019).
//   - coalesceFunc:      package-level Coalesce function (always emitted).
//   - coalesceMethod:    same-package method wrapper delegating to Coalesce (R-DG-012, R-DG-013, R-DG-019).
//   - entityIDFunc:      package-level EntityID function (always emitted, R-DG-034).
//   - entityIDMethod:    same-package method wrapper on the key type (R-DG-012, R-DG-013, R-DG-019, R-DG-034);
//     emitted only when the key type is a named type (EmitEntityIDMethod).
//   - nestedTypeDecl:      companion Delta struct for a delta.nested type (R-DG-016).
//   - nestedApplyFunc:     package-level ApplyU function for a nested type (R-DG-016).
//   - nestedApplyMethod:   same-package method wrapper func (u U) Apply(...) (R-DG-016).
//   - nestedApplyField:    per-field body line for nestedApplyFunc (uses u. receiver).
//   - nestedDiffFunc:      package-level DiffU function for a nested type (R-DG-016).
//   - nestedDiffMethod:    same-package method wrapper func (u U) Diff(...) (R-DG-016).
//   - applyMapField:         apply body block for a delta.nested map field (R-DG-016, uses s.).
//   - nestedApplyMapField:   apply body block for a map field inside a nested type (R-DG-016, uses u.).
//   - diffMapField:          diff body block for a delta.nested map field (R-DG-016, R-DG-006, R-DG-016).
//   - applySliceField:       apply body block for a delta.nested slice field (R-DG-016, R-DG-028, uses s.).
//   - nestedApplySliceField: apply body block for a slice field inside a nested type (R-DG-016, R-DG-028, uses u.).
//   - diffSliceField:        diff body block for a delta.nested slice field (R-DG-016, R-DG-028, R-DG-006, R-DG-016).
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
	// Header: chain-lifecycle envelope (ChainID, Sequence, Provenance).
	runtime.Header
{{- template "fieldDeclsRange" dict "Fields" .Fields "ParentName" .Name}}
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
// non-nil error. See delta-gen-spec.md §6.4 / §7.1 (Errata R-DG-012).
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

{{define "applyField"}}{{if .Suppressed}}result.{{.Name}} = s.{{.Name}}{{else if eq .Shape "clearable"}}{{template "applyClearableField" .}}{{else if eq .Shape "nested"}}{{if .NestedFuncName}}result.{{.Name}} = {{.NestedFuncName}}(s.{{.Name}}, d.{{.DeltaName}}){{else}}result.{{.Name}} = s.{{.Name}}.Apply(d.{{.DeltaName}}){{end}}{{else if eq .Shape "sliceNested"}}{{template "applySliceFieldBody" dict "F" . "Recv" "s"}}{{else if eq .Shape "mapNested"}}{{template "applyMapFieldBody" dict "F" . "Recv" "s"}}{{else}}if d.{{.DeltaName}} != nil { result.{{.Name}} = *d.{{.DeltaName}} } else { result.{{.Name}} = s.{{.Name}} }{{end}}{{end -}}

{{define "applyMethod"}}
// Apply is an ergonomic same-package wrapper that delegates to the
// package-level Apply function (R-DG-012, R-DG-013, R-DG-019).
func (s {{.Name}}) Apply(d {{.DeltaName}}) ({{.Name}}, error) {
	return Apply(s, d)
}
{{end -}}

{{define "diffFunc"}}
// Diff produces the minimal Delta d such that Apply(a, d) payload-equals b.
// It is a pure function; chain-envelope validations live in
// runtime.HeaderForDiff and are propagated to the caller as a non-nil error.
// See delta-gen-spec.md §6.5 / §7.2 (Errata R-DG-012).
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

{{define "diffField"}}{{if eq .Shape "clearable"}}{{template "diffClearableField" .}}{{else if eq .Shape "sliceNested"}}{{template "diffSliceField" .}}{{else if eq .Shape "mapNested"}}{{template "diffMapField" .}}{{else if eq .Shape "nested"}}{{if .NestedDiffFuncName}}d.{{.DeltaName}} = {{.NestedDiffFuncName}}(a.{{.Name}}, b.{{.Name}}){{else}}d.{{.DeltaName}} = a.{{.Name}}.Diff(b.{{.Name}}){{end}}{{else if eq .Shape "pointer"}}if !((a.{{.Name}} == nil && b.{{.Name}} == nil) || (a.{{.Name}} != nil && b.{{.Name}} != nil && {{if .PointeeUseReflectEq}}reflect.DeepEqual(*a.{{.Name}}, *b.{{.Name}}){{else}}*a.{{.Name}} == *b.{{.Name}}{{end}})) { d.{{.DeltaName}} = &b.{{.Name}} }{{else if .UseReflectEq}}if !reflect.DeepEqual(a.{{.Name}}, b.{{.Name}}) { d.{{.DeltaName}} = &b.{{.Name}} }{{else}}if a.{{.Name}} != b.{{.Name}} { d.{{.DeltaName}} = &b.{{.Name}} }{{end}}{{end -}}

{{define "diffMethod"}}
// Diff is an ergonomic same-package wrapper that delegates to the
// package-level Diff function (R-DG-012, R-DG-013, R-DG-019).
func (a {{.Name}}) Diff(b {{.Name}}) ({{.DeltaName}}, error) {
	return Diff(a, b)
}
{{end -}}

{{define "coalesceFunc"}}
// Coalesce folds a slice of TDeltas into s by iterated Apply. It is a pure
// function; chain-envelope validations propagate from the first failing Apply
// step. An empty slice returns (s, nil) without any runtime call. See
// delta-gen-spec.md §7.3 / §8.3 (Errata R-DG-012, R-DG-012).
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
// package-level Coalesce function (R-DG-012, R-DG-013, R-DG-019).
func (s {{.Name}}) Coalesce(ds []{{.DeltaName}}) ({{.Name}}, error) {
	return Coalesce(s, ds)
}
{{end -}}

{{define "entityIDFunc"}}
// EntityID creates a hash for the key field: {{.KeyName}}
//
// It returns the deterministic content-hash EntityID of k. The hash is
// Blake2b-256 over the canonical encoding of k's fields (R-DG-034, R-DG-035, RT-02). It is
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
// package-level EntityID function (R-DG-012, R-DG-013, R-DG-019).
func (k {{.KeyTypeName}}) EntityID() runtime.EntityID {
	return EntityID(k)
}
{{end}}
{{define "nestedTypeDecl"}}
{{- $nested := .}}// {{.DeltaName}} is the Delta companion type for delta.nested fields of
// type {{.Name}}. It is generated by delta-gen and must not be edited.
type {{.DeltaName}} struct {
{{- template "fieldDeclsRange" dict "Fields" .Fields "ParentName" .Name}}
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
// Apply is an ergonomic same-package wrapper (R-DG-012, R-DG-013, R-DG-019).
func (u {{.Name}}) Apply(d {{.DeltaName}}) {{.Name}} { return {{.ApplyFuncName}}(u, d) }
{{end -}}

{{define "nestedApplyField"}}{{if .Suppressed}}result.{{.Name}} = u.{{.Name}}{{else if eq .Shape "nested"}}{{if .NestedFuncName}}result.{{.Name}} = {{.NestedFuncName}}(u.{{.Name}}, d.{{.DeltaName}}){{else}}result.{{.Name}} = u.{{.Name}}.Apply(d.{{.DeltaName}}){{end}}{{else if eq .Shape "sliceNested"}}{{template "applySliceFieldBody" dict "F" . "Recv" "u"}}{{else if eq .Shape "mapNested"}}{{template "applyMapFieldBody" dict "F" . "Recv" "u"}}{{else}}if d.{{.DeltaName}} != nil { result.{{.Name}} = *d.{{.DeltaName}} } else { result.{{.Name}} = u.{{.Name}} }{{end}}{{end -}}

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
// Diff is an ergonomic same-package wrapper (R-DG-012, R-DG-013, R-DG-019).
func (u {{.Name}}) Diff(other {{.Name}}) {{.DeltaName}} { return {{.DiffFuncName}}(u, other) }
{{end -}}

{{define "applyMapFieldBody"}}
// apply delta.nested map field {{.F.Name}} (R-DG-016):
// 1. copy source map, 2. delete removed keys, 3. upsert updated entries.
{
	__m := make({{.F.DeltaType}}, len({{.Recv}}.{{.F.Name}}))
	for __k, __v := range {{.Recv}}.{{.F.Name}} { __m[__k] = __v }
	for _, __k := range d.{{.F.MapRemovedName}} { delete(__m, __k) }
	for __k, __v := range d.{{.F.DeltaName}} { __m[__k] = __v }
	result.{{.F.Name}} = __m
}{{end -}}

{{define "diffMapField"}}
// diff delta.nested map field {{.Name}} (R-DG-016, R-DG-006, R-DG-016 upsert semantics):
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

{{define "applySliceFieldBody"}}
// apply delta.nested slice field {{.F.Name}} (R-DG-016, R-DG-028, R-DG-006, R-DG-016 set-diff semantics):
// 1. filter removed elements, 2. append added elements (R-DG-028 survivor order).
{{- if .F.SliceElemUseReflectEq}}
// Element type is not comparable: O(n²) reflect.DeepEqual fallback (§5.2).
{
	__src := {{.Recv}}.{{.F.Name}}
	if len(d.{{.F.SliceRemovedName}}) > 0 {
		__out := make({{.F.DeltaType}}, 0, len(__src))
		for _, __v := range __src {
			__keep := true
			for _, __r := range d.{{.F.SliceRemovedName}} {
				if reflect.DeepEqual(__r, __v) { __keep = false; break }
			}
			if __keep { __out = append(__out, __v) }
		}
		__src = __out
	}
	result.{{.F.Name}} = append(__src, d.{{.F.DeltaName}}...)
}
{{- else}}
// Element type is comparable: O(n) map[T]struct{} membership set.
{
	__src := {{.Recv}}.{{.F.Name}}
	if len(d.{{.F.SliceRemovedName}}) > 0 {
		__rem := make(map[{{.F.SliceElemType}}]struct{}, len(d.{{.F.SliceRemovedName}}))
		for _, __r := range d.{{.F.SliceRemovedName}} { __rem[__r] = struct{}{} }
		__out := make({{.F.DeltaType}}, 0, len(__src))
		for _, __v := range __src {
			if _, __ok := __rem[__v]; !__ok { __out = append(__out, __v) }
		}
		__src = __out
	}
	result.{{.F.Name}} = append(__src, d.{{.F.DeltaName}}...)
}
{{- end}}{{end -}}

{{define "diffSliceField"}}
// diff delta.nested slice field {{.Name}} (R-DG-016, R-DG-028, R-DG-006, R-DG-016 set-diff semantics):
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

{{define "fieldDeclsRange"}}{{$p := .ParentName}}{{range .Fields}}{{if not .Suppressed}}{{if eq .Shape "clearable"}}
	// {{.Name}}: source {{$p}}.{{.Name}} ({{.SourceTypeStr}}) — tri-state envelope (OpIgnore / OpAssert / OpRetract); inner Apply via {{.ClearableApplyFunc}}.
	{{.DeltaName}} {{.DeltaType}}
{{- else if eq .Shape "sliceNested"}}
	// {{.Name}}: source {{$p}}.{{.Name}} ({{.SourceTypeStr}}) — compositional slice delta — elements present in b absent in a.
	{{.DeltaName}} {{.DeltaType}}
	// (continues compositional slice delta for {{$p}}.{{.Name}}) — elements present in a absent in b.
	{{.SliceRemovedName}} {{.DeltaType}}
{{- else if eq .Shape "mapNested"}}
	// {{.Name}}: source {{$p}}.{{.Name}} ({{.SourceTypeStr}}) — compositional map delta — entries to upsert (insert or overwrite).
	{{.DeltaName}} {{.DeltaType}}
	// (continues compositional map delta for {{$p}}.{{.Name}}) — keys to delete.
	{{.MapRemovedName}} []{{.MapKeyType}}
{{- else if eq .Shape "nested"}}
	// {{.Name}}: source {{$p}}.{{.Name}} ({{.SourceTypeStr}}) — compositional delta; recursive Apply.
	{{.DeltaName}} {{.DeltaType}}
{{- else if eq .Shape "pointer"}}
	// {{.Name}}: source {{$p}}.{{.Name}} ({{.SourceTypeStr}}) — atomic replace of *T; outer nil = no change, inner nil = clear.
	{{.DeltaName}} {{.DeltaType}}
{{- else}}
	// {{.Name}}: source {{$p}}.{{.Name}} ({{.SourceTypeStr}}) — atomic replace; nil = no change.
	{{.DeltaName}} {{.DeltaType}}
{{- end}}{{end}}{{end}}{{end -}}

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
		if b.{{.Name}} == nil {
			d.{{.DeltaName}} = runtime.FieldDelta[{{.ClearableInner}}]{Op: runtime.OpRetract}
		} else {
			d.{{.DeltaName}} = runtime.FieldDelta[{{.ClearableInner}}]{Op: runtime.OpAssert, Value: __inner}
		}
	}
}{{end}}{{end -}}

{{define "mapWrapper"}}
// {{.DeltaName}} is the clearable-envelope inner type for {{.SourceParent}}.{{.SourceField}}
// (delta.nested + delta.clearable map field). It carries the per-entry upsert/remove delta.
type {{.DeltaName}} struct {
	// {{.WrapperUpdatedName}}: source {{.SourceParent}}.{{.SourceField}} ({{.WrapperMapType}}) — entries to upsert (insert or overwrite). Cleared when the parent envelope's Op is OpRetract.
	{{.WrapperUpdatedName}} {{.WrapperMapType}}
	// {{.WrapperRemovedName}}: source {{.SourceParent}}.{{.SourceField}} ({{.WrapperMapType}}) — keys to delete.
	{{.WrapperRemovedName}} []{{.WrapperMapKeyType}}
}

// IsEmpty reports whether the delta has no changes.
// Used by the Diff three-branch predicate to decide between OpIgnore and OpAssert/OpRetract.
func (w {{.DeltaName}}) IsEmpty() bool {
	return len(w.{{.WrapperUpdatedName}}) == 0 && len(w.{{.WrapperRemovedName}}) == 0
}

// {{.ApplyFuncName}} applies w to prior, returning the resulting map (R-DG-016).
func {{.ApplyFuncName}}(prior {{.WrapperMapType}}, w {{.DeltaName}}) {{.WrapperMapType}} {
	__m := make({{.WrapperMapType}}, len(prior))
	for __k, __v := range prior { __m[__k] = __v }
	for _, __k := range w.{{.WrapperRemovedName}} { delete(__m, __k) }
	for __k, __v := range w.{{.WrapperUpdatedName}} { __m[__k] = __v }
	return __m
}

// {{.DiffFuncName}} computes the minimal {{.DeltaName}} such that {{.ApplyFuncName}}(a, d) value-equals b (R-DG-016, R-DG-006, R-DG-016).
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
// {{.DeltaName}} is the clearable-envelope inner type for {{.SourceParent}}.{{.SourceField}}
// (delta.nested + delta.clearable slice field). It carries the set-difference delta.
type {{.DeltaName}} struct {
	// {{.WrapperUpdatedName}}: source {{.SourceParent}}.{{.SourceField}} ({{.WrapperSliceType}}) — elements present in b absent in a. Cleared when the parent envelope's Op is OpRetract.
	{{.WrapperUpdatedName}} {{.WrapperSliceType}}
	// {{.WrapperRemovedName}}: source {{.SourceParent}}.{{.SourceField}} ({{.WrapperSliceType}}) — elements present in a absent in b.
	{{.WrapperRemovedName}} {{.WrapperSliceType}}
}

// IsEmpty reports whether the delta has no changes.
func (w {{.DeltaName}}) IsEmpty() bool {
	return len(w.{{.WrapperUpdatedName}}) == 0 && len(w.{{.WrapperRemovedName}}) == 0
}
{{if .WrapperUseReflectEq}}// {{.ApplyFuncName}} applies w to prior (R-DG-016, R-DG-028, R-DG-006, R-DG-016); O(n²) reflect.DeepEqual fallback (§5.2).
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
// {{.DiffFuncName}} computes the minimal {{.DeltaName}} such that {{.ApplyFuncName}}(a, d) set-equals b (R-DG-016, R-DG-028, R-DG-006, R-DG-016); O(n²).
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
{{- else}}// {{.ApplyFuncName}} applies w to prior (R-DG-016, R-DG-028, R-DG-006, R-DG-016); O(n) map[T]struct{} membership set.
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
// {{.DiffFuncName}} computes the minimal {{.DeltaName}} such that {{.ApplyFuncName}}(a, d) set-equals b (R-DG-016, R-DG-028, R-DG-006, R-DG-016); O(n).
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
	// package (R-DG-012, R-DG-013, R-DG-019), requiring type-reference qualification.
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
		key, val, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		m[key] = val
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

// fieldSource normalises the per-field inputs to buildFieldView so the shared
// dispatch logic is independent of whether the field comes from a ParsedField
// (snapshot path) or a types.Var + raw-tag pair (nested-type path).
type fieldSource struct {
	Name  string
	Typ   types.Type
	Tag   ParsedTag
	Shape FieldShape
}

// fieldBuildCtx carries the shared context for buildFieldView.
type fieldBuildCtx struct {
	qualifier  types.Qualifier
	emitMethod bool
	parentName string          // enclosing struct name, used in error messages
	visited    map[string]bool // R-DG-016 dedup set: prevents duplicate companion emission
	inPath     map[string]bool // R-DG-009 cycle-detection: active DFS ancestry chain
}

// fieldBuildResult is returned by buildFieldView.
//
// For struct-value nested fields (IsNested == true) the caller is responsible
// for cycle detection and for recursing into SubNamed via buildNestedTypeView;
// the SubTypeName and SubNamed fields carry the information needed to do so.
type fieldBuildResult struct {
	FV           fieldView
	Suppressed   bool
	NeedsReflect bool
	// Set for delta.nested struct-value fields; the caller must perform cycle
	// detection, recursion, and companion accumulation.
	IsNested    bool
	SubTypeName string
	SubNamed    *types.Named
}

// buildFieldView is the unified five-shape field dispatch shared by
// buildSnapshotView and buildNestedTypeView.
//
//   - suppressed   (omit/retired): FV.Suppressed = true.
//   - slice-nested (delta.nested on []T): set-diff fieldView (AddedX/RemovedX).
//   - map-nested   (delta.nested on map[K]V): UpdatedX/RemovedX fieldView.
//   - struct-nested(delta.nested on T): fieldView + IsNested flag; caller handles
//     cycle detection and recursion.
//   - atomic       (untagged / commutative): pointer-wrap fieldView.
//
// Callers are responsible for routing clearable fields (delta.nested +
// delta.clearable) before calling this function.
func buildFieldView(src fieldSource, ctx fieldBuildCtx) (fieldBuildResult, error) {
	// Suppressed.
	if src.Tag.Kind == TagKindOmit || src.Tag.Kind == TagKindRetired {
		return fieldBuildResult{FV: fieldView{Name: src.Name, Suppressed: true}, Suppressed: true}, nil
	}

	if src.Tag.Kind == TagKindNested {
		switch src.Shape {
		case ShapeSlice:
			sliceT := src.Typ.Underlying().(*types.Slice)
			sliceStr := types.TypeString(src.Typ, ctx.qualifier)
			elemStr := types.TypeString(sliceT.Elem(), ctx.qualifier)
			fv := fieldView{
				Shape:                 fieldShapeSlice,
				Name:                  src.Name,
				DeltaName:             prefixAdded + src.Name,
				DeltaType:             sliceStr,
				SliceRemovedName:      prefixRemoved + src.Name,
				SliceElemType:         elemStr,
				SliceElemUseReflectEq: !types.Comparable(sliceT.Elem()),
				SourceTypeStr:         sliceStr,
			}
			return fieldBuildResult{FV: fv, NeedsReflect: fv.SliceElemUseReflectEq}, nil

		case ShapeMap:
			mapT := src.Typ.Underlying().(*types.Map)
			keyStr := types.TypeString(mapT.Key(), ctx.qualifier)
			mapStr := types.TypeString(src.Typ, ctx.qualifier)
			fv := fieldView{
				Shape:                fieldShapeMap,
				Name:                 src.Name,
				DeltaName:            prefixUpdated + src.Name,
				DeltaType:            mapStr,
				MapRemovedName:       prefixRemoved + src.Name,
				MapKeyType:           keyStr,
				MapValueUseReflectEq: !types.Comparable(mapT.Elem()),
				SourceTypeStr:        mapStr,
			}
			return fieldBuildResult{FV: fv, NeedsReflect: fv.MapValueUseReflectEq}, nil

		default: // ShapeStructValue
			named, ok := src.Typ.(*types.Named)
			if !ok {
				return fieldBuildResult{}, fmt.Errorf("delta.nested requires a named type")
			}
			subTypeName := named.Obj().Name()
			qualifiedSub := types.TypeString(named, ctx.qualifier)
			nestedFuncName, nestedDiffFuncName := "", ""
			if !ctx.emitMethod {
				nestedFuncName = prefixApply + subTypeName
				nestedDiffFuncName = prefixDiff + subTypeName
			}
			fv := fieldView{
				Shape:              fieldShapeNested,
				Name:               src.Name,
				DeltaName:          src.Name,
				DeltaType:          subTypeName + suffixDelta,
				NestedFuncName:     nestedFuncName,
				NestedDiffFuncName: nestedDiffFuncName,
				SourceTypeStr:      qualifiedSub,
			}
			return fieldBuildResult{FV: fv, IsNested: true, SubTypeName: subTypeName, SubNamed: named}, nil
		}
	}

	// Atomic (TagKindNone or TagKindCommutative): pointer-wrap the source type.
	deltaType := types.TypeString(types.NewPointer(src.Typ), ctx.qualifier)
	fv := fieldView{
		Name:          src.Name,
		DeltaName:     prefixSet + src.Name,
		DeltaType:     deltaType,
		SourceTypeStr: types.TypeString(src.Typ, ctx.qualifier),
	}
	needsReflect := false
	// ShapePointer (*T): nil-equivalence + dereferenced-value comparison (R-DG-016, R-DG-016).
	if src.Shape == ShapePointer {
		fv.Shape = fieldShapePointer
		pointeeT := src.Typ.Underlying().(*types.Pointer).Elem()
		if !types.Comparable(pointeeT) {
			fv.PointeeUseReflectEq = true
			needsReflect = true
		}
	} else {
		fv.Shape = fieldShapeAtomic
		if !types.Comparable(src.Typ) {
			fv.UseReflectEq = true
			needsReflect = true
		}
	}
	return fieldBuildResult{FV: fv, NeedsReflect: needsReflect}, nil
}

// buildNestedTypeView constructs the template view for one delta.nested
// companion type U. It recursively visits any delta.nested sub-fields of U,
// collecting their companion views in bottom-up order (deepest first) so that
// forward references are avoided in the generated output.
//
// visited prevents duplicate emission when multiple fields share the same
// nested type (R-DG-016). inPath tracks the active DFS ancestry chain; an
// entry already in inPath signals a cycle and returns an error (R-DG-009 §3.3.2).
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
		Name:          qualifiedTypeName, // qualified in cross-package mode (R-DG-012, R-DG-013, R-DG-019)
		DeltaName:     typeName + suffixDelta,
		ApplyFuncName: prefixApply + typeName,
		DiffFuncName:  prefixDiff + typeName,
		EmitMethod:    emitMethod,
	}

	ctx := fieldBuildCtx{
		qualifier:  qualifier,
		emitMethod: emitMethod,
		parentName: typeName,
		visited:    visited,
		inPath:     inPath,
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

		if tag.Clearable {
			return nestedTypeView{}, nil, fmt.Errorf(
				"nested type %s field %s: eddt:%q inside a delta.nested type is not yet supported",
				typeName, field.Name(), tagDeltaClearable)
		}

		shape, err := classifyShape(field.Type())
		if err != nil {
			return nestedTypeView{}, nil, fmt.Errorf(
				"nested type %s field %s: %w", typeName, field.Name(), err)
		}

		src := fieldSource{Name: field.Name(), Typ: field.Type(), Tag: tag, Shape: shape}
		res, err := buildFieldView(src, ctx)
		if err != nil {
			return nestedTypeView{}, nil, fmt.Errorf("nested type %s field %s: %w", typeName, field.Name(), err)
		}

		if res.Suppressed {
			nv.Fields = append(nv.Fields, res.FV)
			continue
		}

		// Struct-nested: caller owns cycle detection and recursion.
		if res.IsNested {
			nv.Fields = append(nv.Fields, res.FV)
			nv.DiffFields = append(nv.DiffFields, res.FV)
			subTypeName := res.SubTypeName
			named := res.SubNamed
			if inPath[subTypeName] {
				return nestedTypeView{}, nil, fmt.Errorf(
					"delta.nested type chain forms a cycle at %s (§3.3.2)", subTypeName)
			}
			if !visited[subTypeName] {
				visited[subTypeName] = true
				inPath[subTypeName] = true
				subSt, _ := named.Underlying().(*types.Struct)
				qualifiedSub := types.TypeString(named, qualifier)
				subView, subExtra, err := buildNestedTypeView(subTypeName, qualifiedSub, subSt, qualifier, emitMethod, visited, inPath)
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

		nv.Fields = append(nv.Fields, res.FV)
		nv.DiffFields = append(nv.DiffFields, res.FV)
		if res.NeedsReflect {
			nv.NeedsReflect = true
		}
	}

	return nv, additional, nil
}

// buildClearableFieldView handles the delta.nested+delta.clearable case (R-DG-016..07,
// R-DG-007, R-DG-016/R-DG-007). It mutates sv in place, appending to sv.Fields, sv.DiffFields, and
// sv.NestedTypes as appropriate. The three shapes diverge in inner type and wrapper:
//   - struct: inner = <SubType>Delta; reuses R-DG-016 recursion for dedup.
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
		innerName := f.Name + "Slice" + suffixDelta
		fv := fieldView{
			Name:                   f.Name,
			DeltaName:              f.Name,
			DeltaType:              "runtime.FieldDelta[" + innerName + "]",
			Shape:                  fieldShapeClearable,
			ClearableInner:         innerName,
			ClearableIsStruct:      false,
			ClearableZeroComposite: "nil",
			ClearableApplyFunc:     prefixApply + innerName,
			ClearableDiffFunc:      prefixDiff + innerName,
			SourceTypeStr:          sliceStr,
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
			ApplyFuncName:       prefixApply + innerName,
			DiffFuncName:        prefixDiff + innerName,
			WrapperUpdatedName:  prefixAdded + f.Name,
			WrapperRemovedName:  prefixRemoved + f.Name,
			WrapperSliceType:    sliceStr,
			WrapperSliceElem:    elemStr,
			WrapperUseReflectEq: useReflect,
			SourceField:         f.Name,
			SourceParent:        sv.Name,
		})

	case ShapeMap:
		mapT := f.GoType.Underlying().(*types.Map)
		keyStr := types.TypeString(mapT.Key(), qualifier)
		mapStr := types.TypeString(f.GoType, qualifier)
		useReflect := !types.Comparable(mapT.Elem())
		innerName := f.Name + "Map" + suffixDelta
		fv := fieldView{
			Name:                   f.Name,
			DeltaName:              f.Name,
			DeltaType:              "runtime.FieldDelta[" + innerName + "]",
			Shape:                  fieldShapeClearable,
			ClearableInner:         innerName,
			ClearableIsStruct:      false,
			ClearableZeroComposite: "nil",
			ClearableApplyFunc:     prefixApply + innerName,
			ClearableDiffFunc:      prefixDiff + innerName,
			SourceTypeStr:          mapStr,
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
			ApplyFuncName:       prefixApply + innerName,
			DiffFuncName:        prefixDiff + innerName,
			WrapperUpdatedName:  prefixUpdated + f.Name,
			WrapperRemovedName:  prefixRemoved + f.Name,
			WrapperMapType:      mapStr,
			WrapperMapKeyType:   keyStr,
			WrapperUseReflectEq: useReflect,
			SourceField:         f.Name,
			SourceParent:        sv.Name,
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
			DeltaType:                "runtime.FieldDelta[" + subTypeName + suffixDelta + "]",
			Shape:                    fieldShapeClearable,
			ClearableInner:           subTypeName + suffixDelta,
			ClearableIsStruct:        true,
			ClearableZeroComposite:   qualifiedSub + "{}",
			ClearableApplyFunc:       prefixApply + subTypeName,
			ClearableDiffFunc:        prefixDiff + subTypeName,
			ClearableStructEqReflect: eqReflect,
			SourceTypeStr:            qualifiedSub,
		}
		sv.Fields = append(sv.Fields, fv)
		sv.DiffFields = append(sv.DiffFields, fv)
		if eqReflect {
			sv.NeedsReflect = true
		}
		// Funnel through R-DG-016 recursion so FooDelta/ApplyFoo/DiffFoo are emitted
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
// delta.nested on struct-value shapes triggers R-DG-016 compositional emission:
// a companion UDelta type + ApplyU/DiffU functions are collected in
// sv.NestedTypes (bottom-up order). delta.nested on slice/map shapes
// returns a sentinel error referencing R-DG-016/R-DG-016, R-DG-028. (delta.clearable is
// handled in buildClearableFieldView, R-DG-016..07.)
//
// emitMethod gates same-package method wrappers (R-DG-012, R-DG-013, R-DG-019): pass true for
// same-package output, false for cross-package.
//
// Suppressed fields (delta.omit / delta.retired) are included in sv.Fields
// with Suppressed: true so the Apply template can emit result.F = s.F
// propagation assignments (R-DG-012, R-DG-013). The Delta-side type declaration template
// gates on {{not .Suppressed}} to keep them out of TDelta.
//
// Each admitted atomic field's DeltaType is rendered via types.TypeString on a
// single pointer-wrap of the source GoType:
//
//	scalar T        → *T
//	pointer *T      → **T
//	struct value T  → *T      (atomic, untagged)
//	slice []T       → *[]T    (atomic per R-DG-006, R-DG-016)
//	map[K]V         → *map[K]V (atomic per R-DG-006, R-DG-016)
//
// The caller must pass a qualifier derived from buildImports so that foreign
// packages are recorded as a side effect of type rendering.
func buildSnapshotView(ps *ParsedSnapshot, qualifier types.Qualifier, emitMethod bool) (snapshotView, error) {
	sv := snapshotView{
		Name:      ps.Name,
		DeltaName: ps.Name + suffixDelta,
		KeyName:   ps.KeyVar.Name(),
	}

	// Resolve the key type name and hash lines (R-DG-034).
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

	visited := make(map[string]bool) // dedup set for nested companion types (R-DG-016)
	inPath := make(map[string]bool)  // active DFS ancestry chain for cycle detection (R-DG-009)

	ctx := fieldBuildCtx{
		qualifier:  qualifier,
		emitMethod: emitMethod,
		parentName: ps.Name,
		visited:    visited,
		inPath:     inPath,
	}

	for _, f := range ps.Fields {
		// R-DG-016..07: delta.nested+delta.clearable → FieldDelta[<inner>] envelope.
		// Handled before the shared dispatch since it accumulates directly into sv.
		if f.Tag.Kind == TagKindNested && f.Tag.Clearable {
			if err := buildClearableFieldView(f, &sv, qualifier, emitMethod, visited, inPath); err != nil {
				return snapshotView{}, err
			}
			continue
		}

		src := fieldSource{Name: f.Name, Typ: f.GoType, Tag: f.Tag, Shape: f.Shape}
		res, err := buildFieldView(src, ctx)
		if err != nil {
			return snapshotView{}, fmt.Errorf("field %s.%s: %w", ps.Name, f.Name, err)
		}

		if res.Suppressed {
			sv.Fields = append(sv.Fields, res.FV)
			continue
		}

		// Struct-nested: caller owns cycle detection and recursion (R-DG-016/R-DG-009).
		if res.IsNested {
			sv.Fields = append(sv.Fields, res.FV)
			sv.DiffFields = append(sv.DiffFields, res.FV)
			subTypeName := res.SubTypeName
			named := res.SubNamed
			if inPath[subTypeName] {
				return snapshotView{}, fmt.Errorf(
					"field %s.%s: delta.nested type chain forms a cycle at %s (§3.3.2)",
					ps.Name, f.Name, subTypeName)
			}
			if !visited[subTypeName] {
				visited[subTypeName] = true
				inPath[subTypeName] = true
				subSt, _ := named.Underlying().(*types.Struct)
				qualifiedSub := types.TypeString(named, qualifier)
				subView, subExtra, err := buildNestedTypeView(subTypeName, qualifiedSub, subSt, qualifier, emitMethod, visited, inPath)
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

		sv.Fields = append(sv.Fields, res.FV)
		// DiffFields excludes suppressed fields so the Diff template emits no body
		// line for them (R-DG-012, R-DG-013).
		sv.DiffFields = append(sv.DiffFields, res.FV)
		if res.NeedsReflect {
			sv.NeedsReflect = true
		}
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
//  4. Inject the "reflect" import if any view has NeedsReflect set (R-DG-012, R-DG-013),
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

	// emitMethod gates same-package method wrappers (R-DG-012, R-DG-013, R-DG-019); precomputed once.
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

		// EmitMethod gates the same-package method wrappers (R-DG-012, R-DG-013, R-DG-019).
		sv.EmitMethod = emitMethod

		// EmitEntityIDMethod additionally requires the key type to be a named
		// type: Go forbids defining methods on unnamed basic types (R-DG-034, R-DG-012, R-DG-014).
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
