package deltagen

// parse.go implements the second stage of the delta-gen pipeline: parsing a
// named Snapshot struct into a structured description that the tag-handling
// (R-DG-004, R-DG-005 through R-DG-006, R-DG-007) and emit (Phase 4) stages consume.
//
// # The parse pipeline
//
// Callers invoke `parseSnapshot` once per target struct, passing a `ParseOpts`
// carrier that conveys cross-package mode and the optional CLI key-field
// override. Internally parseSnapshot runs five steps:
//
//  1. headerTypeFor — resolve the runtime.Header type from the transitive
//     package closure. The Header field is recognised by *type identity*, not
//     by name, so that aliased imports
//     ("import eddt go.resystems.io/eddt/runtime") work correctly.
//
//  2. findNamedStruct — locate the named struct in the top-level packages'
//     scopes. The struct must exist and be a named struct type.
//
//  3. parseFields — walk the struct's fields exactly once, separating the
//     single embedded runtime.Header from the candidate payload fields.
//     In cross-package mode (ParseOpts.CrossPackage == true), unexported
//     fields are silently dropped because they are inaccessible from
//     outside the source package (R-DG-012, R-DG-013, R-DG-019). Each candidate field's Go type is
//     classified into one of five shapes (scalar, pointer, struct value,
//     slice, map); function, channel, and interface fields are rejected.
//
//  4. parseKeyField — identify the entity.key field among the candidates,
//     either by tag scan (default) or by ParseOpts.KeyFieldOverride. The
//     chosen field must be a value-typed comparable type — a scalar (basic
//     or named basic) or a struct of all-comparable fields. The key is
//     moved out of the candidate list and surfaced via KeyVar so that the
//     emit stage's payload loops do not need to filter it.
//
//  5. Result assembly — the ParsedSnapshot is returned with HeaderVar,
//     KeyVar, and Fields (payload only) populated.
//
// # What this file does NOT do
//
// Tag parsing and tag-combination validation are separate concerns delivered
// by R-DG-004, R-DG-005 through R-DG-006, R-DG-007. This file records only the raw eddt: tag string so
// those stages can act on it. Key-field semantic validation (presence,
// comparable type) is delivered by parseKeyField in this file.
//
// # Exported surface
//
// Only the result types (`ParsedSnapshot`, `ParsedField`, `FieldShape` and
// its constants) and the options carrier (`ParseOpts`) are exported. All
// functions are package-private. The exported types are consumed by
// generator.go and by the emit stage.

import (
	"fmt"
	"go/types"

	"golang.org/x/tools/go/packages"
)

// FieldShape classifies the structural Go type shape of a Snapshot payload
// field. The classification drives Delta-side field generation in the emit
// stage and tag-combination validation in R-DG-006, R-DG-007.
type FieldShape int

const (
	// ShapeScalar covers bare basic types (bool, int32, string, …) and named
	// types whose underlying type is a basic type (e.g. type Status int32).
	ShapeScalar FieldShape = iota

	// ShapePointer covers all pointer types: *T for any T.
	ShapePointer

	// ShapeStructValue covers named struct types that are not runtime.Header
	// (e.g. time.Time, LocationInfo). These may be tagged delta.nested to
	// generate a companion Delta type.
	ShapeStructValue

	// ShapeSlice covers all slice types: []T for any T.
	ShapeSlice

	// ShapeMap covers all map types: map[K]V. Maps are only valid in
	// combination with delta.omit; that constraint is enforced by R-DG-006, R-DG-007.
	ShapeMap
)

// ParsedField describes one payload field of a Snapshot struct as returned by
// the parse stage. It carries enough information for both the tag-handling
// and emit stages (Phase 4, which uses GoType and Var for type-string
// rendering).
type ParsedField struct {
	// Name is the Go field identifier, preserving the source case. Used
	// verbatim by the emit stage to derive Delta field names (e.g. SetName).
	Name string

	// Tag is the parsed eddt: struct tag for this field. The verbatim source
	// string is preserved in Tag.Raw; downstream stages consume Tag.Kind and
	// Tag.Options rather than re-parsing the raw string.
	Tag ParsedTag

	// Shape is the classified Go type shape of the field.
	Shape FieldShape

	// GoType is the field's resolved type as returned by the type checker.
	// The emit stage uses it to render qualified type expressions in generated
	// code (e.g. *time.Time, []BearerID).
	GoType types.Type

	// Var is the underlying types.Var from the struct definition. Downstream
	// stages use it for package-path lookups and detailed type inspection.
	Var *types.Var
}

// ParsedSnapshot is the result of parsing a single Snapshot struct. It
// contains everything the tag-handling and emit stages need to generate the
// companion Delta type and its associated functions.
type ParsedSnapshot struct {
	// Name is the struct type name (e.g. "UESessionSnapshot").
	Name string

	// PkgPath is the import path of the package that declares the struct.
	// In cross-package mode the emit stage qualifies type references with
	// this path.
	PkgPath string

	// PkgName is the short package name (e.g. "model"), used by the emit
	// stage as the qualifier prefix when CrossPackage is true.
	PkgName string

	// HeaderVar is the types.Var for the embedded runtime.Header field.
	// The emit stage uses it to determine the exact field name used in
	// generated Apply/Diff calls (s.<HeaderVar.Name()>.EntityID, etc.).
	HeaderVar *types.Var

	// KeyVar is the types.Var for the field that carries the
	// eddt:"entity.key" tag (or the field named by ParseOpts.KeyFieldOverride).
	// Always populated for a successful parseSnapshot return; the emit stage
	// uses it to render the EntityID() method and EntityID hash invocations.
	KeyVar *types.Var

	// KeyShape is the structural shape of the entity-key field (ShapeScalar or
	// ShapeStructValue). Used by the emit stage to select the hash strategy for
	// EntityID generation (R-DG-034): one Write* call for scalar keys, one per
	// exported sub-field for struct keys.
	KeyShape FieldShape

	// Fields is the list of payload fields in source declaration order,
	// with the embedded Header, the entity.key field, and (in cross-package
	// mode) unexported fields already removed. The emit stage iterates these
	// directly without any further filtering.
	Fields []ParsedField
}

// ParseOpts is the options carrier accepted by parseSnapshot. It encapsulates
// per-invocation tuning so that the function's positional signature does not
// grow as new parsing concerns land (key-field override in G-04 / G-06,
// future tag-validation hooks, etc.).
//
// The zero value (`ParseOpts{}`) is a valid configuration: same-package mode,
// no override. Callers should construct named-field literals so that future
// additions remain backward-compatible.
type ParseOpts struct {
	// CrossPackage is true when the generator output package differs from
	// the source package (R-DG-012, R-DG-013, R-DG-019). It instructs parseFields to silently drop
	// unexported fields, which would otherwise be inaccessible from the
	// generated code.
	CrossPackage bool

	// KeyFieldOverride names the field in the Snapshot struct that should
	// be treated as the entity-key field, bypassing the eddt:"entity.key"
	// tag scan. The empty string selects tag-based discovery.
	//
	// Populated by the CLI layer in G-06 from --key-field; consumed by
	// parseKeyField. When both a tag and an override name a key field, the
	// override silently wins (the CLI layer emits a --verbose warning).
	KeyFieldOverride string
}

// parseSnapshot resolves and parses the Snapshot struct named structName from
// the loaded packages. It is the single top-level entry point for the parse
// stage; the caller never needs to invoke any other helper from this file.
//
// pkgs must be the result of loadPackages (NeedDeps set) so that the eddt
// runtime package is reachable via FindPkgByPath. opts carries cross-package
// mode and the optional CLI key-field override.
//
// Returned ParsedSnapshot has HeaderVar, KeyVar, and Fields populated. The
// entity.key field is excluded from Fields and surfaced via KeyVar instead,
// so the emit stage's payload loops do not have to filter it.
func parseSnapshot(pkgs []*packages.Package, structName string, opts ParseOpts) (*ParsedSnapshot, error) {
	// Step 1: resolve the runtime.Header type for identity-based recognition.
	// We compare by type identity rather than field name so that aliased
	// imports (e.g. "import eddt go.resystems.io/eddt/runtime") work correctly.
	headerType, err := headerTypeFor(pkgs)
	if err != nil {
		return nil, err
	}

	// Step 2: locate the target struct in the top-level packages.
	named, pkg, err := findNamedStruct(pkgs, structName)
	if err != nil {
		return nil, err
	}

	// Step 3: walk the struct's fields once, separating the Header envelope
	// from candidate payload fields. parseFields applies cross-package
	// unexported-field filtering and rejects unsupported field shapes.
	// The returned candidate list still contains the entity.key field; step 4
	// removes it.
	st := named.Underlying().(*types.Struct)
	headerVar, candidates, err := parseFields(st, structName, headerType, opts)
	if err != nil {
		return nil, err
	}

	// Require exactly one embedded Header. parseFields rejects more than one;
	// the remaining failure mode is total absence.
	if headerVar == nil {
		return nil, fmt.Errorf(
			"struct %q has no embedded runtime.Header field; a conforming Snapshot must embed exactly one Header",
			structName)
	}

	// Step 4: identify and validate the entity.key field, partitioning the
	// candidate list into (keyVar, keyShape, payload fields).
	keyVar, keyShape, fields, err := parseKeyField(candidates, structName, opts)
	if err != nil {
		return nil, err
	}

	// Step 5: assemble the result.
	return &ParsedSnapshot{
		Name:      structName,
		PkgPath:   pkg.PkgPath,
		PkgName:   pkg.Name,
		HeaderVar: headerVar,
		KeyVar:    keyVar,
		KeyShape:  keyShape,
		Fields:    fields,
	}, nil
}
