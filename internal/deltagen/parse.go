package deltagen

// parse.go implements the second stage of the delta-gen pipeline: parsing a
// named Snapshot struct into a structured description that the tag-handling
// (T-01 through T-03) and emit (Phase 4) stages consume.
//
// # The parse pipeline
//
// Callers invoke `parseSnapshot` once per target struct, passing a `ParseOpts`
// carrier that conveys cross-package mode and (in G-04 onward) the optional
// CLI key-field override. Internally parseSnapshot runs four steps:
//
//  1. headerTypeFor — resolve the runtime.Header type from the transitive
//     package closure. The Header field is recognised by *type identity*, not
//     by name, so that aliased imports
//     ("import eddt go.resystems.io/eddt/runtime") work correctly.
//
//  2. findNamedStruct — locate the named struct in the top-level packages'
//     scopes. The struct must exist and be a named struct type.
//
//  3. walkFields — walk the struct's fields exactly once, separating the
//     single embedded runtime.Header from the candidate payload fields.
//     In cross-package mode (ParseOpts.CrossPackage == true), unexported
//     fields are silently dropped because they are inaccessible from
//     outside the source package (E-12). Each candidate field's Go type is
//     classified into one of five shapes (scalar, pointer, struct value,
//     slice, map); function, channel, and interface fields are rejected.
//
//  4. Result assembly — the ParsedSnapshot is returned with HeaderVar and
//     Fields populated. KeyVar is left nil; the G-04 parseKeyField step
//     (added by a later refinement) will identify the entity.key field
//     among the candidates and move it out of Fields into KeyVar.
//
// # What this file does NOT do
//
// Tag parsing and tag-combination validation are separate concerns delivered
// by T-01 through T-03. This file records only the raw eddt: tag string so
// those stages can act on it. Key-field semantic validation (presence,
// struct-of-comparable-fields) is delivered by G-04.
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
	"reflect"

	"golang.org/x/tools/go/packages"
)

// runtimePkgImportPath is the canonical import path for the eddt runtime
// package. It is used to locate runtime.Header for type-identity comparison.
const runtimePkgImportPath = "go.resystems.io/eddt/runtime"

// FieldShape classifies the structural Go type shape of a Snapshot payload
// field. The classification drives Delta-side field generation in the emit
// stage and tag-combination validation in T-02.
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
	// combination with delta.omit; that constraint is enforced by T-02.
	ShapeMap
)

// ParsedField describes one payload field of a Snapshot struct as returned by
// the parse stage. It carries enough information for both the tag-handling
// stage (T-01 through T-03, which inspects RawTag and Shape) and the emit
// stage (Phase 4, which uses GoType and Var for type-string rendering).
type ParsedField struct {
	// Name is the Go field identifier, preserving the source case. Used
	// verbatim by the emit stage to derive Delta field names (e.g. SetName).
	Name string

	// RawTag is the raw value of the eddt: struct tag, empty when the field
	// carries no eddt: tag. Tag parsing and validation are performed by G-04
	// and T-01 through T-03; this stage only records the raw string.
	RawTag string

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
	// Nil after parseSnapshot returns in the current refinement step (G-07);
	// G-04 will populate it as part of an internal parseKeyField step and
	// move the corresponding entry out of Fields.
	KeyVar *types.Var

	// Fields is the list of payload fields in source declaration order,
	// with the embedded Header and (in cross-package mode) unexported fields
	// already removed. Once G-04 lands, the entity.key field is also excluded
	// from Fields and surfaced via KeyVar instead.
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
	// the source package (E-12). It instructs walkFields to silently drop
	// unexported fields, which would otherwise be inaccessible from the
	// generated code.
	CrossPackage bool

	// KeyFieldOverride names the field in the Snapshot struct that should
	// be treated as the entity-key field, bypassing the eddt:"entity.key"
	// tag scan. The empty string selects tag-based discovery.
	//
	// Populated by the CLI layer in G-06 from --key-field; consumed by
	// G-04's parseKeyField step. G-07 (this refinement) carries the field
	// through the signature but does not act on it; the value is ignored
	// until G-04 lands.
	KeyFieldOverride string
}

// parseSnapshot resolves and parses the Snapshot struct named structName from
// the loaded packages. It is the single top-level entry point for the parse
// stage; the caller never needs to invoke any other helper from this file.
//
// pkgs must be the result of loadPackages (NeedDeps set) so that the eddt
// runtime package is reachable via FindPkgByPath. opts carries cross-package
// mode and (post-G-04) the optional CLI key-field override.
//
// Returned ParsedSnapshot has HeaderVar and Fields populated. KeyVar is nil
// at this refinement step (G-07); a subsequent refinement (G-04) will add a
// parseKeyField step that fills KeyVar and removes the key from Fields.
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
	// from candidate payload fields. walkFields applies cross-package
	// unexported-field filtering and rejects unsupported field shapes.
	st := named.Underlying().(*types.Struct)
	headerVar, fields, err := walkFields(st, structName, headerType, opts)
	if err != nil {
		return nil, err
	}

	// Step 4: require exactly one embedded Header. walkFields rejects more
	// than one; the remaining failure mode is total absence.
	if headerVar == nil {
		return nil, fmt.Errorf(
			"struct %q has no embedded runtime.Header field; a conforming Snapshot must embed exactly one Header",
			structName)
	}

	// G-04 hook: a future refinement will call parseKeyField here to identify
	// the entity.key field among `fields` and move it into KeyVar. Until then
	// the override carried by opts.KeyFieldOverride is ignored; the field is
	// retained as a no-op to keep the call-site signature stable across the
	// G-07 → G-04 transition.

	return &ParsedSnapshot{
		Name:      structName,
		PkgPath:   pkg.PkgPath,
		PkgName:   pkg.Name,
		HeaderVar: headerVar,
		Fields:    fields,
	}, nil
}

// walkFields walks the fields of st exactly once, returning the embedded
// runtime.Header field separately from the candidate payload fields. It is
// the internal helper that step 3 of parseSnapshot delegates to.
//
// Responsibilities:
//
//   - Identify the embedded runtime.Header field by type identity (compared
//     against headerType). Multiple Header fields are an error.
//   - In cross-package mode (opts.CrossPackage), silently drop unexported
//     fields — they are inaccessible from the generated code.
//   - Classify each non-Header field's Go type via classifyShape and reject
//     unsupported shapes (function, channel, interface).
//   - Capture each candidate's raw eddt: tag string verbatim for downstream
//     consumers (G-04 key-field discovery, T-01 tag parsing).
//
// The candidate slice may include a field tagged eddt:"entity.key"; G-04's
// parseKeyField will subsequently remove it. walkFields itself is tag-blind
// — it does not interpret the captured RawTag strings.
//
// structName is supplied only for error-message context; it is not used in
// any structural decision.
func walkFields(
	st *types.Struct,
	structName string,
	headerType types.Type,
	opts ParseOpts,
) (header *types.Var, fields []ParsedField, err error) {
	for i := 0; i < st.NumFields(); i++ {
		field := st.Field(i)

		// Cross-package mode: drop unexported fields. They are inaccessible
		// from outside the source package and must not appear in generated code.
		if opts.CrossPackage && !field.Exported() {
			continue
		}

		// Identify the embedded runtime.Header field by type identity.
		// Multiple Headers are a generation-time error (E-10 / R-12).
		if types.Identical(field.Type(), headerType) {
			if header != nil {
				return nil, nil, fmt.Errorf(
					"struct %q has multiple embedded runtime.Header fields; exactly one is required",
					structName)
			}
			header = field
			continue
		}

		// Classify the payload field's structural Go-type shape.
		shape, err := classifyShape(field.Type())
		if err != nil {
			return nil, nil, fmt.Errorf("field %s.%s: %w", structName, field.Name(), err)
		}

		// Capture the raw eddt: tag string verbatim. Tag parsing is a
		// separate concern (G-04 / T-01); walkFields stores only the string.
		rawTag := reflect.StructTag(st.Tag(i)).Get("eddt")

		fields = append(fields, ParsedField{
			Name:   field.Name(),
			RawTag: rawTag,
			Shape:  shape,
			GoType: field.Type(),
			Var:    field,
		})
	}

	return header, fields, nil
}

// headerTypeFor returns the types.Type for runtime.Header by resolving the
// eddt runtime package from the transitive package closure.
//
// The runtime package is a dependency of any conforming Snapshot package and
// is therefore reachable via FindPkgByPath when NeedDeps was set during load.
// If it is not found (because the source package does not import it), the
// returned error guides the user to add the dependency.
func headerTypeFor(pkgs []*packages.Package) (types.Type, error) {
	// Locate the runtime package in the full transitive closure.
	rp := FindPkgByPath(pkgs, runtimePkgImportPath)
	if rp == nil {
		return nil, fmt.Errorf(
			"could not find %s in loaded packages; "+
				"ensure the source package imports go.resystems.io/eddt/runtime",
			runtimePkgImportPath)
	}

	// Look up the Header type name in the package's top-level scope.
	obj := rp.Types.Scope().Lookup("Header")
	if obj == nil {
		return nil, fmt.Errorf("runtime.Header not found in package scope of %s", runtimePkgImportPath)
	}

	return obj.Type(), nil
}

// findNamedStruct searches the top-level packages for a type named name that
// is a struct. It returns the *types.Named, the containing package, and an
// error if the name is absent or does not refer to a struct type.
//
// Only the top-level (non-dependency) packages are searched, because the
// target Snapshot type must be in one of the packages the user passed via
// --pkg. Dependency packages (transitive closure loaded by NeedDeps) are not
// searched.
func findNamedStruct(pkgs []*packages.Package, name string) (*types.Named, *packages.Package, error) {
	for _, pkg := range pkgs {
		// Look up the name in the package's top-level declaration scope.
		obj := pkg.Types.Scope().Lookup(name)
		if obj == nil {
			continue
		}

		// The name must resolve to a type name (not a var, func, or const).
		typeName, ok := obj.(*types.TypeName)
		if !ok {
			return nil, nil, fmt.Errorf("%q in package %q is not a type", name, pkg.PkgPath)
		}

		// The type must be a named type (not a type alias to a built-in).
		named, ok := typeName.Type().(*types.Named)
		if !ok {
			return nil, nil, fmt.Errorf("%q in package %q is not a named type", name, pkg.PkgPath)
		}

		// The underlying type must be a struct.
		if _, ok := named.Underlying().(*types.Struct); !ok {
			return nil, nil, fmt.Errorf("%q in package %q is not a struct type", name, pkg.PkgPath)
		}

		return named, pkg, nil
	}

	return nil, nil, fmt.Errorf("struct %q not found in any loaded package", name)
}

// classifyShape returns the FieldShape for a payload field type t.
//
// Classification is driven by the type's underlying type (t.Underlying()), so
// that named types (e.g. type Status int32) are correctly classified by their
// structural nature rather than their name. The one exception is pointers:
// *T already has *types.Pointer as its underlying type.
//
// Function, channel, and interface types are rejected as unsupported; all
// other non-listed types (e.g. tuple, union) are also rejected. Map types are
// accepted and classified as ShapeMap; the tag-combination constraint (maps
// are only valid with delta.omit) is enforced by T-02.
func classifyShape(t types.Type) (FieldShape, error) {
	switch t.Underlying().(type) {
	case *types.Basic:
		// Bare basic types (bool, int32, string, …) and named types whose
		// underlying type is basic (e.g. type Status int32, type ID string).
		return ShapeScalar, nil

	case *types.Struct:
		// Named struct types (time.Time, LocationInfo, …) and any anonymous
		// struct literal. These are value-type struct fields; delta.nested
		// opts into recursive companion-type generation (T-02).
		return ShapeStructValue, nil

	case *types.Pointer:
		// Pointer to any type: *Foo, *int32, *time.Time, etc.
		return ShapePointer, nil

	case *types.Slice:
		// Slice of any element type: []BearerRef, []string, etc.
		return ShapeSlice, nil

	case *types.Map:
		// Map types are classified but not immediately rejected; their
		// validity depends on the delta.omit tag (checked by T-02).
		return ShapeMap, nil

	case *types.Signature:
		return 0, fmt.Errorf("function-valued fields are not supported by delta-gen (§3.2)")

	case *types.Chan:
		return 0, fmt.Errorf("channel fields are not supported by delta-gen (§3.2)")

	case *types.Interface:
		return 0, fmt.Errorf("interface-typed fields are not supported by delta-gen (§3.2)")

	default:
		return 0, fmt.Errorf("unsupported field type %T (not in delta-gen §3.2 payload shape catalogue)", t.Underlying())
	}
}
