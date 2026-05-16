package deltagen

// parse.go implements the second stage of the delta-gen pipeline: parsing a
// named Snapshot struct into a structured description that the tag-handling
// (T-01 through T-03) and emit (Phase 4) stages consume.
//
// # What this file does
//
// Given the type-checked packages from the load stage and the name of the
// target Snapshot struct, parseSnapshot:
//
//  1. Resolves the runtime.Header type object from the transitive package
//     closure. The Header field is recognised by type identity, not by name,
//     so that aliased imports ("import eddt go.resystems.io/eddt/runtime")
//     are handled correctly.
//
//  2. Finds the named struct in the top-level packages' scopes. The struct
//     must be exported; an unexported or missing name is an error.
//
//  3. Walks the struct's fields, separating the single embedded runtime.Header
//     from payload fields. Exactly one Header field is required; zero or more
//     than one is a generation-time error.
//
//  4. In cross-package mode (Generator.CrossPackage == true), unexported
//     payload fields are silently dropped before the result is returned,
//     because they are inaccessible from outside the source package (E-12).
//
//  5. Classifies each remaining payload field's Go type into one of five
//     shapes. Unsupported types (function, channel, interface) are rejected
//     with a descriptive error. Map types are classified as ShapeMap and
//     returned without error; the tag-combination constraint (maps require
//     delta.omit) is enforced separately by T-02.
//
// # What this file does NOT do
//
// Tag parsing, tag-combination validation, and entity.key field recognition
// are separate concerns delivered by G-04 (key field parser) and T-01 through
// T-03 (tag handling). This file records only the raw eddt: tag string so
// those stages can act on it.
//
// # Exported surface
//
// Only the result types (ParsedSnapshot, ParsedField, FieldShape and its
// constants) are exported; all functions are package-private. The exported
// types are consumed by generator.go and by the parse and emit stages.

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

	// Fields is the list of payload fields in source declaration order,
	// with the embedded Header and (in cross-package mode) unexported fields
	// already removed.
	Fields []ParsedField
}

// parseSnapshot resolves and parses the Snapshot struct named structName from
// the loaded packages. It is the top-level entry point for the parse stage.
//
// pkgs must be the result of loadPackages (NeedDeps set), so that the eddt
// runtime package is reachable via FindPkgByPath. crossPackage must match
// Generator.CrossPackage so that unexported fields are filtered correctly.
func parseSnapshot(pkgs []*packages.Package, structName string, crossPackage bool) (*ParsedSnapshot, error) {
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

	// Step 3: walk the struct's fields.
	// For each field: if its type is runtime.Header record it as the envelope;
	// otherwise classify its shape and add it to the payload list.
	st := named.Underlying().(*types.Struct)
	var headerVar *types.Var
	var fields []ParsedField

	for i := 0; i < st.NumFields(); i++ {
		field := st.Field(i)

		// Cross-package mode: drop unexported fields — they are inaccessible
		// from outside the source package and must not appear in generated code.
		if crossPackage && !field.Exported() {
			continue
		}

		// Identify the embedded runtime.Header field by type identity.
		if types.Identical(field.Type(), headerType) {
			if headerVar != nil {
				return nil, fmt.Errorf(
					"struct %q has multiple embedded runtime.Header fields; exactly one is required",
					structName)
			}
			headerVar = field
			continue
		}

		// Classify the payload field's shape.
		shape, err := classifyShape(field.Type())
		if err != nil {
			return nil, fmt.Errorf("field %s.%s: %w", structName, field.Name(), err)
		}

		// Extract the raw eddt: struct tag (empty string if no eddt: tag).
		rawTag := reflect.StructTag(st.Tag(i)).Get("eddt")

		fields = append(fields, ParsedField{
			Name:   field.Name(),
			RawTag: rawTag,
			Shape:  shape,
			GoType: field.Type(),
			Var:    field,
		})
	}

	// Step 4: require exactly one embedded Header.
	if headerVar == nil {
		return nil, fmt.Errorf(
			"struct %q has no embedded runtime.Header field; a conforming Snapshot must embed exactly one Header",
			structName)
	}

	return &ParsedSnapshot{
		Name:      structName,
		PkgPath:   pkg.PkgPath,
		PkgName:   pkg.Name,
		HeaderVar: headerVar,
		Fields:    fields,
	}, nil
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
