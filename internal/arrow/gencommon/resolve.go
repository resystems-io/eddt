package gencommon

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"strings"

	"golang.org/x/tools/go/packages"
)

// pkgPathOf returns the import path of the package that defines obj, or ""
// if obj has no associated package (e.g. universe-scope builtins).
func pkgPathOf(obj types.Object) string {
	if obj.Pkg() != nil {
		return obj.Pkg().Path()
	}
	return ""
}

// arrowArrayType derives the concrete Arrow array type from the builder type
// by stripping the "Builder" suffix. E.g. "*array.Int32Builder" → "*array.Int32".
func arrowArrayType(builder string) string {
	return strings.TrimSuffix(builder, "Builder")
}

// unmarshalForMarshal returns the reciprocal unmarshal method for a given marshal method.
// MarshalText → UnmarshalText, MarshalBinary → UnmarshalBinary, String → "" (no inverse).
func unmarshalForMarshal(method string) string {
	switch method {
	case "MarshalText":
		return "UnmarshalText"
	case "MarshalBinary":
		return "UnmarshalBinary"
	default:
		return ""
	}
}

// zeroExprForCast returns the Go zero-value expression for a given cast type.
func zeroExprForCast(castType string) string {
	switch castType {
	case "string":
		return `""`
	case "bool":
		return "false"
	case "[]byte":
		return "nil"
	default:
		// All numeric types (int8..float64, arrow.Timestamp) zero to 0.
		return "0"
	}
}

// -- ast-resolution: AST-based field resolution path.
//
// This path is the primary entry point for struct field resolution. It
// dispatches on AST node type (Ident, StarExpr, ArrayType, MapType,
// SelectorExpr) and delegates to the type checker (via TypesInfo) for
// cross-package types, named types, and selector expressions. For named
// slice/map/array types, it bridges to fieldInfoFromType.

// fieldInfoFromExpr resolves an AST expression to a FieldInfo. This is the
// primary entry point called by Parse for each declared struct field and by
// resolveEmbeddedFields for promoted fields.
func fieldInfoFromExpr(pkg *packages.Package, allPkgs []*packages.Package, name string, expr ast.Expr, queue *[]structRef, processed map[string]bool) (FieldInfo, error) {
	switch t := expr.(type) {
	case *ast.Ident:
		return fieldInfoFromIdent(pkg, allPkgs, name, t, false, queue, processed)

	case *ast.StarExpr:
		// Pointer type - this could be to a struct or a primitive
		if ident, ok := t.X.(*ast.Ident); ok {
			return fieldInfoFromIdent(pkg, allPkgs, name, ident, true, queue, processed)
		}

		// Check for selector expression (e.g. *netip.Addr, *pkg2.Inner)
		if sel, ok := t.X.(*ast.SelectorExpr); ok {
			typ := pkg.TypesInfo.TypeOf(sel)
			if typ != nil {
				// If this is a named struct type from one of the explicitly loaded packages,
				// treat it as a native Arrow struct rather than falling back to marshal methods.
				if named, ok := typ.(*types.Named); ok {
					if _, isStruct := named.Underlying().(*types.Struct); isStruct {
						if named.Obj().Pkg() != nil {
							pkgPath := named.Obj().Pkg().Path()
							if FindPkgByPath(allPkgs, pkgPath) != nil {
								structName := named.Obj().Name()
								pkgName := named.Obj().Pkg().Name()
								return buildStructFieldInfo(name, structName, pkgName, pkgPath, true, queue, processed), nil
							}
						}
					}
				}
				// Check for well-known stdlib types with dedicated Arrow mappings.
				if named, ok := typ.(*types.Named); ok {
					if fi, ok := resolveWellKnownType(name, named, true); ok {
						return fi, nil
					}
				}
				// External type: fall back to marshal method detection.
				method := detectMarshalMethod(typ)
				if method != "" {
					return buildMarshalFieldInfo(name, typ, method, true), nil
				}
				return FieldInfo{}, fmt.Errorf("external type *%s does not implement TextMarshaler, Stringer, or BinaryMarshaler", typ)
			}
		}
		return FieldInfo{}, fmt.Errorf("unsupported pointer type")

	case *ast.ArrayType:
		// Fixed-size array ([N]T) — use Arrow FixedSizeList.
		if t.Len != nil {
			lit, ok := t.Len.(*ast.BasicLit)
			if !ok || lit.Kind != token.INT {
				return FieldInfo{}, fmt.Errorf("fixed-size array length must be an integer literal")
			}

			// []byte special case does not apply to [N]byte — treat as fixed-size list of uint8.
			eltInfo, err := fieldInfoFromExpr(pkg, allPkgs, "", t.Elt, queue, processed)
			if err != nil {
				return FieldInfo{}, fmt.Errorf("fixed-size array element %w", err)
			}

			return buildFixedArrayFieldInfo(name, lit.Value, eltInfo, false), nil
		}

		// []byte is represented as Arrow Binary, not a List of Uint8.
		if eltIdent, ok := t.Elt.(*ast.Ident); ok && eltIdent.Name == "byte" {
			return buildByteSliceFieldInfo(name, false), nil
		}

		// Slice type
		eltInfo, err := fieldInfoFromExpr(pkg, allPkgs, "", t.Elt, queue, processed)
		if err != nil {
			return FieldInfo{}, fmt.Errorf("slice element %w", err)
		}

		return buildSliceFieldInfo(name, eltInfo, false), nil

	case *ast.MapType:
		// Map type
		keyInfo, err := fieldInfoFromExpr(pkg, allPkgs, "", t.Key, queue, processed)
		if err != nil {
			return FieldInfo{}, fmt.Errorf("map key %w", err)
		}
		if keyInfo.IsStruct {
			return FieldInfo{}, fmt.Errorf("struct maps keys are not supported")
		}

		valInfo, err := fieldInfoFromExpr(pkg, allPkgs, "", t.Value, queue, processed)
		if err != nil {
			return FieldInfo{}, fmt.Errorf("map value %w", err)
		}

		return buildMapFieldInfo(name, keyInfo, valInfo, false), nil

	case *ast.SelectorExpr:
		// External package type (e.g., netip.Addr, time.Time, or pkg2.Inner)
		typ := pkg.TypesInfo.TypeOf(t)
		if typ == nil {
			return FieldInfo{}, fmt.Errorf("could not resolve type for selector expression")
		}

		// If this is a named struct type from one of the explicitly loaded packages,
		// treat it as a native Arrow struct rather than falling back to marshal methods.
		if named, ok := typ.(*types.Named); ok {
			if _, isStruct := named.Underlying().(*types.Struct); isStruct {
				if named.Obj().Pkg() != nil {
					pkgPath := named.Obj().Pkg().Path()
					if FindPkgByPath(allPkgs, pkgPath) != nil {
						structName := named.Obj().Name()
						pkgName := named.Obj().Pkg().Name()
						return buildStructFieldInfo(name, structName, pkgName, pkgPath, false, queue, processed), nil
					}
				}
			}
		}

		// Check for well-known stdlib types with dedicated Arrow mappings.
		if named, ok := typ.(*types.Named); ok {
			if fi, ok := resolveWellKnownType(name, named, false); ok {
				return fi, nil
			}
		}

		// External type not in any loaded package: fall back to marshal method detection.
		method := detectMarshalMethod(typ)
		if method == "" {
			return FieldInfo{}, fmt.Errorf("external type %s does not implement TextMarshaler, Stringer, or BinaryMarshaler", typ)
		}
		return buildMarshalFieldInfo(name, typ, method, false), nil
	}

	return FieldInfo{}, fmt.Errorf("unsupported AST expression type: %T", expr)
}

// fieldInfoFromIdent resolves an ast.Ident to a FieldInfo. Handles local struct
// references, named types over primitives, named types over composites
// (delegating to fieldInfoFromType), and bare primitives.
func fieldInfoFromIdent(pkg *packages.Package, allPkgs []*packages.Package, name string, ident *ast.Ident, isPointer bool, queue *[]structRef, processed map[string]bool) (FieldInfo, error) {
	// Check for struct reference or named primitive
	obj := pkg.TypesInfo.ObjectOf(ident)
	if obj != nil {
		if _, ok := obj.Type().Underlying().(*types.Struct); ok {
			structPkgPath := pkg.PkgPath
			structPkgName := pkg.Name
			if obj.Pkg() != nil {
				structPkgPath = obj.Pkg().Path()
				structPkgName = obj.Pkg().Name()
			}
			return buildStructFieldInfo(name, obj.Name(), structPkgName, structPkgPath, isPointer, queue, processed), nil
		}

		// Check for named type over a primitive (e.g., type MyStates int)
		if basic, ok := obj.Type().Underlying().(*types.Basic); ok {
			syntheticIdent := &ast.Ident{Name: basic.Name()}
			_, arrowType, arrowBuilder, castType, err := primitiveArrowType(syntheticIdent)
			if err == nil {
				goTypeName := obj.Name()
				if isPointer {
					goTypeName = "*" + goTypeName
				}
				return FieldInfo{
					Name:           name,
					GoType:         goTypeName,
					ArrowType:      arrowType,
					ArrowBuilder:   arrowBuilder,
					CastType:       castType,
					IsPointer:      isPointer,
					ArrowArrayType: arrowArrayType(arrowBuilder),
					ValueMethod:    "Value",
					ZeroExpr:       zeroExprForCast(castType),
					TypePkgPath:    pkgPathOf(obj),
				}, nil
			}
		}

		// Named slice, map, or array type (e.g., type Tags []string, type Config map[string]int).
		// Resolve the underlying composite via fieldInfoFromType and preserve the named
		// type's name as GoType.
		switch obj.Type().Underlying().(type) {
		case *types.Slice, *types.Map, *types.Array:
			fi, err := fieldInfoFromType(pkg, allPkgs, name, obj.Type().Underlying(), isPointer, queue, processed)
			if err != nil {
				return FieldInfo{}, fmt.Errorf("named type %s: %w", obj.Name(), err)
			}
			fi.GoType = obj.Name()
			if isPointer {
				fi.GoType = "*" + fi.GoType
			}
			fi.TypePkgPath = pkgPathOf(obj)
			return fi, nil
		}
	}

	// Primitive type
	goType, arrowType, arrowBuilder, castType, err := primitiveArrowType(ident)
	if err != nil {
		if isPointer {
			return FieldInfo{}, fmt.Errorf("unsupported pointer type: %w", err)
		}
		return FieldInfo{}, err
	}

	goTypeName := goType
	if isPointer {
		goTypeName = "*" + goTypeName
	}

	return FieldInfo{
		Name:           name,
		GoType:         goTypeName,
		ArrowType:      arrowType,
		ArrowBuilder:   arrowBuilder,
		CastType:       castType,
		IsPointer:      isPointer,
		ArrowArrayType: arrowArrayType(arrowBuilder),
		ValueMethod:    "Value",
		ZeroExpr:       zeroExprForCast(castType),
	}, nil
}

// primitiveArrowType maps a primitive Go AST identifier to its Arrow type
// representation, returning the Go type string, Arrow type string, Builder
// type string, cast type, and an error if unsupported.
func primitiveArrowType(expr ast.Expr) (string, string, string, string, error) {
	ident, ok := expr.(*ast.Ident)
	if !ok {
		return "", "", "", "", fmt.Errorf("complex types not supported in Phase 1 primitives list")
	}

	goType := ident.Name
	var arrowType string
	var arrowBuilder string
	var castType string

	switch goType {
	case "int8":
		arrowType = "arrow.PrimitiveTypes.Int8"
		arrowBuilder = "*array.Int8Builder"
		castType = "int8"
	case "int16":
		arrowType = "arrow.PrimitiveTypes.Int16"
		arrowBuilder = "*array.Int16Builder"
		castType = "int16"
	case "int32", "rune":
		arrowType = "arrow.PrimitiveTypes.Int32"
		arrowBuilder = "*array.Int32Builder"
		castType = "int32"
	case "int64", "int":
		arrowType = "arrow.PrimitiveTypes.Int64"
		arrowBuilder = "*array.Int64Builder"
		castType = "int64"
	case "uint8", "byte":
		arrowType = "arrow.PrimitiveTypes.Uint8"
		arrowBuilder = "*array.Uint8Builder"
		castType = "uint8"
	case "uint16":
		arrowType = "arrow.PrimitiveTypes.Uint16"
		arrowBuilder = "*array.Uint16Builder"
		castType = "uint16"
	case "uint32":
		arrowType = "arrow.PrimitiveTypes.Uint32"
		arrowBuilder = "*array.Uint32Builder"
		castType = "uint32"
	case "uint64", "uint":
		arrowType = "arrow.PrimitiveTypes.Uint64"
		arrowBuilder = "*array.Uint64Builder"
		castType = "uint64"
	case "float32":
		arrowType = "arrow.PrimitiveTypes.Float32"
		arrowBuilder = "*array.Float32Builder"
		castType = "float32"
	case "float64":
		arrowType = "arrow.PrimitiveTypes.Float64"
		arrowBuilder = "*array.Float64Builder"
		castType = "float64"
	case "string":
		arrowType = "arrow.BinaryTypes.String"
		arrowBuilder = "*array.StringBuilder"
		castType = "string"
	case "bool":
		arrowType = "arrow.FixedWidthTypes.Boolean"
		arrowBuilder = "*array.BooleanBuilder"
		castType = "bool"
	default:
		return "", "", "", "", fmt.Errorf("unsupported primitive type: %s", goType)
	}

	return goType, arrowType, arrowBuilder, castType, nil
}

// -- typechecker-resolution: Type-checker-based field resolution path.
//
// This path resolves go/types representations to FieldInfo. It is called by
// fieldInfoFromIdent for named slice/map/array types (where the underlying
// composite structure is available from the type checker but no AST expression
// exists), and recursively for element/key/value types of containers.

// fieldInfoFromType resolves a types.Type to a FieldInfo. Operates purely on
// go/types representations, independent of the AST.
func fieldInfoFromType(pkg *packages.Package, allPkgs []*packages.Package, name string, typ types.Type, isPointer bool, queue *[]structRef, processed map[string]bool) (FieldInfo, error) {
	switch t := typ.(type) {
	case *types.Basic:
		return fieldInfoFromBasic(name, t, isPointer)

	case *types.Named:
		// Well-known types (time.Time, time.Duration, protobuf types).
		if fi, ok := resolveWellKnownType(name, t, isPointer); ok {
			return fi, nil
		}

		switch u := t.Underlying().(type) {
		case *types.Struct:
			if t.Obj().Pkg() != nil {
				pkgPath := t.Obj().Pkg().Path()
				if FindPkgByPath(allPkgs, pkgPath) != nil {
					return buildStructFieldInfo(name, t.Obj().Name(), t.Obj().Pkg().Name(), pkgPath, isPointer, queue, processed), nil
				}
			}
			// External struct — try marshal methods.
			method := detectMarshalMethod(t)
			if method != "" {
				return buildMarshalFieldInfo(name, t, method, isPointer), nil
			}
			return FieldInfo{}, fmt.Errorf("external type %s does not implement TextMarshaler, Stringer, or BinaryMarshaler", t)

		case *types.Basic:
			return fieldInfoFromBasic(name, u, isPointer)

		case *types.Slice, *types.Map, *types.Array:
			return fieldInfoFromType(pkg, allPkgs, name, u, isPointer, queue, processed)

		default:
			method := detectMarshalMethod(t)
			if method != "" {
				return buildMarshalFieldInfo(name, t, method, isPointer), nil
			}
			return FieldInfo{}, fmt.Errorf("unsupported named type %s", t)
		}

	case *types.Pointer:
		return fieldInfoFromType(pkg, allPkgs, name, t.Elem(), true, queue, processed)

	case *types.Slice:
		// []byte special case.
		if basic, ok := t.Elem().(*types.Basic); ok && basic.Kind() == types.Byte {
			return buildByteSliceFieldInfo(name, isPointer), nil
		}

		eltInfo, err := fieldInfoFromType(pkg, allPkgs, "", t.Elem(), false, queue, processed)
		if err != nil {
			return FieldInfo{}, fmt.Errorf("slice element: %w", err)
		}

		return buildSliceFieldInfo(name, eltInfo, isPointer), nil

	case *types.Map:
		keyInfo, err := fieldInfoFromType(pkg, allPkgs, "", t.Key(), false, queue, processed)
		if err != nil {
			return FieldInfo{}, fmt.Errorf("map key: %w", err)
		}
		if keyInfo.IsStruct {
			return FieldInfo{}, fmt.Errorf("struct map keys are not supported")
		}

		valInfo, err := fieldInfoFromType(pkg, allPkgs, "", t.Elem(), false, queue, processed)
		if err != nil {
			return FieldInfo{}, fmt.Errorf("map value: %w", err)
		}

		return buildMapFieldInfo(name, keyInfo, valInfo, isPointer), nil

	case *types.Array:
		eltInfo, err := fieldInfoFromType(pkg, allPkgs, "", t.Elem(), false, queue, processed)
		if err != nil {
			return FieldInfo{}, fmt.Errorf("array element: %w", err)
		}

		lenStr := fmt.Sprintf("%d", t.Len())
		return buildFixedArrayFieldInfo(name, lenStr, eltInfo, isPointer), nil
	}

	return FieldInfo{}, fmt.Errorf("unsupported type: %s", typ)
}

// fieldInfoFromBasic maps a types.Basic to a FieldInfo with the corresponding
// Arrow primitive type.
func fieldInfoFromBasic(name string, basic *types.Basic, isPointer bool) (FieldInfo, error) {
	var arrowType, arrowBuilder, castType string
	switch basic.Kind() {
	case types.Int8:
		arrowType, arrowBuilder, castType = "arrow.PrimitiveTypes.Int8", "*array.Int8Builder", "int8"
	case types.Int16:
		arrowType, arrowBuilder, castType = "arrow.PrimitiveTypes.Int16", "*array.Int16Builder", "int16"
	case types.Int32:
		arrowType, arrowBuilder, castType = "arrow.PrimitiveTypes.Int32", "*array.Int32Builder", "int32"
	case types.Int, types.Int64:
		arrowType, arrowBuilder, castType = "arrow.PrimitiveTypes.Int64", "*array.Int64Builder", "int64"
	case types.Uint8:
		arrowType, arrowBuilder, castType = "arrow.PrimitiveTypes.Uint8", "*array.Uint8Builder", "uint8"
	case types.Uint16:
		arrowType, arrowBuilder, castType = "arrow.PrimitiveTypes.Uint16", "*array.Uint16Builder", "uint16"
	case types.Uint32:
		arrowType, arrowBuilder, castType = "arrow.PrimitiveTypes.Uint32", "*array.Uint32Builder", "uint32"
	case types.Uint, types.Uint64:
		arrowType, arrowBuilder, castType = "arrow.PrimitiveTypes.Uint64", "*array.Uint64Builder", "uint64"
	case types.Float32:
		arrowType, arrowBuilder, castType = "arrow.PrimitiveTypes.Float32", "*array.Float32Builder", "float32"
	case types.Float64:
		arrowType, arrowBuilder, castType = "arrow.PrimitiveTypes.Float64", "*array.Float64Builder", "float64"
	case types.String:
		arrowType, arrowBuilder, castType = "arrow.BinaryTypes.String", "*array.StringBuilder", "string"
	case types.Bool:
		arrowType, arrowBuilder, castType = "arrow.FixedWidthTypes.Boolean", "*array.BooleanBuilder", "bool"
	default:
		return FieldInfo{}, fmt.Errorf("unsupported basic type: %s", basic.Name())
	}

	goType := castType
	if isPointer {
		goType = "*" + goType
	}
	return FieldInfo{
		Name: name, GoType: goType, ArrowType: arrowType,
		ArrowBuilder: arrowBuilder, CastType: castType, IsPointer: isPointer,
		ArrowArrayType: arrowArrayType(arrowBuilder),
		ValueMethod:    "Value",
		ZeroExpr:       zeroExprForCast(castType),
	}, nil
}

// -- shared-resolution: Type resolution helpers used by both the AST and type-checker paths.

// resolveWellKnownType checks if a named type is a well-known stdlib type with
// a dedicated Arrow mapping (e.g., time.Duration → Int64, time.Time → Timestamp).
// Returns (FieldInfo, true) if matched, or (FieldInfo{}, false) if not.
func resolveWellKnownType(name string, named *types.Named, isPointer bool) (FieldInfo, bool) {
	if named.Obj().Pkg() == nil {
		return FieldInfo{}, false
	}
	pkgPath := named.Obj().Pkg().Path()
	typeName := named.Obj().Name()

	if pkgPath == "time" && typeName == "Duration" {
		goType := "time.Duration"
		if isPointer {
			goType = "*time.Duration"
		}
		return FieldInfo{
			Name:               name,
			GoType:             goType,
			ArrowType:          "arrow.PrimitiveTypes.Int64",
			ArrowBuilder:       "*array.Int64Builder",
			CastType:           "int64",
			IsPointer:          isPointer,
			ArrowArrayType:     "*array.Int64",
			ValueMethod:        "Value",
			ConvertBackExpr:    "time.Duration(%s)",
			ConvertBackImports: []string{"time"},
			ZeroExpr:           "0",
		}, true
	}

	if pkgPath == "time" && typeName == "Time" {
		goType := "time.Time"
		if isPointer {
			goType = "*time.Time"
		}
		return FieldInfo{
			Name:               name,
			GoType:             goType,
			ArrowType:          "arrow.FixedWidthTypes.Timestamp_ns",
			ArrowBuilder:       "*array.TimestampBuilder",
			CastType:           "arrow.Timestamp",
			ConvertMethod:      "UnixNano",
			IsPointer:          isPointer,
			ArrowArrayType:     "*array.Timestamp",
			ValueMethod:        "Value",
			ConvertBackExpr:    "time.Unix(0, int64(%s))",
			ConvertBackImports: []string{"time"},
			ZeroExpr:           "time.Time{}",
		}, true
	}

	if pkgPath == "google.golang.org/protobuf/types/known/durationpb" && typeName == "Duration" {
		goType := "durationpb.Duration"
		if isPointer {
			goType = "*durationpb.Duration"
		}
		zeroExpr := "durationpb.Duration{}"
		if isPointer {
			zeroExpr = "nil"
		}
		return FieldInfo{
			Name:               name,
			GoType:             goType,
			ArrowType:          "arrow.PrimitiveTypes.Int64",
			ArrowBuilder:       "*array.Int64Builder",
			CastType:           "int64",
			ConvertMethod:      "AsDuration",
			IsPointer:          isPointer,
			ArrowArrayType:     "*array.Int64",
			ValueMethod:        "Value",
			ConvertBackExpr:    "durationpb.New(time.Duration(%s))",
			ConvertBackIsPtr:   true,
			ConvertBackImports: []string{"time", "google.golang.org/protobuf/types/known/durationpb"},
			ZeroExpr:           zeroExpr,
		}, true
	}

	if pkgPath == "google.golang.org/protobuf/types/known/timestamppb" && typeName == "Timestamp" {
		goType := "timestamppb.Timestamp"
		if isPointer {
			goType = "*timestamppb.Timestamp"
		}
		zeroExpr := "timestamppb.Timestamp{}"
		if isPointer {
			zeroExpr = "nil"
		}
		return FieldInfo{
			Name:               name,
			GoType:             goType,
			ArrowType:          "arrow.FixedWidthTypes.Timestamp_ns",
			ArrowBuilder:       "*array.TimestampBuilder",
			CastType:           "arrow.Timestamp",
			ConvertMethod:      "AsTime().UnixNano",
			IsPointer:          isPointer,
			ArrowArrayType:     "*array.Timestamp",
			ValueMethod:        "Value",
			ConvertBackExpr:    "timestamppb.New(time.Unix(0, int64(%s)))",
			ConvertBackIsPtr:   true,
			ConvertBackImports: []string{"time", "google.golang.org/protobuf/types/known/timestamppb"},
			ZeroExpr:           zeroExpr,
		}, true
	}

	return FieldInfo{}, false
}

// detectMarshalMethod checks if a type implements serialization interfaces.
// Priority: MarshalText (encoding.TextMarshaler) > String (fmt.Stringer) > MarshalBinary (encoding.BinaryMarshaler).
// Returns the method name to use, or "" if none is found.
func detectMarshalMethod(typ types.Type) string {
	// Use pointer method set to include both value and pointer receiver methods.
	// This is safe because struct fields are always addressable.
	var checkType types.Type
	if _, isPtr := typ.(*types.Pointer); isPtr {
		checkType = typ
	} else {
		checkType = types.NewPointer(typ)
	}
	mset := types.NewMethodSet(checkType)

	if mset.Lookup(nil, "MarshalText") != nil {
		return "MarshalText"
	}
	if mset.Lookup(nil, "String") != nil {
		return "String"
	}
	if mset.Lookup(nil, "MarshalBinary") != nil {
		return "MarshalBinary"
	}
	return ""
}

// marshalMethodArrowType returns the Arrow type and builder for a given marshal method.
// MarshalText and String produce string columns; MarshalBinary produces binary columns.
func marshalMethodArrowType(method string) (string, string) {
	if method == "MarshalBinary" {
		return "arrow.BinaryTypes.Binary", "*array.BinaryBuilder"
	}
	return "arrow.BinaryTypes.String", "*array.StringBuilder"
}

// -- builders: Shared FieldInfo construction helpers used by both resolution paths.

// buildStructFieldInfo constructs a FieldInfo for a struct field (value or pointer)
// and enqueues the struct name for recursive processing.
func buildStructFieldInfo(name string, structName string, pkgName string, pkgPath string, isPointer bool, queue *[]structRef, processed map[string]bool) FieldInfo {
	qualName := pkgPath + "." + structName
	if !processed[qualName] {
		*queue = append(*queue, structRef{PkgPath: pkgPath, Name: structName})
	}

	goType := structName
	if isPointer {
		goType = "*" + structName
	}

	zeroExpr := structName + "{}"
	if isPointer {
		zeroExpr = "nil"
	}

	return FieldInfo{
		Name:           name,
		GoType:         goType,
		ArrowType:      fmt.Sprintf("arrow.StructOf(New%sSchema().Fields()...)", structName),
		ArrowBuilder:   "*array.StructBuilder",
		IsStruct:       true,
		IsPointer:      isPointer,
		StructName:     structName,
		ArrowArrayType: "*array.Struct",
		ZeroExpr:       zeroExpr,
	}
}

// eltArrowType returns the Arrow type expression for a FieldInfo, using the
// struct schema constructor when the field is a struct type.
func eltArrowType(fi FieldInfo) string {
	if fi.IsStruct {
		return fmt.Sprintf("arrow.StructOf(New%sSchema().Fields()...)", fi.StructName)
	}
	return fi.ArrowType
}

// buildSliceFieldInfo constructs a FieldInfo for a slice type from its resolved element.
func buildSliceFieldInfo(name string, eltInfo FieldInfo, isPointer bool) FieldInfo {
	goType := "[]" + eltInfo.GoType
	if isPointer {
		goType = "*" + goType
	}
	return FieldInfo{
		Name:           name,
		GoType:         goType,
		ArrowType:      fmt.Sprintf("arrow.ListOf(%s)", eltArrowType(eltInfo)),
		ArrowBuilder:   "*array.ListBuilder",
		IsList:         true,
		EltInfo:        &eltInfo,
		IsPointer:      isPointer,
		ArrowArrayType: "*array.List",
		ZeroExpr:       "nil",
	}
}

// buildMapFieldInfo constructs a FieldInfo for a map type from its resolved key and value.
func buildMapFieldInfo(name string, keyInfo, valInfo FieldInfo, isPointer bool) FieldInfo {
	goType := fmt.Sprintf("map[%s]%s", keyInfo.GoType, valInfo.GoType)
	if isPointer {
		goType = "*" + goType
	}
	return FieldInfo{
		Name:           name,
		GoType:         goType,
		ArrowType:      fmt.Sprintf("arrow.MapOf(%s, %s)", keyInfo.ArrowType, eltArrowType(valInfo)),
		ArrowBuilder:   "*array.MapBuilder",
		IsMap:          true,
		KeyInfo:        &keyInfo,
		EltInfo:        &valInfo,
		IsPointer:      isPointer,
		ArrowArrayType: "*array.Map",
		ZeroExpr:       "nil",
	}
}

// buildFixedArrayFieldInfo constructs a FieldInfo for a fixed-size array from its resolved element.
func buildFixedArrayFieldInfo(name string, lenStr string, eltInfo FieldInfo, isPointer bool) FieldInfo {
	goType := fmt.Sprintf("[%s]%s", lenStr, eltInfo.GoType)
	if isPointer {
		goType = "*" + goType
	}
	return FieldInfo{
		Name:            name,
		GoType:          goType,
		ArrowType:       fmt.Sprintf("arrow.FixedSizeListOfNonNullable(%s, %s)", lenStr, eltArrowType(eltInfo)),
		ArrowBuilder:    "*array.FixedSizeListBuilder",
		IsFixedSizeList: true,
		FixedSizeLen:    lenStr,
		EltInfo:         &eltInfo,
		IsPointer:       isPointer,
		ArrowArrayType:  "*array.FixedSizeList",
		ZeroExpr:        fmt.Sprintf("[%s]%s{}", lenStr, eltInfo.GoType),
	}
}

// buildByteSliceFieldInfo constructs a FieldInfo for a []byte field (Arrow Binary).
func buildByteSliceFieldInfo(name string, isPointer bool) FieldInfo {
	goType := "[]byte"
	if isPointer {
		goType = "*[]byte"
	}
	return FieldInfo{
		Name:           name,
		GoType:         goType,
		ArrowType:      "arrow.BinaryTypes.Binary",
		ArrowBuilder:   "*array.BinaryBuilder",
		CastType:       "[]byte",
		IsPointer:      isPointer,
		ArrowArrayType: "*array.Binary",
		ValueMethod:    "Value",
		ZeroExpr:       "nil",
	}
}

// marshalGoTypeInfo extracts the short Go type name and import path from a types.Type.
// For named types, it produces "pkg.Name" (e.g., "netip.Addr") instead of the full
// import-path form ("net/netip.Addr") that types.Type.String() produces.
func marshalGoTypeInfo(typ types.Type) (shortType string, importPath string) {
	underlying := typ
	if ptr, ok := underlying.(*types.Pointer); ok {
		underlying = ptr.Elem()
	}
	if named, ok := underlying.(*types.Named); ok && named.Obj().Pkg() != nil {
		shortType = named.Obj().Pkg().Name() + "." + named.Obj().Name()
		importPath = named.Obj().Pkg().Path()
	} else {
		shortType = underlying.String()
	}
	return
}

// buildMarshalFieldInfo constructs a FieldInfo for an external type resolved via marshal method.
func buildMarshalFieldInfo(name string, typ types.Type, method string, isPointer bool) FieldInfo {
	shortType, importPath := marshalGoTypeInfo(typ)
	goType := shortType
	if isPointer {
		goType = "*" + shortType
	}
	zeroExpr := shortType + "{}"
	if isPointer {
		zeroExpr = "nil"
	}
	var unmarshalImports []string
	if importPath != "" && unmarshalForMarshal(method) != "" {
		unmarshalImports = []string{importPath}
	}
	arrowType, arrowBuilder := marshalMethodArrowType(method)
	return FieldInfo{
		Name:             name,
		GoType:           goType,
		ArrowType:        arrowType,
		ArrowBuilder:     arrowBuilder,
		MarshalMethod:    method,
		IsPointer:        isPointer,
		ArrowArrayType:   arrowArrayType(arrowBuilder),
		ValueMethod:      "Value",
		UnmarshalMethod:  unmarshalForMarshal(method),
		ZeroExpr:         zeroExpr,
		UnmarshalImports: unmarshalImports,
	}
}
