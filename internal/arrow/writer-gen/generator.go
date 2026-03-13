package writergen

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"

	"golang.org/x/tools/go/packages"
)

// FieldInfo contains information about a parsed struct field.
type FieldInfo struct {
	Name             string
	ArrowType        string // The Apache Arrow datatype string (e.g., "arrow.PrimitiveTypes.Int32")
	ArrowBuilder     string // The Arrow array builder type (e.g., "*array.Int32Builder")
	GoType           string // The original Go type string
	IsList           bool
	IsMap            bool
	IsStruct         bool   // True if the field itself is a struct or pointer-to-struct
	IsPointer        bool   // True if the field is a pointer
	StructName       string // If IsStruct=true, the name of the struct
	KeyArrowBuilder  string // Used for the map keys builder type
	ValArrowBuilder  string // Used for the list items and map values builder type
	CastType         string // The Go type used when appending to the builder
	KeyCastType      string // The Go type used when appending a map key
	ValCastType      string // The Go type used when appending a map value or list item
	IsFixedSizeList  bool   // True if the field is a fixed-size array ([N]T)
	FixedSizeLen     string // The array length as a string literal (e.g. "4")
	ValIsStruct      bool   // True if list value or map value is a struct
	ValIsPointer     bool   // True if list value or map value is a pointer
	ValStructName    string // If ValIsStruct is true, the name of that struct
	MarshalMethod    string // Serialization method for external types: "MarshalText", "String", "MarshalBinary", or ""
	ValMarshalMethod string // Serialization method for list/map value external types
}

// StructInfo contains information about a parsed Go struct.
type StructInfo struct {
	Name      string
	Fields    []FieldInfo
	PkgPath   string // import path of the package this struct belongs to
	PkgName   string // base package name of the package this struct belongs to
	Qualifier string // qualifier prefix for this struct in generated code (e.g. "mypkg." or "")
}

// Generator holds the configuration for generating Arrow writers.
type Generator struct {
	InputPkgs     []string
	TargetStructs []string
	OutPath       string
	Verbose       bool
	PkgAliases    []string // raw alias mappings in "original=replacement" format
}

// NewGenerator initializes a new Generator.
func NewGenerator(inputPkgs []string, targetStructs []string, outPath string, verbose bool, pkgAliases []string) *Generator {
	return &Generator{
		InputPkgs:     inputPkgs,
		TargetStructs: targetStructs,
		OutPath:       outPath,
		Verbose:       verbose,
		PkgAliases:    pkgAliases,
	}
}

// collectPackageErrors counts the number of errors across loaded packages
// without printing to stderr (unlike packages.PrintErrors).
func collectPackageErrors(pkgs []*packages.Package) int {
	count := 0
	packages.Visit(pkgs, nil, func(pkg *packages.Package) {
		count += len(pkg.Errors)
	})
	return count
}

// loadPackages loads all packages from InputPkgs, one directory at a time.
// Each directory is loaded independently to support separate Go modules.
func (g *Generator) loadPackages() ([]*packages.Package, error) {
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
	}
	var all []*packages.Package
	for _, dir := range g.InputPkgs {
		cfg.Dir = dir
		pkgs, err := packages.Load(cfg, ".")
		if err != nil {
			return nil, fmt.Errorf("failed to load package directory %q: %w", dir, err)
		}
		if errCount := collectPackageErrors(pkgs); errCount > 0 {
			return nil, fmt.Errorf("package loading had %d error(s) in %q", errCount, dir)
		}
		all = append(all, pkgs...)
	}
	return all, nil
}

// findPkgByPath returns the loaded package with the given import path, or nil.
func findPkgByPath(pkgs []*packages.Package, pkgPath string) *packages.Package {
	for _, p := range pkgs {
		if p.PkgPath == pkgPath {
			return p
		}
	}
	return nil
}

// Parse extracts StructInfo for the targeted structs and discovers the primary package name and path.
// The returned pkgName and pkgPath refer to the first loaded input package (used for output package
// auto-detection when no --pkg-name override is given). Each StructInfo carries its own PkgPath/PkgName.
func (g *Generator) Parse() (string, string, []StructInfo, error) {
	allPkgs, err := g.loadPackages()
	if err != nil {
		return "", "", nil, err
	}

	var parsedPkgName string
	var parsedPkgPath string
	if len(allPkgs) > 0 {
		parsedPkgName = allPkgs[0].Name
		parsedPkgPath = allPkgs[0].PkgPath
	}

	queue := make([]string, len(g.TargetStructs))
	copy(queue, g.TargetStructs)
	processed := make(map[string]bool)
	var results []StructInfo

	for len(queue) > 0 {
		target := queue[0]
		queue = queue[1:]

		if processed[target] {
			continue
		}
		processed[target] = true

		found := false
		for _, pkg := range allPkgs {
			for _, file := range pkg.Syntax {
				ast.Inspect(file, func(n ast.Node) bool {
					ts, ok := n.(*ast.TypeSpec)
					if !ok || ts.Name.Name != target {
						return true
					}

					st, ok := ts.Type.(*ast.StructType)
					if !ok {
						return true
					}

					found = true
					info := StructInfo{
						Name:    ts.Name.Name,
						PkgPath: pkg.PkgPath,
						PkgName: pkg.Name,
					}

					for _, field := range st.Fields.List {
						if len(field.Names) == 0 {
							continue // Skip embedded fields for now
						}

						fieldName := field.Names[0].Name
						fieldInfo, err := mapToFieldInfo(pkg, allPkgs, fieldName, field.Type, &queue, processed)
						if err != nil {
							fmt.Printf("Warning: Skipping field %s in %s: %v\n", fieldName, ts.Name.Name, err)
							continue
						}

						info.Fields = append(info.Fields, fieldInfo)
					}

					results = append(results, info)
					return false
				})
				if found {
					break
				}
			}
			if found {
				break
			}
		}

		if !found && g.Verbose {
			fmt.Printf("Warning: Could not find definition for targeted struct: %s\n", target)
		}
	}

	return parsedPkgName, parsedPkgPath, results, nil
}

// mapToFieldInfo maps an AST expression to a FieldInfo struct.
func mapToFieldInfo(pkg *packages.Package, allPkgs []*packages.Package, name string, expr ast.Expr, queue *[]string, processed map[string]bool) (FieldInfo, error) {
	switch t := expr.(type) {
	case *ast.Ident:
		return resolveIdent(pkg, allPkgs, name, t, false, queue, processed)

	case *ast.StarExpr:
		// Pointer type - this could be to a struct or a primitive
		if ident, ok := t.X.(*ast.Ident); ok {
			return resolveIdent(pkg, allPkgs, name, ident, true, queue, processed)
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
							if findPkgByPath(allPkgs, pkgPath) != nil {
								structName := named.Obj().Name()
								pkgName := named.Obj().Pkg().Name()
								return mapStructField(name, structName, pkgName, pkgPath, true, queue, processed), nil
							}
						}
					}
				}
				// External type: fall back to marshal method detection.
				method := detectMarshalMethod(typ)
				if method != "" {
					arrowType, arrowBuilder := marshalMethodArrowType(method)
					return FieldInfo{
						Name:          name,
						GoType:        "*" + typ.String(),
						ArrowType:     arrowType,
						ArrowBuilder:  arrowBuilder,
						MarshalMethod: method,
						IsPointer:     true,
					}, nil
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
			eltInfo, err := mapToFieldInfo(pkg, allPkgs, "", t.Elt, queue, processed)
			if err != nil {
				return FieldInfo{}, fmt.Errorf("fixed-size array element %w", err)
			}

			arrowType := fmt.Sprintf("arrow.FixedSizeListOfNonNullable(%s, %s)", lit.Value, eltInfo.ArrowType)
			if eltInfo.IsStruct {
				arrowType = fmt.Sprintf("arrow.FixedSizeListOfNonNullable(%s, arrow.StructOf(New%sSchema().Fields()...))", lit.Value, eltInfo.StructName)
			}

			return FieldInfo{
				Name:             name,
				GoType:           fmt.Sprintf("[%s]%s", lit.Value, eltInfo.GoType),
				ArrowType:        arrowType,
				ArrowBuilder:     "*array.FixedSizeListBuilder",
				IsFixedSizeList:  true,
				FixedSizeLen:     lit.Value,
				ValArrowBuilder:  eltInfo.ArrowBuilder,
				ValCastType:      eltInfo.CastType,
				ValIsStruct:      eltInfo.IsStruct,
				ValIsPointer:     eltInfo.IsPointer,
				ValStructName:    eltInfo.StructName,
				ValMarshalMethod: eltInfo.MarshalMethod,
			}, nil
		}

		// []byte is represented as Arrow Binary, not a List of Uint8.
		if eltIdent, ok := t.Elt.(*ast.Ident); ok && eltIdent.Name == "byte" {
			return FieldInfo{
				Name:         name,
				GoType:       "[]byte",
				ArrowType:    "arrow.BinaryTypes.Binary",
				ArrowBuilder: "*array.BinaryBuilder",
				CastType:     "[]byte",
			}, nil
		}

		// Slice type
		eltInfo, err := mapToFieldInfo(pkg, allPkgs, "", t.Elt, queue, processed)
		if err != nil {
			return FieldInfo{}, fmt.Errorf("slice element %w", err)
		}

		arrowType := fmt.Sprintf("arrow.ListOf(%s)", eltInfo.ArrowType)
		if eltInfo.IsStruct {
			arrowType = fmt.Sprintf("arrow.ListOf(arrow.StructOf(New%sSchema().Fields()...))", eltInfo.StructName)
		}

		return FieldInfo{
			Name:             name,
			GoType:           "[]" + eltInfo.GoType,
			ArrowType:        arrowType,
			ArrowBuilder:     "*array.ListBuilder",
			IsList:           true,
			ValArrowBuilder:  eltInfo.ArrowBuilder,
			ValCastType:      eltInfo.CastType,
			IsStruct:         false, // A slice itself is not a struct
			ValIsStruct:      eltInfo.IsStruct,
			ValIsPointer:     eltInfo.IsPointer,
			ValStructName:    eltInfo.StructName,
			ValMarshalMethod: eltInfo.MarshalMethod,
		}, nil

	case *ast.MapType:
		// Map type
		keyInfo, err := mapToFieldInfo(pkg, allPkgs, "", t.Key, queue, processed)
		if err != nil {
			return FieldInfo{}, fmt.Errorf("map key %w", err)
		}
		if keyInfo.IsStruct {
			return FieldInfo{}, fmt.Errorf("struct maps keys are not supported")
		}

		valInfo, err := mapToFieldInfo(pkg, allPkgs, "", t.Value, queue, processed)
		if err != nil {
			return FieldInfo{}, fmt.Errorf("map value %w", err)
		}

		valArrowType := valInfo.ArrowType
		if valInfo.IsStruct {
			valArrowType = fmt.Sprintf("arrow.StructOf(New%sSchema().Fields()...)", valInfo.StructName)
		}

		return FieldInfo{
			Name:            name,
			GoType:          fmt.Sprintf("map[%s]%s", keyInfo.GoType, valInfo.GoType),
			ArrowType:       fmt.Sprintf("arrow.MapOf(%s, %s)", keyInfo.ArrowType, valArrowType),
			ArrowBuilder:    "*array.MapBuilder",
			IsMap:           true,
			KeyArrowBuilder: keyInfo.ArrowBuilder,
			ValArrowBuilder: valInfo.ArrowBuilder,
			KeyCastType:     keyInfo.CastType,
			ValCastType:     valInfo.CastType,
			IsStruct:        false, // A map itself is not a struct
			ValIsStruct:     valInfo.IsStruct,
			ValIsPointer:    valInfo.IsPointer,
			ValStructName:   valInfo.StructName,
		}, nil

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
					if findPkgByPath(allPkgs, pkgPath) != nil {
						structName := named.Obj().Name()
						pkgName := named.Obj().Pkg().Name()
						return mapStructField(name, structName, pkgName, pkgPath, false, queue, processed), nil
					}
				}
			}
		}

		// External type not in any loaded package: fall back to marshal method detection.
		method := detectMarshalMethod(typ)
		if method == "" {
			return FieldInfo{}, fmt.Errorf("external type %s does not implement TextMarshaler, Stringer, or BinaryMarshaler", typ)
		}
		arrowType, arrowBuilder := marshalMethodArrowType(method)
		return FieldInfo{
			Name:          name,
			GoType:        typ.String(),
			ArrowType:     arrowType,
			ArrowBuilder:  arrowBuilder,
			MarshalMethod: method,
		}, nil
	}

	return FieldInfo{}, fmt.Errorf("unsupported AST expression type: %T", expr)
}

// resolveIdent handles unified type resolution for ast.Ident, tracking if it was accessed via a pointer.
func resolveIdent(pkg *packages.Package, allPkgs []*packages.Package, name string, ident *ast.Ident, isPointer bool, queue *[]string, processed map[string]bool) (FieldInfo, error) {
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
			return mapStructField(name, obj.Name(), structPkgName, structPkgPath, isPointer, queue, processed), nil
		}

		// Check for named type over a primitive (e.g., type MyStates int)
		if basic, ok := obj.Type().Underlying().(*types.Basic); ok {
			syntheticIdent := &ast.Ident{Name: basic.Name()}
			_, arrowType, arrowBuilder, castType, err := mapToArrowType(syntheticIdent)
			if err == nil {
				goTypeName := obj.Name()
				if isPointer {
					goTypeName = "*" + goTypeName
				}
				return FieldInfo{
					Name:         name,
					GoType:       goTypeName,
					ArrowType:    arrowType,
					ArrowBuilder: arrowBuilder,
					CastType:     castType,
					IsPointer:    isPointer,
				}, nil
			}
		}
	}

	// Primitive type
	goType, arrowType, arrowBuilder, castType, err := mapToArrowType(ident)
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
		Name:         name,
		GoType:       goTypeName,
		ArrowType:    arrowType,
		ArrowBuilder: arrowBuilder,
		CastType:     castType,
		IsPointer:    isPointer,
	}, nil
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

// mapStructField builds a FieldInfo for a struct field (value or pointer).
// pkgName and pkgPath record the origin package of the referenced struct.
func mapStructField(name string, structName string, pkgName string, pkgPath string, isPointer bool, queue *[]string, processed map[string]bool) FieldInfo {
	if !processed[structName] {
		*queue = append(*queue, structName)
	}

	goType := structName
	if isPointer {
		goType = "*" + structName
	}

	return FieldInfo{
		Name:         name,
		GoType:       goType,
		ArrowType:    fmt.Sprintf("arrow.StructOf(New%sSchema().Fields()...)", structName),
		ArrowBuilder: "*array.StructBuilder",
		IsStruct:     true,
		IsPointer:    isPointer,
		StructName:   structName,
	}
}

// mapToArrowType maps a primitive Go AST expression to its primitive Arrow type representation
// returning the Go type string, the Arrow type string, the Builder type string, and an error if unsupported.
func mapToArrowType(expr ast.Expr) (string, string, string, string, error) {
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
