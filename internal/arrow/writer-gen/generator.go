package writergen

import (
	"fmt"
	"go/ast"
	"go/types"

	"golang.org/x/tools/go/packages"
)

// FieldInfo contains information about a parsed struct field.
type FieldInfo struct {
	Name            string
	ArrowType       string // The Apache Arrow datatype string (e.g., "arrow.PrimitiveTypes.Int32")
	ArrowBuilder    string // The Arrow array builder type (e.g., "*array.Int32Builder")
	GoType          string // The original Go type string
	IsList          bool
	IsMap           bool
	IsStruct        bool   // True if the field itself is a struct or pointer-to-struct
	IsPointer       bool   // True if the field is a pointer
	StructName      string // If IsStruct=true, the name of the struct
	KeyArrowBuilder string // Used for the map keys builder type
	ValArrowBuilder string // Used for the list items and map values builder type
	CastType        string // The Go type used when appending to the builder
	KeyCastType     string // The Go type used when appending a map key
	ValCastType     string // The Go type used when appending a map value or list item
	ValIsStruct     bool   // True if list value or map value is a struct
	ValIsPointer    bool   // True if list value or map value is a pointer
	ValStructName   string // If ValIsStruct is true, the name of that struct
}

// StructInfo contains information about a parsed Go struct.
type StructInfo struct {
	Name   string
	Fields []FieldInfo
}

// Generator holds the configuration for generating Arrow writers.
type Generator struct {
	InputPkg      string
	TargetStructs []string
	OutPath       string
	Verbose       bool
	PkgAlias      string
}

// NewGenerator initializes a new Generator.
func NewGenerator(inputPkg string, targetStructs []string, outPath string, verbose bool, pkgAlias string) *Generator {
	return &Generator{
		InputPkg:      inputPkg,
		TargetStructs: targetStructs,
		OutPath:       outPath,
		Verbose:       verbose,
		PkgAlias:      pkgAlias,
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

// Parse extracts StructInfo for the targeted structs and discovers the package name.
func (g *Generator) Parse() (string, string, []StructInfo, error) {
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		Dir:  g.InputPkg,
	}
	pkgs, err := packages.Load(cfg, ".")
	if err != nil {
		return "", "", nil, fmt.Errorf("failed to load package directory %q: %w", g.InputPkg, err)
	}
	if errCount := collectPackageErrors(pkgs); errCount > 0 {
		return "", "", nil, fmt.Errorf("package loading had %d error(s) in %q", errCount, g.InputPkg)
	}

	var parsedPkgName string
	var parsedPkgPath string
	if len(pkgs) > 0 {
		parsedPkgName = pkgs[0].Name
		parsedPkgPath = pkgs[0].PkgPath
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
		for _, pkg := range pkgs {
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
					info := StructInfo{Name: ts.Name.Name}

					for _, field := range st.Fields.List {
						if len(field.Names) == 0 {
							continue // Skip embedded fields for now
						}

						fieldName := field.Names[0].Name
						fieldInfo, err := mapToFieldInfo(pkg, fieldName, field.Type, &queue, processed)
						if err != nil {
							if g.Verbose {
								fmt.Printf("Warning: Skipping field %s in %s: %v\n", fieldName, ts.Name.Name, err)
							}
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
func mapToFieldInfo(pkg *packages.Package, name string, expr ast.Expr, queue *[]string, processed map[string]bool) (FieldInfo, error) {
	switch t := expr.(type) {
	case *ast.Ident:
		// Check for struct reference
		obj := pkg.TypesInfo.ObjectOf(t)
		if obj != nil {
			if _, ok := obj.Type().Underlying().(*types.Struct); ok {
				return mapStructField(name, obj.Name(), false, queue, processed), nil
			}
		}

		// Primitive type
		goType, arrowType, arrowBuilder, castType, err := mapToArrowType(t)
		if err != nil {
			return FieldInfo{}, err
		}
		return FieldInfo{
			Name:         name,
			GoType:       goType,
			ArrowType:    arrowType,
			ArrowBuilder: arrowBuilder,
			CastType:     castType,
		}, nil

	case *ast.StarExpr:
		// Pointer type - this could be to a struct or a primitive
		ident, ok := t.X.(*ast.Ident)
		if ok {
			obj := pkg.TypesInfo.ObjectOf(ident)
			if obj != nil {
				if _, isStruct := obj.Type().Underlying().(*types.Struct); isStruct {
					return mapStructField(name, obj.Name(), true, queue, processed), nil
				}
			}

			// If it's not a struct, try mapping it as a primitive
			goType, arrowType, arrowBuilder, castType, err := mapToArrowType(ident)
			if err != nil {
				return FieldInfo{}, fmt.Errorf("unsupported pointer type: %w", err)
			}
			return FieldInfo{
				Name:         name,
				GoType:       "*" + goType,
				ArrowType:    arrowType,
				ArrowBuilder: arrowBuilder,
				CastType:     castType,
				IsPointer:    true,
			}, nil
		}
		return FieldInfo{}, fmt.Errorf("unsupported pointer type")

	case *ast.ArrayType:
		// Slice type
		eltInfo, err := mapToFieldInfo(pkg, "", t.Elt, queue, processed)
		if err != nil {
			return FieldInfo{}, fmt.Errorf("slice element %w", err)
		}

		arrowType := fmt.Sprintf("arrow.ListOf(%s)", eltInfo.ArrowType)
		if eltInfo.IsStruct {
			arrowType = fmt.Sprintf("arrow.ListOf(arrow.StructOf(New%sSchema().Fields()...))", eltInfo.StructName)
		}

		return FieldInfo{
			Name:            name,
			GoType:          "[]" + eltInfo.GoType,
			ArrowType:       arrowType,
			ArrowBuilder:    "*array.ListBuilder",
			IsList:          true,
			ValArrowBuilder: eltInfo.ArrowBuilder,
			ValCastType:     eltInfo.CastType,
			IsStruct:        false, // A slice itself is not a struct
			ValIsStruct:     eltInfo.IsStruct,
			ValIsPointer:    eltInfo.IsPointer,
			ValStructName:   eltInfo.StructName,
		}, nil

	case *ast.MapType:
		// Map type
		keyInfo, err := mapToFieldInfo(pkg, "", t.Key, queue, processed)
		if err != nil {
			return FieldInfo{}, fmt.Errorf("map key %w", err)
		}
		if keyInfo.IsStruct {
			return FieldInfo{}, fmt.Errorf("struct maps keys are not supported")
		}

		valInfo, err := mapToFieldInfo(pkg, "", t.Value, queue, processed)
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
	}

	return FieldInfo{}, fmt.Errorf("unsupported AST expression type: %T", expr)
}

func mapStructField(name string, structName string, isPointer bool, queue *[]string, processed map[string]bool) FieldInfo {
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
	case "int32":
		arrowType = "arrow.PrimitiveTypes.Int32"
		arrowBuilder = "*array.Int32Builder"
		castType = "int32"
	case "int64", "int":
		arrowType = "arrow.PrimitiveTypes.Int64"
		arrowBuilder = "*array.Int64Builder"
		castType = "int64"
	case "uint8":
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
