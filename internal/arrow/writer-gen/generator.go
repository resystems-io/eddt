package writergen

import (
	"fmt"
	"go/ast"

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
	KeyArrowBuilder string // Used for the map keys builder type
	ValArrowBuilder string // Used for the list items and map values builder type
	CastType        string // The Go type used when appending to the builder
	KeyCastType     string // The Go type used when appending a map key
	ValCastType     string // The Go type used when appending a map value or list item
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
}

// NewGenerator initializes a new Generator.
func NewGenerator(inputPkg string, targetStructs []string, outPath string, verbose bool) *Generator {
	return &Generator{
		InputPkg:      inputPkg,
		TargetStructs: targetStructs,
		OutPath:       outPath,
		Verbose:       verbose,
	}
}

// Parse extracts StructInfo for the targeted structs.
func (g *Generator) Parse() ([]StructInfo, error) {
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		Dir:  g.InputPkg,
	}
	pkgs, err := packages.Load(cfg, ".")
	if err != nil {
		return nil, fmt.Errorf("failed to load package directory %q: %w", g.InputPkg, err)
	}
	if packages.PrintErrors(pkgs) > 0 {
		return nil, fmt.Errorf("package loading had errors in %q", g.InputPkg)
	}

	targets := make(map[string]bool)
	for _, t := range g.TargetStructs {
		targets[t] = true
	}

	var results []StructInfo

	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			ast.Inspect(file, func(n ast.Node) bool {
				ts, ok := n.(*ast.TypeSpec)
				if !ok {
					return true
				}

				if !targets[ts.Name.Name] {
					return true
				}

				st, ok := ts.Type.(*ast.StructType)
				if !ok {
					return true
				}

				info := StructInfo{Name: ts.Name.Name}

				for _, field := range st.Fields.List {
					if len(field.Names) == 0 {
						continue // Skip embedded fields for now
					}

					fieldName := field.Names[0].Name
					fieldInfo, err := mapToFieldInfo(fieldName, field.Type)
					if err != nil {
						if g.Verbose {
							fmt.Printf("Warning: Skipping field %s in %s: %v\n", fieldName, ts.Name.Name, err)
						}
						continue
					}

					info.Fields = append(info.Fields, fieldInfo)
				}

				results = append(results, info)
				return false // Don't traverse inside the struct
			})
		}
	}

	return results, nil
}

// mapToFieldInfo maps an AST expression to a FieldInfo struct.
func mapToFieldInfo(name string, expr ast.Expr) (FieldInfo, error) {
	switch t := expr.(type) {
	case *ast.Ident:
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

	case *ast.ArrayType:
		// Slice type
		eltGoType, eltArrowType, eltArrowBuilder, eltCastType, err := mapToArrowType(t.Elt)
		if err != nil {
			return FieldInfo{}, fmt.Errorf("slice element %w", err)
		}
		return FieldInfo{
			Name:            name,
			GoType:          "[]" + eltGoType,
			ArrowType:       fmt.Sprintf("arrow.ListOf(%s)", eltArrowType),
			ArrowBuilder:    "*array.ListBuilder",
			IsList:          true,
			ValArrowBuilder: eltArrowBuilder,
			ValCastType:     eltCastType,
		}, nil

	case *ast.MapType:
		// Map type
		keyGoType, keyArrowType, keyArrowBuilder, keyCastType, err := mapToArrowType(t.Key)
		if err != nil {
			return FieldInfo{}, fmt.Errorf("map key %w", err)
		}


		valGoType, valArrowType, valArrowBuilder, valCastType, err := mapToArrowType(t.Value)
		if err != nil {
			return FieldInfo{}, fmt.Errorf("map value %w", err)
		}
		return FieldInfo{
			Name:            name,
			GoType:          fmt.Sprintf("map[%s]%s", keyGoType, valGoType),
			ArrowType:       fmt.Sprintf("arrow.MapOf(%s, %s)", keyArrowType, valArrowType),
			ArrowBuilder:    "*array.MapBuilder",
			IsMap:           true,
			KeyArrowBuilder: keyArrowBuilder,
			ValArrowBuilder: valArrowBuilder,
			KeyCastType:     keyCastType,
			ValCastType:     valCastType,
		}, nil
	}

	return FieldInfo{}, fmt.Errorf("unsupported AST expression type: %T", expr)
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
