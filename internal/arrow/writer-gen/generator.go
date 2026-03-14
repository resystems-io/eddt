package writergen

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"path/filepath"
	"strings"

	"golang.org/x/tools/go/packages"
)

// -- types: Core data structures and constructor for the generator.

// FieldInfo contains information about a parsed struct field.
// For container types (list, fixed-size-list, map), element and key metadata
// is carried recursively via EltInfo and KeyInfo pointers, enabling arbitrary
// nesting depth (e.g. [][]map[K][]V).
type FieldInfo struct {
	Name            string
	ArrowType       string // The Apache Arrow datatype string (e.g., "arrow.PrimitiveTypes.Int32")
	ArrowBuilder    string // The Arrow array builder type (e.g., "*array.Int32Builder")
	GoType          string // The original Go type string
	IsList          bool
	IsMap           bool
	IsStruct        bool       // True if the field itself is a struct or pointer-to-struct
	IsPointer       bool       // True if the field is a pointer
	StructName      string     // If IsStruct=true, the name of the struct
	CastType        string     // The Go type used when appending to the builder
	IsFixedSizeList bool       // True if the field is a fixed-size array ([N]T)
	FixedSizeLen    string     // The array length as a string literal (e.g. "4")
	MarshalMethod   string     // Serialization method for external types: "MarshalText", "String", "MarshalBinary", or ""
	ConvertMethod   string     // Method to call on the value before casting (e.g. "UnixNano" for time.Time)
	EltInfo         *FieldInfo // Element info for lists, fixed-size-lists, and map values (recursive)
	KeyInfo         *FieldInfo // Key info for maps
}

// StructInfo contains information about a parsed Go struct.
type StructInfo struct {
	Name      string
	Fields    []FieldInfo
	PkgPath   string // import path of the package this struct belongs to
	PkgName   string // base package name of the package this struct belongs to
	Qualifier string // qualifier prefix for this struct in generated code (e.g. "mypkg." or "")
}

// structRef identifies a struct to be processed, optionally qualified by package path.
// When PkgPath is empty (user-specified initial targets), all loaded packages are searched.
// When PkgPath is set (discovered via field references), only the specified package is searched.
type structRef struct {
	PkgPath string
	Name    string
}

// Generator holds the configuration for generating Arrow writers.
type Generator struct {
	InputPkgs     []string
	TargetStructs []string
	OutPath       string
	Verbose       bool
	PkgAliases    []string // raw alias mappings in "original=replacement" format
	Version       string   // short commitish for the generated header; may be empty
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

// -- package-loading: Load and validate Go packages via golang.org/x/tools/go/packages.

// collectPackageErrors counts the number of errors across loaded packages
// without printing to stderr (unlike packages.PrintErrors).
func collectPackageErrors(pkgs []*packages.Package) int {
	count := 0
	packages.Visit(pkgs, nil, func(pkg *packages.Package) {
		count += len(pkg.Errors)
	})
	return count
}

// isFilesystemPath reports whether s looks like a filesystem path rather than
// a Go import path, following the convention from `go help packages`:
// paths starting with ".", "..", or "/" are filesystem paths.
func isFilesystemPath(s string) bool {
	return s == "." || strings.HasPrefix(s, "./") || strings.HasPrefix(s, "../") ||
		strings.HasPrefix(s, "/") || filepath.IsAbs(s)
}

// loadPackages loads all packages from InputPkgs. Filesystem paths (starting
// with ".", "..", or "/") are loaded one-at-a-time with cfg.Dir set, preserving
// support for separate Go modules. Go import paths are batched into a single
// packages.Load call resolved from the invoking module's go.mod.
func (g *Generator) loadPackages() ([]*packages.Package, error) {
	mode := packages.NeedName | packages.NeedFiles | packages.NeedSyntax |
		packages.NeedTypes | packages.NeedTypesInfo

	var all []*packages.Package
	var importPaths []string

	// Phase 1: Load filesystem paths one-at-a-time (may be separate modules).
	for _, input := range g.InputPkgs {
		if !isFilesystemPath(input) {
			importPaths = append(importPaths, input)
			continue
		}
		cfg := &packages.Config{Mode: mode, Dir: input}
		pkgs, err := packages.Load(cfg, ".")
		if err != nil {
			return nil, fmt.Errorf("failed to load package directory %q: %w", input, err)
		}
		if errCount := collectPackageErrors(pkgs); errCount > 0 {
			return nil, fmt.Errorf("package loading had %d error(s) in %q", errCount, input)
		}
		all = append(all, pkgs...)
	}

	// Phase 2: Load import paths in a single call (all resolved from the
	// invoking module's go.mod via cwd).
	if len(importPaths) > 0 {
		cfg := &packages.Config{Mode: mode}
		pkgs, err := packages.Load(cfg, importPaths...)
		if err != nil {
			return nil, fmt.Errorf("failed to load import paths %v: %w", importPaths, err)
		}
		if errCount := collectPackageErrors(pkgs); errCount > 0 {
			return nil, formatImportPathErrors(pkgs)
		}
		all = append(all, pkgs...)
	}

	return all, nil
}

// formatImportPathErrors inspects package-level errors for common patterns
// (e.g., missing module) and wraps them with actionable guidance.
func formatImportPathErrors(pkgs []*packages.Package) error {
	var msgs []string
	for _, pkg := range pkgs {
		for _, e := range pkg.Errors {
			msg := e.Msg
			if strings.Contains(msg, "no required module provides") ||
				strings.Contains(msg, "could not import") {
				msg += fmt.Sprintf("\n\n  To add it to your module's dependencies, run:"+
					"\n    go get %s\n\n  Then re-run the generator.", pkg.PkgPath)
			}
			msgs = append(msgs, msg)
		}
	}
	return fmt.Errorf("failed to load import paths:\n  %s", strings.Join(msgs, "\n  "))
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

// -- parsing: Top-level orchestration that walks targeted structs and their fields.

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

	queue := make([]structRef, len(g.TargetStructs))
	for i, t := range g.TargetStructs {
		queue[i] = structRef{Name: t}
	}
	processed := make(map[string]bool)
	var results []StructInfo

	for len(queue) > 0 {
		ref := queue[0]
		queue = queue[1:]

		// Determine which packages to search. Discovered structs (PkgPath set)
		// search only their origin package; initial targets search all packages.
		var searchPkgs []*packages.Package
		if ref.PkgPath != "" {
			qualName := ref.PkgPath + "." + ref.Name
			if processed[qualName] {
				continue
			}
			pkg := findPkgByPath(allPkgs, ref.PkgPath)
			if pkg == nil {
				continue
			}
			searchPkgs = []*packages.Package{pkg}
		} else {
			searchPkgs = allPkgs
		}

		found := false
		for _, pkg := range searchPkgs {
			qualName := pkg.PkgPath + "." + ref.Name
			if processed[qualName] {
				found = true
				break
			}

			for _, file := range pkg.Syntax {
				ast.Inspect(file, func(n ast.Node) bool {
					ts, ok := n.(*ast.TypeSpec)
					if !ok || ts.Name.Name != ref.Name {
						return true
					}

					st, ok := ts.Type.(*ast.StructType)
					if !ok {
						return true
					}

					found = true
					processed[qualName] = true
					info := StructInfo{
						Name:    ts.Name.Name,
						PkgPath: pkg.PkgPath,
						PkgName: pkg.Name,
					}

					// Pre-scan: collect explicitly named fields for shadowing detection.
					explicitNames := map[string]bool{}
					for _, field := range st.Fields.List {
						for _, name := range field.Names {
							if name.Name != "_" {
								explicitNames[name.Name] = true
							}
						}
					}

					// Pre-resolve embedded fields for cross-embedding ambiguity detection.
					// A promoted field name that appears from multiple embeddings is
					// ambiguous in Go (neither is accessible) and must be skipped.
					embeddedByIdx := make([][]FieldInfo, len(st.Fields.List))
					promotedCount := map[string]int{}
					for i, field := range st.Fields.List {
						if len(field.Names) != 0 {
							continue
						}
						promoted := resolveEmbeddedFields(pkg, allPkgs, field, &queue, processed)
						embeddedByIdx[i] = promoted
						for _, fi := range promoted {
							promotedCount[fi.Name]++
						}
					}

					// Process fields in declaration order, interleaving promoted fields
					// at the position of their embedding.
					for i, field := range st.Fields.List {
						if len(field.Names) == 0 {
							// Embedded field — include non-shadowed, non-ambiguous promoted fields.
							for _, fi := range embeddedByIdx[i] {
								if explicitNames[fi.Name] || promotedCount[fi.Name] > 1 {
									continue
								}
								info.Fields = append(info.Fields, fi)
							}
							continue
						}

						fieldName := field.Names[0].Name
						if fieldName == "_" {
							continue // blank-identifier fields are padding; skip
						}
						fieldInfo, err := fieldInfoFromExpr(pkg, allPkgs, fieldName, field.Type, &queue, processed)
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
			fmt.Printf("Warning: Could not find definition for targeted struct: %s\n", ref.Name)
		}
	}

	return parsedPkgName, parsedPkgPath, results, nil
}

// resolveEmbeddedFields resolves an embedded struct field and returns its
// promoted fields as flattened FieldInfo entries. The embedded struct itself
// is NOT added to the processing queue — only struct types referenced by its
// fields are queued (via fieldInfoFromExpr).
//
// Returns nil if the embedded type cannot be resolved, is not a struct, is a
// pointer embedding, or its package is not loaded.
func resolveEmbeddedFields(pkg *packages.Package, allPkgs []*packages.Package, field *ast.Field, queue *[]structRef, processed map[string]bool) []FieldInfo {
	// Detect pointer embedding (*Base) — skip for now.
	fieldType := field.Type
	if _, isPtr := fieldType.(*ast.StarExpr); isPtr {
		fmt.Println("Warning: Skipping pointer-embedded struct (not yet supported)")
		return nil
	}

	// Resolve the type via the type checker.
	typ := pkg.TypesInfo.TypeOf(fieldType)
	if typ == nil {
		return nil
	}

	named, ok := typ.(*types.Named)
	if !ok {
		fmt.Printf("Warning: Embedded type %s is not a named type; skipping\n", typ)
		return nil
	}

	if _, isStruct := named.Underlying().(*types.Struct); !isStruct {
		fmt.Printf("Warning: Embedded type %s is not a struct; skipping\n", named.Obj().Name())
		return nil
	}

	// Find the loaded package containing the embedded struct.
	embObjPkg := named.Obj().Pkg()
	if embObjPkg == nil {
		return nil
	}
	loadedPkg := findPkgByPath(allPkgs, embObjPkg.Path())
	if loadedPkg == nil {
		fmt.Printf("Warning: Package %s for embedded struct %s is not loaded; skipping\n",
			embObjPkg.Path(), named.Obj().Name())
		return nil
	}

	// Find the struct's AST declaration.
	structName := named.Obj().Name()
	var structType *ast.StructType
	for _, file := range loadedPkg.Syntax {
		ast.Inspect(file, func(n ast.Node) bool {
			ts, ok := n.(*ast.TypeSpec)
			if !ok || ts.Name.Name != structName {
				return true
			}
			if st, ok := ts.Type.(*ast.StructType); ok {
				structType = st
			}
			return false
		})
		if structType != nil {
			break
		}
	}

	if structType == nil {
		fmt.Printf("Warning: Could not find AST declaration for embedded struct %s\n", structName)
		return nil
	}

	// Iterate fields and build FieldInfo entries.
	var fields []FieldInfo
	for _, embField := range structType.Fields.List {
		if len(embField.Names) == 0 {
			// Nested embedding — skip (depth-1 flattening only).
			continue
		}

		embFieldName := embField.Names[0].Name
		if embFieldName == "_" {
			continue
		}

		fieldInfo, err := fieldInfoFromExpr(loadedPkg, allPkgs, embFieldName, embField.Type, queue, processed)
		if err != nil {
			fmt.Printf("Warning: Skipping promoted field %s from %s: %v\n", embFieldName, structName, err)
			continue
		}

		fields = append(fields, fieldInfo)
	}

	return fields
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
							if findPkgByPath(allPkgs, pkgPath) != nil {
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
					return buildMarshalFieldInfo(name, "*"+typ.String(), method, true), nil
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
					if findPkgByPath(allPkgs, pkgPath) != nil {
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
		return buildMarshalFieldInfo(name, typ.String(), method, false), nil
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
					Name:         name,
					GoType:       goTypeName,
					ArrowType:    arrowType,
					ArrowBuilder: arrowBuilder,
					CastType:     castType,
					IsPointer:    isPointer,
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
		Name:         name,
		GoType:       goTypeName,
		ArrowType:    arrowType,
		ArrowBuilder: arrowBuilder,
		CastType:     castType,
		IsPointer:    isPointer,
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
				if findPkgByPath(allPkgs, pkgPath) != nil {
					return buildStructFieldInfo(name, t.Obj().Name(), t.Obj().Pkg().Name(), pkgPath, isPointer, queue, processed), nil
				}
			}
			// External struct — try marshal methods.
			method := detectMarshalMethod(t)
			if method != "" {
				goType := t.String()
				if isPointer {
					goType = "*" + goType
				}
				return buildMarshalFieldInfo(name, goType, method, isPointer), nil
			}
			return FieldInfo{}, fmt.Errorf("external type %s does not implement TextMarshaler, Stringer, or BinaryMarshaler", t)

		case *types.Basic:
			return fieldInfoFromBasic(name, u, isPointer)

		case *types.Slice, *types.Map, *types.Array:
			return fieldInfoFromType(pkg, allPkgs, name, u, isPointer, queue, processed)

		default:
			method := detectMarshalMethod(t)
			if method != "" {
				goType := t.String()
				if isPointer {
					goType = "*" + goType
				}
				return buildMarshalFieldInfo(name, goType, method, isPointer), nil
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
			Name:         name,
			GoType:       goType,
			ArrowType:    "arrow.PrimitiveTypes.Int64",
			ArrowBuilder: "*array.Int64Builder",
			CastType:     "int64",
			IsPointer:    isPointer,
		}, true
	}

	if pkgPath == "time" && typeName == "Time" {
		goType := "time.Time"
		if isPointer {
			goType = "*time.Time"
		}
		return FieldInfo{
			Name:          name,
			GoType:        goType,
			ArrowType:     "arrow.FixedWidthTypes.Timestamp_ns",
			ArrowBuilder:  "*array.TimestampBuilder",
			CastType:      "arrow.Timestamp",
			ConvertMethod: "UnixNano",
			IsPointer:     isPointer,
		}, true
	}

	if pkgPath == "google.golang.org/protobuf/types/known/durationpb" && typeName == "Duration" {
		goType := "durationpb.Duration"
		if isPointer {
			goType = "*durationpb.Duration"
		}
		return FieldInfo{
			Name:          name,
			GoType:        goType,
			ArrowType:     "arrow.PrimitiveTypes.Int64",
			ArrowBuilder:  "*array.Int64Builder",
			CastType:      "int64",
			ConvertMethod: "AsDuration",
			IsPointer:     isPointer,
		}, true
	}

	if pkgPath == "google.golang.org/protobuf/types/known/timestamppb" && typeName == "Timestamp" {
		goType := "timestamppb.Timestamp"
		if isPointer {
			goType = "*timestamppb.Timestamp"
		}
		return FieldInfo{
			Name:          name,
			GoType:        goType,
			ArrowType:     "arrow.FixedWidthTypes.Timestamp_ns",
			ArrowBuilder:  "*array.TimestampBuilder",
			CastType:      "arrow.Timestamp",
			ConvertMethod: "AsTime().UnixNano",
			IsPointer:     isPointer,
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
		Name:         name,
		GoType:       goType,
		ArrowType:    fmt.Sprintf("arrow.ListOf(%s)", eltArrowType(eltInfo)),
		ArrowBuilder: "*array.ListBuilder",
		IsList:       true,
		EltInfo:      &eltInfo,
		IsPointer:    isPointer,
	}
}

// buildMapFieldInfo constructs a FieldInfo for a map type from its resolved key and value.
func buildMapFieldInfo(name string, keyInfo, valInfo FieldInfo, isPointer bool) FieldInfo {
	goType := fmt.Sprintf("map[%s]%s", keyInfo.GoType, valInfo.GoType)
	if isPointer {
		goType = "*" + goType
	}
	return FieldInfo{
		Name:         name,
		GoType:       goType,
		ArrowType:    fmt.Sprintf("arrow.MapOf(%s, %s)", keyInfo.ArrowType, eltArrowType(valInfo)),
		ArrowBuilder: "*array.MapBuilder",
		IsMap:        true,
		KeyInfo:      &keyInfo,
		EltInfo:      &valInfo,
		IsPointer:    isPointer,
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
	}
}

// buildByteSliceFieldInfo constructs a FieldInfo for a []byte field (Arrow Binary).
func buildByteSliceFieldInfo(name string, isPointer bool) FieldInfo {
	goType := "[]byte"
	if isPointer {
		goType = "*[]byte"
	}
	return FieldInfo{
		Name:         name,
		GoType:       goType,
		ArrowType:    "arrow.BinaryTypes.Binary",
		ArrowBuilder: "*array.BinaryBuilder",
		CastType:     "[]byte",
		IsPointer:    isPointer,
	}
}

// buildMarshalFieldInfo constructs a FieldInfo for an external type resolved via marshal method.
func buildMarshalFieldInfo(name string, goType string, method string, isPointer bool) FieldInfo {
	arrowType, arrowBuilder := marshalMethodArrowType(method)
	return FieldInfo{
		Name:          name,
		GoType:        goType,
		ArrowType:     arrowType,
		ArrowBuilder:  arrowBuilder,
		MarshalMethod: method,
		IsPointer:     isPointer,
	}
}
