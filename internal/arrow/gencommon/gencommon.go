// Package gencommon provides shared type parsing and resolution logic for the
// arrow-writer-gen and arrow-reader-gen code generators. It loads Go packages
// via golang.org/x/tools/go/packages, resolves struct fields to Arrow type
// metadata (FieldInfo), and produces StructInfo records ready for template-based
// code generation.
package gencommon

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
)

// -- types: Core data structures shared by writer-gen and reader-gen.

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
	StructQualifier string     // If IsStruct=true, the package qualifier (e.g. "otherpkg." or "")
	CastType        string     // The Go type used when appending to the builder
	IsFixedSizeList bool       // True if the field is a fixed-size array ([N]T)
	FixedSizeLen    string     // The array length as a string literal (e.g. "4")
	MarshalMethod   string     // Serialization method for external types: "MarshalText", "String", "MarshalBinary", or ""
	ConvertMethod   string     // Method to call on the value before casting (e.g. "UnixNano" for time.Time)
	EltInfo         *FieldInfo // Element info for lists, fixed-size-lists, and map values (recursive)
	KeyInfo         *FieldInfo // Key info for maps

	// Reader-specific fields — populated during parsing for use by reader-gen templates.

	ArrowArrayType     string   // Concrete Arrow array type for downcast (e.g., "*array.Int32", "*array.List")
	ValueMethod        string   // Extraction method on the array type: "Value" for leaf types, "" for containers
	UnmarshalMethod    string   // Reciprocal of MarshalMethod: "UnmarshalText", "UnmarshalBinary", or "" (Stringer has no inverse)
	ConvertBackExpr    string   // Template snippet for the inverse of ConvertMethod (e.g., "time.Duration(%s)", "time.Unix(0, int64(%s))")
	ConvertBackIsPtr   bool     // True if ConvertBackExpr returns a pointer (e.g. durationpb.New → *durationpb.Duration)
	ConvertBackImports []string // Import paths needed by ConvertBackExpr (e.g. ["time"])
	UnmarshalImports   []string // Import paths needed by UnmarshalMethod (e.g., ["net/netip"])
	ZeroExpr           string   // Zero-value expression for the Go type, used for null handling (e.g., "0", `""`, "false", "nil")
}

// StructInfo contains information about a parsed Go struct.
type StructInfo struct {
	Name      string
	Fields    []FieldInfo
	PkgPath   string // import path of the package this struct belongs to
	PkgName   string // base package name of the package this struct belongs to
	Qualifier string // qualifier prefix for this struct in generated code (e.g. "mypkg." or "")
}

// ImportInfo describes a single package import in the generated file.
type ImportInfo struct {
	Path  string // full import path (e.g. "myapp/entities")
	Name  string // base package name (e.g. "entities")
	Alias string // alias to use in generated code; empty means use Name
}

// structRef identifies a struct to be processed, optionally qualified by package path.
// When PkgPath is empty (user-specified initial targets), all loaded packages are searched.
// When PkgPath is set (discovered via field references), only the specified package is searched.
type structRef struct {
	PkgPath string
	Name    string
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

// loadPackages loads all packages from inputPkgs. Filesystem paths (starting
// with ".", "..", or "/") are loaded one-at-a-time with cfg.Dir set, preserving
// support for separate Go modules. Go import paths are batched into a single
// packages.Load call resolved from the invoking module's go.mod.
func loadPackages(inputPkgs []string) ([]*packages.Package, error) {
	mode := packages.NeedName | packages.NeedFiles | packages.NeedSyntax |
		packages.NeedTypes | packages.NeedTypesInfo

	var all []*packages.Package
	var importPaths []string

	// Phase 1: Load filesystem paths one-at-a-time (may be separate modules).
	for _, input := range inputPkgs {
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

// FindPkgByPath returns the loaded package with the given import path, or nil.
func FindPkgByPath(pkgs []*packages.Package, pkgPath string) *packages.Package {
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
func Parse(inputPkgs, targetStructs []string, verbose bool) (string, string, []StructInfo, error) {
	allPkgs, err := loadPackages(inputPkgs)
	if err != nil {
		return "", "", nil, err
	}

	var parsedPkgName string
	var parsedPkgPath string
	if len(allPkgs) > 0 {
		parsedPkgName = allPkgs[0].Name
		parsedPkgPath = allPkgs[0].PkgPath
	}

	queue := make([]structRef, len(targetStructs))
	for i, t := range targetStructs {
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
			pkg := FindPkgByPath(allPkgs, ref.PkgPath)
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

		if !found && verbose {
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
	loadedPkg := FindPkgByPath(allPkgs, embObjPkg.Path())
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

// -- validation: Shared validation helpers used by both generators.

// DetectStructNameCollisions checks for duplicate struct names across different
// packages. Same-named structs would produce duplicate generated helper functions
// (e.g. two AppendInnerStruct), causing a compile error in the output.
func DetectStructNameCollisions(structs []StructInfo) error {
	seen := map[string][]string{} // name -> list of pkgPaths
	for _, si := range structs {
		seen[si.Name] = append(seen[si.Name], si.PkgPath)
	}
	for name, pkgs := range seen {
		if len(pkgs) > 1 {
			return fmt.Errorf("struct name %q appears in multiple packages (%s); generated helper function names would collide",
				name, strings.Join(pkgs, ", "))
		}
	}
	return nil
}

// FilterUnexportedFields removes unexported fields from structs that will be
// accessed cross-package (indicated by a non-empty Qualifier). Accessing
// unexported fields from another package is a compile error in Go.
func FilterUnexportedFields(structs []StructInfo, outputPkg string) {
	for i := range structs {
		si := &structs[i]
		if si.Qualifier == "" {
			continue // same package — all fields accessible
		}
		filtered := si.Fields[:0]
		for _, f := range si.Fields {
			if !token.IsExported(f.Name) {
				fmt.Printf("Warning: Skipping unexported field %s in %s (inaccessible from output package %q)\n",
					f.Name, si.Name, outputPkg)
				continue
			}
			filtered = append(filtered, f)
		}
		si.Fields = filtered
	}
}

// -- output-context: Cross-package resolution pipeline shared by both generators.

// ParsePkgAliases validates and parses raw "importpath=alias" strings into a map.
func ParsePkgAliases(raw []string) (map[string]string, error) {
	aliases := make(map[string]string, len(raw))
	for _, a := range raw {
		parts := strings.SplitN(a, "=", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("invalid --pkg-alias %q: expected format 'original=replacement'", a)
		}
		aliases[parts[0]] = parts[1]
	}
	return aliases, nil
}

// ResolveOutputContext prepares parsed StructInfo results for template-based code
// generation in the given output package. It performs the complete cross-package
// resolution pipeline:
//
//  1. Determines the output package name (override or auto-detected from parsedPkgName).
//  2. Detects struct name collisions across packages.
//  3. Builds the import map from struct package origins vs the output package.
//  4. Validates that no import name/alias collides with the reserved set.
//  5. Sets Qualifier on each StructInfo for cross-package type references.
//  6. Propagates StructQualifier to FieldInfo entries (recursively) for struct-typed fields.
//  7. Filters unexported fields from cross-package structs.
//
// pkgAliases maps full import paths to desired aliases. reservedNames is the set
// of identifier names already claimed by generator-specific imports (e.g.
// {"arrow": true, "array": true, "memory": true} for writer-gen).
//
// Returns the determined package name and a sorted imports list.
// Mutates structs in place (Qualifier, StructQualifier, field filtering).
func ResolveOutputContext(parsedPkgName string, structs []StructInfo, outPkgOverride string, pkgAliases map[string]string, reservedNames map[string]bool) (string, []ImportInfo, error) {
	if len(structs) == 0 {
		return "", nil, fmt.Errorf("no target structs found matching specifications")
	}

	// Detect struct name collisions across packages.
	if err := DetectStructNameCollisions(structs); err != nil {
		return "", nil, err
	}

	// Determine output package name.
	packageName := outPkgOverride
	if packageName == "" {
		packageName = parsedPkgName
		if packageName == "" {
			return "", nil, fmt.Errorf("could not determine package name from input; use --pkg-name to specify one explicitly")
		}
	}

	// resolveAlias returns the alias for a given package import path, or "".
	resolveAlias := func(pkgPath string) string {
		if alias, ok := pkgAliases[pkgPath]; ok {
			return alias
		}
		return ""
	}

	// Build the import map: collect unique packages that need to be imported.
	// A package needs importing when its name differs from the output package
	// name, or an alias mapping explicitly targets it.
	type importKey = string
	importMap := map[importKey]ImportInfo{}
	for _, si := range structs {
		if si.PkgPath == "" {
			continue
		}
		if _, exists := importMap[si.PkgPath]; exists {
			continue
		}
		alias := resolveAlias(si.PkgPath)
		if si.PkgName != packageName || alias != "" {
			importMap[si.PkgPath] = ImportInfo{
				Path:  si.PkgPath,
				Name:  si.PkgName,
				Alias: alias,
			}
		}
	}

	// Convert import map to a sorted slice for deterministic output.
	imports := make([]ImportInfo, 0, len(importMap))
	for _, imp := range importMap {
		imports = append(imports, imp)
	}
	sort.Slice(imports, func(i, j int) bool { return imports[i].Path < imports[j].Path })

	// Validate that reserved import names don't collide with any import or the output package.
	for _, imp := range imports {
		effectiveName := imp.Name
		if imp.Alias != "" {
			effectiveName = imp.Alias
		}
		if reservedNames[effectiveName] {
			return "", nil, fmt.Errorf("imported package alias/name %q collides with a required Arrow import; use --pkg-alias to choose a different alias", effectiveName)
		}
	}
	if reservedNames[packageName] {
		return "", nil, fmt.Errorf("output package name %q collides with an import used in generated code; choose a different --pkg-name", packageName)
	}

	// Set Qualifier on each StructInfo based on whether it needs an import.
	for i := range structs {
		si := &structs[i]
		if imp, ok := importMap[si.PkgPath]; ok {
			qualifier := imp.Name
			if imp.Alias != "" {
				qualifier = imp.Alias
			}
			si.Qualifier = qualifier + "."
		}
	}

	// Build a lookup from struct name → qualifier for propagation to FieldInfo.
	structQualifiers := map[string]string{}
	for _, si := range structs {
		structQualifiers[si.Name] = si.Qualifier
	}

	// Propagate StructQualifier to all struct-typed FieldInfo entries (recursively).
	for i := range structs {
		for j := range structs[i].Fields {
			setStructQualifiers(&structs[i].Fields[j], structQualifiers)
		}
	}

	// Filter unexported fields from cross-package structs.
	FilterUnexportedFields(structs, packageName)

	return packageName, imports, nil
}

// CollectConvertBackImports walks all struct fields (recursively through
// EltInfo/KeyInfo) and returns deduplicated ImportInfo entries required by
// ConvertBackExpr expressions in the reader-generated code.
func CollectConvertBackImports(structs []StructInfo) []ImportInfo {
	seen := map[string]bool{}
	var result []ImportInfo
	var walk func(fi *FieldInfo)
	walk = func(fi *FieldInfo) {
		for _, imp := range fi.ConvertBackImports {
			if !seen[imp] {
				seen[imp] = true
				// Derive base package name from the import path.
				parts := strings.Split(imp, "/")
				result = append(result, ImportInfo{Path: imp, Name: parts[len(parts)-1]})
			}
		}
		if fi.EltInfo != nil {
			walk(fi.EltInfo)
		}
		if fi.KeyInfo != nil {
			walk(fi.KeyInfo)
		}
	}
	for i := range structs {
		for j := range structs[i].Fields {
			walk(&structs[i].Fields[j])
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Path < result[j].Path })
	return result
}

// CollectUnmarshalImports walks all struct fields (recursively through
// EltInfo/KeyInfo) and returns deduplicated ImportInfo entries required by
// UnmarshalMethod expressions in the reader-generated code.
func CollectUnmarshalImports(structs []StructInfo) []ImportInfo {
	seen := map[string]bool{}
	var result []ImportInfo
	var walk func(fi *FieldInfo)
	walk = func(fi *FieldInfo) {
		for _, imp := range fi.UnmarshalImports {
			if !seen[imp] {
				seen[imp] = true
				parts := strings.Split(imp, "/")
				result = append(result, ImportInfo{Path: imp, Name: parts[len(parts)-1]})
			}
		}
		if fi.EltInfo != nil {
			walk(fi.EltInfo)
		}
		if fi.KeyInfo != nil {
			walk(fi.KeyInfo)
		}
	}
	for i := range structs {
		for j := range structs[i].Fields {
			walk(&structs[i].Fields[j])
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Path < result[j].Path })
	return result
}

// HasUnmarshalFields reports whether any field across all structs (recursively
// through EltInfo/KeyInfo) has a non-empty UnmarshalMethod.
func HasUnmarshalFields(structs []StructInfo) bool {
	var walk func(fi *FieldInfo) bool
	walk = func(fi *FieldInfo) bool {
		if fi.UnmarshalMethod != "" {
			return true
		}
		if fi.EltInfo != nil && walk(fi.EltInfo) {
			return true
		}
		if fi.KeyInfo != nil && walk(fi.KeyInfo) {
			return true
		}
		return false
	}
	for i := range structs {
		for j := range structs[i].Fields {
			if walk(&structs[i].Fields[j]) {
				return true
			}
		}
	}
	return false
}

// setStructQualifiers recursively sets StructQualifier on struct-typed fields
// and traverses EltInfo/KeyInfo trees for nested containers.
func setStructQualifiers(fi *FieldInfo, qualifiers map[string]string) {
	if fi.IsStruct && fi.StructName != "" {
		fi.StructQualifier = qualifiers[fi.StructName]
	}
	if fi.EltInfo != nil {
		setStructQualifiers(fi.EltInfo, qualifiers)
	}
	if fi.KeyInfo != nil {
		setStructQualifiers(fi.KeyInfo, qualifiers)
	}
}
