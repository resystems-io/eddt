package readergen

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"go.resystems.io/eddt/internal/arrow/gencommon"
)

// Generator holds the configuration for generating Arrow readers.
type Generator struct {
	InputPkgs     []string
	TargetStructs []string
	OutPath       string
	Verbose       bool
	PkgAliases    []string  // raw alias mappings in "original=replacement" format
	Version       string    // short commitish for the generated header; may be empty
	Warn          io.Writer // destination for diagnostic warnings; defaults to os.Stderr
}

// warnf writes a formatted diagnostic message to g.Warn, falling back to
// os.Stderr when the field is nil.
func (g *Generator) warnf(format string, args ...any) {
	w := g.Warn
	if w == nil {
		w = os.Stderr
	}
	fmt.Fprintf(w, format, args...)
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

// Parse extracts StructInfo for the targeted structs via the shared gencommon engine.
// The returned pkgName and pkgPath refer to the first loaded input package (used for output package
// auto-detection when no --pkg-name override is given). Each StructInfo carries its own PkgPath/PkgName.
func (g *Generator) Parse() (string, string, []gencommon.StructInfo, error) {
	return gencommon.Parse(g.InputPkgs, g.TargetStructs, g.Verbose)
}

// Run executes the full generation pipeline: parse -> apply template -> write to file.
//
// If outPkgNameOverride is empty, the output package name is auto-detected from the first input package.
// If outPkgNameOverride is set and differs from a struct's origin package, the generated code will
// import that package and qualify struct type references accordingly.
func (g *Generator) Run(outPkgNameOverride string) error {
	pkgAliases, err := gencommon.ParsePkgAliases(g.PkgAliases)
	if err != nil {
		return err
	}

	parsedPkgName, _, structs, err := g.Parse()
	if err != nil {
		return err
	}

	// Elide schema helpers already declared by companion files in the output package.
	// See internal/arrow/gencommon/output_scan.go for semantics and limitations.
	existing, err := gencommon.ScanOutputPackageSchemas(filepath.Dir(g.OutPath), g.OutPath, "ArrowReader")
	if err != nil {
		return fmt.Errorf("scanning output package for existing schemas: %w", err)
	}
	structs, elidedSchemas := gencommon.PartitionByExistingSchemas(structs, existing, "ArrowReader")

	// Reader-gen imports arrow and array but not memory.
	reserved := map[string]bool{"arrow": true, "array": true}
	packageName, imports, err := gencommon.ResolveOutputContext(parsedPkgName, structs, outPkgNameOverride, pkgAliases, reserved)
	if err != nil {
		return err
	}

	// Collect extra imports needed by ConvertBackExpr (e.g. "time", protobuf packages).
	imports = gencommon.MergeImports(imports, gencommon.CollectConvertBackImports(structs))

	// Compute unmarshal flag and collect unmarshal imports (e.g. "net/netip").
	hasUnmarshal := gencommon.HasUnmarshalFields(structs)
	if hasUnmarshal {
		imports = gencommon.MergeImports(imports, gencommon.CollectUnmarshalImports(structs))
	}

	// Warn about Stringer-only fields (no unmarshal inverse).
	for _, si := range structs {
		for _, f := range si.Fields {
			if f.MarshalMethod == "String" && f.UnmarshalMethod == "" {
				g.warnf("Warning: field %s.%s uses String() with no unmarshal inverse; skipping in reader\n", si.Name, f.Name)
			}
		}
	}

	data := templateData{
		PackageName:        packageName,
		Version:            g.Version,
		Imports:            imports,
		Structs:            structs,
		HasUnmarshalFields: hasUnmarshal,
		ElidedSchemas:      elidedSchemas,
	}

	var buf bytes.Buffer
	if err := readerTemplate.Execute(&buf, data); err != nil {
		return fmt.Errorf("failed to execute template: %w", err)
	}

	return gencommon.WriteFormattedGo(g.OutPath, &buf)
}
