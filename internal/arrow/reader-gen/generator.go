package readergen

import (
	"bytes"
	"fmt"
	"go/format"
	"os"

	"go.resystems.io/eddt/internal/arrow/gencommon"
)

// Generator holds the configuration for generating Arrow readers.
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

	// Reader-gen imports arrow and array but not memory.
	reserved := map[string]bool{"arrow": true, "array": true}
	packageName, imports, err := gencommon.ResolveOutputContext(parsedPkgName, structs, outPkgNameOverride, pkgAliases, reserved)
	if err != nil {
		return err
	}

	data := templateData{
		PackageName: packageName,
		Version:     g.Version,
		Imports:     imports,
		Structs:     structs,
	}

	var buf bytes.Buffer
	if err := readerTemplate.Execute(&buf, data); err != nil {
		return fmt.Errorf("failed to execute template: %w", err)
	}

	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		return fmt.Errorf("failed to format generated source: %w\nSource:\n%s", err, buf.String())
	}

	if err := os.WriteFile(g.OutPath, formatted, 0644); err != nil {
		return fmt.Errorf("failed to write output file: %w", err)
	}

	return nil
}
