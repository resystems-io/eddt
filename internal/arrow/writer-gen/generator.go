package writergen

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"go.resystems.io/eddt/internal/arrow/gencommon"
)

// Generator holds the configuration for generating Arrow writers.
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

// templateData is the data contract between Run and the writer template.
type templateData struct {
	PackageName   string
	Version       string
	Imports       []gencommon.ImportInfo
	Structs       []gencommon.StructInfo
	ElidedSchemas []gencommon.ElidedSchema
}

// Run executes the full generation pipeline: parse -> apply template -> write to file.
//
// If outPkgNameOverride is empty, the output package name is auto-detected from the first input package.
// If outPkgNameOverride is set and differs from a struct's origin package, the generated code will
// import that package and qualify struct type references accordingly.
//
// Package aliases are specified via PkgAliases in "importpath=alias" format, where importpath
// must be the full Go import path of the package (as resolved by packages.Load from the input
// directory). When an alias is provided, the generated import uses that alias and all type
// references are qualified with the alias instead of the base package name.
//
// External package types (e.g., netip.Addr, time.Time) that are NOT in the provided InputPkgs
// are handled through interface-based serialization with the following priority:
//  1. encoding.TextMarshaler — type is serialized to string via MarshalText()
//  2. fmt.Stringer — type is serialized to string via String()
//  3. encoding.BinaryMarshaler — type is serialized to binary via MarshalBinary()
//
// If the external type does not implement any of these interfaces, the field is skipped
// and a warning is emitted during generation.
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
	existing, err := gencommon.ScanOutputPackageSchemas(filepath.Dir(g.OutPath), g.OutPath, "Schema")
	if err != nil {
		return fmt.Errorf("scanning output package for existing schemas: %w", err)
	}
	structs, elidedSchemas := gencommon.PartitionByExistingSchemas(structs, existing, "Schema")

	// Writer-gen always imports arrow, array, and memory.
	reserved := map[string]bool{"arrow": true, "array": true, "memory": true}
	packageName, imports, err := gencommon.ResolveOutputContext(parsedPkgName, structs, outPkgNameOverride, pkgAliases, reserved)
	if err != nil {
		return err
	}

	data := templateData{
		PackageName:   packageName,
		Version:       g.Version,
		Imports:       imports,
		Structs:       structs,
		ElidedSchemas: elidedSchemas,
	}

	var buf bytes.Buffer
	if err := writerTemplate.Execute(&buf, data); err != nil {
		return fmt.Errorf("failed to execute template: %w", err)
	}

	return gencommon.WriteFormattedGo(g.OutPath, &buf)
}
