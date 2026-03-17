package writergen

import (
	"go.resystems.io/eddt/internal/arrow/gencommon"
)

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

// Parse extracts StructInfo for the targeted structs via the shared gencommon engine.
// The returned pkgName and pkgPath refer to the first loaded input package (used for output package
// auto-detection when no --pkg-name override is given). Each StructInfo carries its own PkgPath/PkgName.
func (g *Generator) Parse() (string, string, []gencommon.StructInfo, error) {
	return gencommon.Parse(g.InputPkgs, g.TargetStructs, g.Verbose)
}
