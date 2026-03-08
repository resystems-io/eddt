package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	writergen "go.resystems.io/eddt/internal/arrow/writer-gen"
)

var (
	inputPkg      string
	targetStructs []string
	outPath       string
	verbose       bool
)

func newRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "arrow-writer-gen",
		Short: "Generate Apache Arrow append writers for Go structs",
		Long: `A code generation tool to automatically generate high-performance reflection-free
Apache Arrow append writers for Go structs.

Example usage:
  arrow-writer-gen --pkg ./internal/model --structs User,Order --out custom_arrow_writer.go`,
		RunE: runGenerator,
	}

	cmd.Flags().StringVarP(&inputPkg, "pkg", "p", ".", "Input package directory containing the structs")
	cmd.Flags().StringSliceVarP(&targetStructs, "structs", "s", nil, "Specific struct(s) to generate writers for (comma-separated)")
	cmd.Flags().StringVarP(&outPath, "out", "o", "arrow-writer-gen.go", "Output file path")
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose output")

	if err := cmd.MarkFlagRequired("structs"); err != nil {
		panic(err)
	}

	return cmd
}

func runGenerator(cmd *cobra.Command, args []string) error {
	if len(targetStructs) == 0 {
		return fmt.Errorf("at least one target struct must be specified")
	}

	if verbose {
		fmt.Printf("Generating Arrow writers for structs: %v\n", targetStructs)
		fmt.Printf("Input package: %s\n", inputPkg)
		fmt.Printf("Output file: %s\n", outPath)
	}

	gen := writergen.NewGenerator(inputPkg, targetStructs, outPath, verbose)
	// We'll use a simple package name extraction or fallback to "main" or "arrowwriters"
	// For simplicity, we just use a default "model" package name unless improved.
	pkgName := "model" // TODO: Perhaps add a --pkgName flag, or extract from AST
	if err := gen.Run(pkgName); err != nil {
		return err
	}

	fmt.Printf("Successfully generated %s\n", outPath)
	return nil
}

func main() {
	if err := newRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}
