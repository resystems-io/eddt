package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	writergen "go.resystems.io/eddt/internal/arrow/writer-gen"
)

func newRootCmd() *cobra.Command {
	var (
		inputPkg      string
		outPkgName    string
		targetStructs []string
		outPath       string
		verbose       bool
	)

	cmd := &cobra.Command{
		Use:   "arrow-writer-gen",
		Short: "Generate Apache Arrow append writers for Go structs",
		Long: `A code generation tool to automatically generate high-performance reflection-free
Apache Arrow append writers for Go structs.

Example usage:
  arrow-writer-gen --pkg ./internal/model --structs User,Order --out custom_arrow_writer.go`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(targetStructs) == 0 {
				return fmt.Errorf("at least one target struct must be specified")
			}

			if verbose {
				fmt.Printf("Generating Arrow writers for structs: %v\n", targetStructs)
				fmt.Printf("Input package: %s\n", inputPkg)
				if outPkgName != "" {
					fmt.Printf("Output package override: %s\n", outPkgName)
				}
				fmt.Printf("Output file: %s\n", outPath)
			}

			gen := writergen.NewGenerator(inputPkg, targetStructs, outPath, verbose)
			if err := gen.Run(outPkgName); err != nil {
				return err
			}

			fmt.Printf("Successfully generated %s\n", outPath)
			return nil
		},
	}

	cmd.Flags().StringVarP(&inputPkg, "pkg", "p", ".", "Input package directory containing the structs")
	cmd.Flags().StringVarP(&outPkgName, "pkg-name", "n", "", "Output package name (defaults to input package name)")
	cmd.Flags().StringSliceVarP(&targetStructs, "structs", "s", nil, "Specific struct(s) to generate writers for (comma-separated)")
	cmd.Flags().StringVarP(&outPath, "out", "o", "arrow-writer-gen.go", "Output file path")
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose output")

	if err := cmd.MarkFlagRequired("structs"); err != nil {
		panic(err)
	}

	return cmd
}

func main() {
	if err := newRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}
