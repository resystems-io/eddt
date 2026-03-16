package main

import (
	"fmt"
	"os"
	"runtime/debug"

	"github.com/spf13/cobra"
	readergen "go.resystems.io/eddt/internal/arrow/reader-gen"
)

func newRootCmd() *cobra.Command {
	var (
		inputPkgs     []string
		outPkgName    string
		pkgAliases    []string
		targetStructs []string
		outPath       string
		verbose       bool
	)

	cmd := &cobra.Command{
		Use:   "arrow-reader-gen",
		Short: "Generate Apache Arrow readers for Go structs",
		Long: `A code generation tool to automatically generate high-performance reflection-free
Apache Arrow readers for Go structs.

Example usage:
  # Single package
  arrow-reader-gen --pkg ./internal/model --structs User,Order --out custom_arrow_reader.go

  # Multiple packages (structs from pkg2 are resolved natively, not via unmarshal fallback)
  arrow-reader-gen --pkg ./internal/model --pkg ./internal/types --structs Outer --out reader.go

  # Package from go.mod (import path — requires 'go get' first)
  arrow-reader-gen --pkg github.com/user/repo/model --structs User --out reader.go

  # Alias a package to avoid name collisions (key is the full Go import path)
  arrow-reader-gen --pkg ./internal/model --pkg ./internal/types --pkg-alias myapp/internal/types=modeltypes --structs Outer --out reader.go`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(targetStructs) == 0 {
				return fmt.Errorf("at least one target struct must be specified")
			}

			if verbose {
				fmt.Printf("Generating Arrow readers for structs: %v\n", targetStructs)
				fmt.Printf("Input packages: %v\n", inputPkgs)
				if outPkgName != "" {
					fmt.Printf("Output package override: %s\n", outPkgName)
				}
				fmt.Printf("Output file: %s\n", outPath)
			}

			gen := readergen.NewGenerator(inputPkgs, targetStructs, outPath, verbose, pkgAliases)
			gen.Version = vcsRevision()
			if err := gen.Run(outPkgName); err != nil {
				return err
			}

			fmt.Printf("Successfully generated %s\n", outPath)
			return nil
		},
	}

	cmd.Flags().StringSliceVarP(&inputPkgs, "pkg", "p", []string{"."}, "Input packages: filesystem paths (./internal/model) or Go import paths (github.com/user/repo/pkg). Import paths must be in your go.mod; run 'go get <pkg>' first if needed.")
	cmd.Flags().StringVarP(&outPkgName, "pkg-name", "n", "", "Output package name (defaults to input package name)")
	cmd.Flags().StringSliceVarP(&pkgAliases, "pkg-alias", "a", nil, "Aliases for imported packages in 'importpath=alias' format (e.g. go.example.com/pkg=mypkg)")
	cmd.Flags().StringSliceVarP(&targetStructs, "structs", "s", nil, "Specific struct(s) to generate readers for (comma-separated)")
	cmd.Flags().StringVarP(&outPath, "out", "o", "arrow-reader-gen.go", "Output file path")
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose output")

	if err := cmd.MarkFlagRequired("structs"); err != nil {
		panic(err)
	}

	return cmd
}

func vcsRevision() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	for _, s := range info.Settings {
		if s.Key == "vcs.revision" && len(s.Value) >= 8 {
			return s.Value[:8]
		}
	}
	return ""
}

func main() {
	if err := newRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}
