package main

import (
	"fmt"
	"os"
	"runtime/debug"

	"github.com/spf13/cobra"
	deltagen "go.resystems.io/eddt/internal/deltagen"
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
		Use:   "delta-gen",
		Short: "Generate Apply, Diff, Coalesce, and EntityID from an annotated Snapshot struct",
		Long: `A code generation tool that reads an EDDT Snapshot struct annotated with
eddt:"entity.key" and delta.* struct tags and emits the companion Delta type
together with Apply, Diff, Coalesce, and EntityID methods.

Example usage:
  # Generate for a single Snapshot struct
  delta-gen --pkg ./internal/model --structs UESnapshot --out ue_snapshot_delta.go

  # Multiple input packages (structs from pkg2 resolved natively)
  delta-gen --pkg ./internal/model --pkg ./internal/types --structs UESnapshot --out ue_delta.go

  # Package from go.mod (import path — requires 'go get' first)
  delta-gen --pkg github.com/user/repo/model --structs UESnapshot --out ue_delta.go

  # Alias a package to avoid name collisions (key is the full Go import path)
  delta-gen --pkg ./internal/model --pkg ./internal/types --pkg-alias myapp/internal/types=modeltypes --structs UESnapshot --out ue_delta.go`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(targetStructs) == 0 {
				return fmt.Errorf("at least one target struct must be specified")
			}

			if verbose {
				fmt.Printf("Generating delta types for structs: %v\n", targetStructs)
				fmt.Printf("Input packages: %v\n", inputPkgs)
				if outPkgName != "" {
					fmt.Printf("Output package override: %s\n", outPkgName)
				}
				fmt.Printf("Output file: %s\n", outPath)
			}

			gen := deltagen.NewGenerator(inputPkgs, targetStructs, outPath, verbose, pkgAliases)
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
	cmd.Flags().StringSliceVarP(&targetStructs, "structs", "s", nil, "Snapshot struct(s) to generate delta types for (comma-separated)")
	cmd.Flags().StringVarP(&outPath, "out", "o", "delta-gen.go", "Output file path")
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
