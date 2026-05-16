package main

import (
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"
	"strings"

	"github.com/spf13/cobra"
	deltagen "go.resystems.io/eddt/internal/deltagen"
)

func newRootCmd() *cobra.Command {
	var (
		inputPkgs      []string
		outPkgName     string
		pkgAliases     []string
		targetStructs  []string
		outPath        string
		verbose        bool
		keyFieldValues []string
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

			keyFields, err := parseKeyFields(keyFieldValues, targetStructs)
			if err != nil {
				return err
			}

			level := slog.LevelWarn
			if verbose {
				level = slog.LevelInfo
			}
			log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
			log.Info("generating delta types", "structs", targetStructs, "packages", inputPkgs, "out", outPath)
			if outPkgName != "" {
				log.Info("output package override", "pkg_name", outPkgName)
			}

			gen := deltagen.NewGenerator(inputPkgs, targetStructs, outPath, verbose, pkgAliases)
			gen.Version = vcsRevision()
			gen.KeyFields = keyFields
			gen.Log = log
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
	cmd.Flags().StringSliceVar(&keyFieldValues, "key-field", nil,
		"Entity-key field override: bare 'FieldName' applies to all --structs targets; "+
			"'StructName=FieldName' applies to one struct only. Repeatable and comma-separated. "+
			"Overrides the eddt:\"entity.key\" tag when both are present.")

	if err := cmd.MarkFlagRequired("structs"); err != nil {
		panic(err)
	}

	return cmd
}

// parseKeyFields converts the raw --key-field flag values into a per-struct map.
//
// Each value is either a bare FieldName (applies to all structs in targetStructs)
// or a StructName=FieldName pair (applies to one named struct). The two forms
// mirror the --pkg-alias importpath=alias convention. StringSliceVarP splits
// comma-separated values at the Cobra layer, so --key-field "A=X,B=Y" and two
// separate --key-field flags are equivalent.
//
// Precedence rules:
//   - Bare values are applied first (expanding to every struct).
//   - Per-struct values then overwrite the bare entry for the named struct.
//   - Two bare values for the same struct is an error (ambiguous).
//   - A StructName that is not in targetStructs is an error.
//   - An empty FieldName part (e.g. "StructName=") is an error.
func parseKeyFields(values []string, targetStructs []string) (map[string]string, error) {
	if len(values) == 0 {
		return nil, nil
	}

	structSet := make(map[string]bool, len(targetStructs))
	for _, s := range targetStructs {
		structSet[s] = true
	}

	result := make(map[string]string, len(targetStructs))

	// Pass 1: expand bare values to all structs. Two bare values for the same
	// struct are ambiguous — the last bare value would win silently, so we
	// detect the duplicate and error instead.
	var bareField string
	var bareSeen bool
	for _, v := range values {
		if strings.Contains(v, "=") {
			continue
		}
		if v == "" {
			return nil, fmt.Errorf("--key-field: empty field name")
		}
		if bareSeen && v != bareField {
			return nil, fmt.Errorf("--key-field: ambiguous bare values %q and %q; use StructName=FieldName form to target specific structs", bareField, v)
		}
		bareField = v
		bareSeen = true
		for _, s := range targetStructs {
			result[s] = v
		}
	}

	// Pass 2: apply per-struct values, overriding any bare entry.
	for _, v := range values {
		if !strings.Contains(v, "=") {
			continue
		}
		idx := strings.Index(v, "=")
		structName := v[:idx]
		fieldName := v[idx+1:]
		if structName == "" {
			return nil, fmt.Errorf("--key-field: missing struct name in %q", v)
		}
		if fieldName == "" {
			return nil, fmt.Errorf("--key-field: empty field name in %q", v)
		}
		if !structSet[structName] {
			return nil, fmt.Errorf("--key-field: struct %q is not listed in --structs", structName)
		}
		result[structName] = fieldName
	}

	return result, nil
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
