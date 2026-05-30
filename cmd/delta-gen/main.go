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
		inputPkgs           []string
		outPkgName          string
		pkgAliases          []string
		targetStructs       []string
		structsAlias        []string // secondary alias for --type/-t; merged in RunE
		outPath             string
		verbose             bool
		keyFieldValues      []string
		standalone          bool
		standaloneHash      string
		standaloneTypesFile string
	)

	cmd := &cobra.Command{
		Use:   "delta-gen [StructName ...]",
		Short: "Generate Apply, Diff, Coalesce, and EntityID from an annotated Snapshot struct",
		Long: `A code generation tool that reads an EDDT Snapshot struct annotated with
eddt:"entity.key" and delta.* struct tags and emits the companion Delta type
together with Apply, Diff, Coalesce, and EntityID methods.

Struct names may be passed as positional arguments or via --type (-t).

Example usage:
  # Single struct — output auto-derived as ue_snapshot_delta.go
  delta-gen UESnapshot

  # Multiple structs — writes one auto-derived file per struct
  delta-gen UESnapshot SessionSnapshot

  # Multiple structs into a single explicit output file
  delta-gen UESnapshot SessionSnapshot --out combined_delta.go

  # Explicit package path
  delta-gen --pkg ./internal/model UESnapshot

  # Package from go.mod (import path — requires 'go get' first)
  delta-gen --pkg github.com/user/repo/model --type UESnapshot

  # Alias a package to avoid name collisions (key is the full Go import path)
  delta-gen --pkg ./internal/model --pkg ./internal/types --pkg-alias myapp/internal/types=modeltypes UESnapshot`,
		Args: cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Merge --structs/-s into --type/-t (both target the same concept).
			targetStructs = append(targetStructs, structsAlias...)
			targetStructs = append(targetStructs, args...)
			if len(targetStructs) == 0 {
				return fmt.Errorf("at least one target struct must be specified " +
					"(as a positional argument or via --type / -t / --structs / -s)")
			}

			if standalone && standaloneHash != "blake2b" && standaloneHash != "sha256" {
				return fmt.Errorf("--standalone-hash: expected \"blake2b\" or \"sha256\", got %q", standaloneHash)
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
			if outPkgName != "" {
				log.Info("output package override", "pkg_name", outPkgName)
			}

			rev := vcsRevision()

			if outPath != "" {
				// Explicit --out: bundle all targets into one file.
				log.Info("generating delta types", "structs", targetStructs, "packages", inputPkgs, "out", outPath)
				gen := deltagen.New(deltagen.Config{
					InputPkgs:           inputPkgs,
					TargetStructs:       targetStructs,
					OutPath:             outPath,
					PkgAliases:          pkgAliases,
					Version:             rev,
					KeyFields:           keyFields,
					Log:                 log,
					OutPkgNameOverride:  outPkgName,
					Standalone:          standalone,
					StandaloneHash:      standaloneHash,
					StandaloneTypesFile: standaloneTypesFile,
				})
				if err := gen.Run(); err != nil {
					return err
				}
				fmt.Printf("Successfully generated %s\n", outPath)
				return nil
			}

			// No --out: auto-derive one file per target struct.
			for _, name := range targetStructs {
				derived := deriveOutPath(name)
				log.Info("generating delta type", "struct", name, "packages", inputPkgs, "out", derived)
				gen := deltagen.New(deltagen.Config{
					InputPkgs:          inputPkgs,
					TargetStructs:      []string{name},
					OutPath:            derived,
					PkgAliases:         pkgAliases,
					Version:            rev,
					KeyFields:          keyFields,
					Log:                log,
					OutPkgNameOverride: outPkgName,
				})
				if err := gen.Run(); err != nil {
					return fmt.Errorf("generating %s: %w", name, err)
				}
				fmt.Printf("Successfully generated %s\n", derived)
			}
			return nil
		},
	}

	cmd.Flags().StringSliceVarP(&inputPkgs, "pkg", "p", []string{"."}, "Input packages: filesystem paths (./internal/model) or Go import paths (github.com/user/repo/pkg). Import paths must be in your go.mod; run 'go get <pkg>' first if needed.")
	cmd.Flags().StringVarP(&outPkgName, "pkg-name", "n", "", "Output package name (defaults to input package name)")
	cmd.Flags().StringSliceVarP(&pkgAliases, "pkg-alias", "a", nil, "Aliases for imported packages in 'importpath=alias' format (e.g. go.example.com/pkg=mypkg)")
	cmd.Flags().StringSliceVarP(&targetStructs, "type", "t", nil, "Snapshot struct(s) to generate delta types for. Repeatable or comma-separated. May also be passed as positional args.")
	cmd.Flags().StringSliceVarP(&structsAlias, "structs", "s", nil,
		"Alias for --type / -t (matches arrow-{reader,writer}-gen convention).")
	cmd.Flags().StringVarP(&outPath, "out", "o", "", "Output file path. If omitted, each struct is written to its own auto-derived <snake_struct>_delta.go file. If set, all structs are bundled into the named file.")
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose output")
	cmd.Flags().StringSliceVar(&keyFieldValues, "key-field", nil,
		"Entity-key field override: bare 'FieldName' applies to all --type targets; "+
			"'StructName=FieldName' applies to one struct only. Repeatable and comma-separated. "+
			"Overrides the eddt:\"entity.key\" tag when both are present.")
	cmd.Flags().BoolVar(&standalone, "standalone", false,
		"Generate runtime-independent code: no eddt/runtime import, runtime.Header not required. "+
			"Apply/Diff/Coalesce are pure functions with no error return. "+
			"A companion local-types file is emitted alongside the *_delta.go file.")
	cmd.Flags().StringVar(&standaloneHash, "standalone-hash", "blake2b",
		"EntityID hash algorithm for standalone mode: \"blake2b\" (default, compatible with eddt/runtime) "+
			"or \"sha256\" (stdlib-only, different EntityID values). Ignored unless --standalone is set.")
	cmd.Flags().StringVar(&standaloneTypesFile, "standalone-types", "delta_types.go",
		"Filename of the generated companion local-types file in standalone mode "+
			"(default \"delta_types.go\"). Resolved relative to the output file directory. "+
			"Ignored unless --standalone is set.")

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
			return nil, fmt.Errorf("--key-field: struct %q is not listed in --type", structName)
		}
		result[structName] = fieldName
	}

	return result, nil
}

// deriveOutPath returns "<snake_case_struct>_delta.go".
//
// Snake-case rule (Go stringer convention): a "_" is inserted before an
// uppercase rune when it is either preceded by a lowercase/digit rune, or
// preceded by an uppercase rune that is itself followed by a lowercase rune
// (handles acronym boundaries like "UESnapshot" → "ue_snapshot" and
// "HTTPHandler" → "http_handler").  The result is lowercased.
func deriveOutPath(structName string) string {
	runes := []rune(structName)
	var b strings.Builder
	for i, r := range runes {
		if i == 0 {
			b.WriteRune(r)
			continue
		}
		prev := runes[i-1]
		next := rune(0)
		if i+1 < len(runes) {
			next = runes[i+1]
		}
		// Insert "_" before an uppercase rune when:
		//   (a) the previous rune is lowercase or a digit, OR
		//   (b) the previous rune is uppercase and the next is lowercase.
		if isUpper(r) && (isLowerOrDigit(prev) || (isUpper(prev) && isLower(next))) {
			b.WriteByte('_')
		}
		b.WriteRune(r)
	}
	return strings.ToLower(b.String()) + "_delta.go"
}

func isUpper(r rune) bool        { return r >= 'A' && r <= 'Z' }
func isLower(r rune) bool        { return r >= 'a' && r <= 'z' }
func isLowerOrDigit(r rune) bool { return isLower(r) || (r >= '0' && r <= '9') }

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
