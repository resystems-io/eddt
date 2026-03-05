package main

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"go.resystems.io/eddt/internal/parquet/annotator"
)

var (
	targetDir     string
	targetFiles   []string
	targetStructs []string
	verbose       bool
)

func newRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "parquet-annotator",
		Short: "Annotate Go structs with Parquet metadata tags",
		Long: `A tool to automatically annotate Go structs with Parquet metadata tags.
It parses Go source files, finds structs, and adds appropriate 'parquet' tags
to the fields based on their types.

Example usage via go:generate:
  //go:generate go run go.resystems.io/eddt/cmd/parquet-annotator -d .`,
		RunE: runAnnotator,
	}

	cmd.Flags().StringVarP(&targetDir, "dir", "d", "", "Directory to walk and annotate Go files")
	cmd.Flags().StringSliceVarP(&targetFiles, "file", "f", nil, "Specific Go file(s) to annotate")
	cmd.Flags().StringSliceVarP(&targetStructs, "struct", "s", nil, "Specific struct(s) to annotate. If omitted, all in the file are annotated")
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose output")

	return cmd
}

func runAnnotator(cmd *cobra.Command, args []string) error {
	if targetDir == "" && len(targetFiles) == 0 {
		targetDir = "."
	}

	app := annotator.NewParquetAnnotator()
	app.Verbose = verbose
	app.TargetStructs = targetStructs

	end := make(chan struct{})
	defer close(end)

	tasksIn := app.Start(end)

	processFile := func(path string) error {
		if !strings.HasSuffix(path, ".go") {
			return nil
		}

		if verbose {
			fmt.Printf("Processing %s\n", path)
		}

		// Read file
		fileData, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("failed to read %s: %w", path, err)
		}
		inBuf := bytes.NewReader(fileData)
		var out bytes.Buffer

		// Send task to annotator
		done := make(chan error, 1) // Buffer 1 to avoid leaking if main exits early
		tasksIn <- annotator.Task{In: inBuf, Out: &out, Done: done}

		// Wait for this specific task to finish
		err = <-done
		if err != nil {
			return fmt.Errorf("failed to annotate %s: %w", path, err)
		}

		// Write output back to the file
		err = os.WriteFile(path, out.Bytes(), 0644)
		if err != nil {
			return fmt.Errorf("failed to write to %s: %w", path, err)
		}

		return nil
	}

	var filesToProcess []string

	// Collect specific files to process
	for _, f := range targetFiles {
		if _, err := os.Stat(f); os.IsNotExist(err) {
			return fmt.Errorf("file %s does not exist", f)
		}
		if strings.HasSuffix(f, ".go") {
			filesToProcess = append(filesToProcess, f)
		}
	}

	// Collect files from directory
	if targetDir != "" {
		if _, err := os.Stat(targetDir); os.IsNotExist(err) {
			return fmt.Errorf("directory %s does not exist", targetDir)
		}
		err := filepath.WalkDir(targetDir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if path != targetDir && strings.HasPrefix(d.Name(), ".") {
					return filepath.SkipDir
				}
				return nil
			}
			if strings.HasSuffix(path, ".go") {
				filesToProcess = append(filesToProcess, path)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	// Process files
	for _, path := range filesToProcess {
		if err := processFile(path); err != nil {
			return err
		}
	}

	return nil
}

func main() {
	if err := newRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}
