package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// The transform commands will manage the process of recording digital twin data as transform representations.
func init() {
	rootCmd.AddCommand(transformCmd)

	transformCmd.AddCommand(transformOverviewCmd)
	transformCmd.AddCommand(transformDownsamplingCmd)
	transformCmd.AddCommand(transformSynthesisCmd)
	transformCmd.AddCommand(transformComparisonCmd)
}

type TransformationConfig struct {
	SolutionIDs      []string
}

var transformCmd = &cobra.Command{
	Use:   "transform",
	Short: "Transform elements",
	Long:  `Transform provides mechanisms to transform event data prior to storage.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(os.Stdout, "Please choose a subcommand for managing transforms.\n")
	},
}

var transformOverviewCmd = &cobra.Command{
	Use:   "overview",
	Short: "Overview explains how transform data is recorded and accessed.",
	Long: `Overview explains how the event-drive digital twin domain is record in tables for bulk access.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(os.Stdout, `# EDDT Table Management

		Coming soon...
`)
	},
}

var transformInitialiseCmd = &cobra.Command{
	Use:   "downsample",
	Short: "Initialise any resources needed for transform dataset recording.",
	Long: `Initialise provides the mechanisms to initialise database resources needed to store transform data.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(os.Stdout, `# EDDT Table Management

		Not yet implemented.
`)
	},
}

var transformDownsamplingCmd = &cobra.Command{
	Use:   "downsample",
	Short: "Downsample performs aggregations of upstream event notifications.",
	Long: `Downsample will accumulate and aggregate upstream event notifications according to roll-ups
and aggregation instructions.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(os.Stdout, "Not yet implemented.")
	},
}

var transformSynthesisCmd = &cobra.Command{
	Use:   "synthesise",
	Short: "Synthesise new event streams based on incoming payloads.",
	Long: `Synthesise creates new payloads based on upstream event notification payloads.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(os.Stdout, "Not yet implemented.")
	},
}

var transformComparisonCmd = &cobra.Command{
	Use:   "compare",
	Short: "Compare generates data diffs.",
	Long: `Compare creates diffs of upstream datasets, relative to their predecessor history.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(os.Stdout, "Not yet implemented.")
	},
}
