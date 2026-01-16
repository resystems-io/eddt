package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// The tabulate commands will manage the process of recording digital twin data as tabular representations.
func init() {
	rootCmd.AddCommand(tabulateCmd)

	tabulateCmd.AddCommand(tabulateOverviewCmd)
	tabulateCmd.AddCommand(tabulateInitialiseCmd)
	tabulateCmd.AddCommand(tabulateRecordCmd)
	tabulateCmd.AddCommand(tabulateServeCmd)
}

type TabulationConfig struct {
	SolutionIDs      []string
}

var tabulateCmd = &cobra.Command{
	Use:   "tabulate",
	Short: "Tabulate elements",
	Long:  `Tabulate provides mechanisms to record tabular datasets describing system elements.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(os.Stdout, "Please choose a subcommand for managing relationships.\n")
	},
}

var tabulateOverviewCmd = &cobra.Command{
	Use:   "overview",
	Short: "Overview explains how tabular data is recorded and accessed.",
	Long: `Overview explains how the event-drive digital twin domain is record in tables for bulk access.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(os.Stdout, `# EDDT Table Management

		Coming soon...
`)
	},
}

var tabulateInitialiseCmd = &cobra.Command{
	Use:   "initialise",
	Short: "Initialise any resources needed for tabular dataset recording.",
	Long: `Initialise provides the mechanisms to initialise database resources needed to store tabular data.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(os.Stdout, `# EDDT Table Management

		Not yet implemented.
`)
	},
}

var tabulateRecordCmd = &cobra.Command{
	Use:   "record",
	Short: "Record incoming event notifications in tables.",
	Long: `Record payloads that have passed through routing and transformations
and that are ready for tabular storage in indexing.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(os.Stdout, "Not yet implemented.")
	},
}

var tabulateServeCmd = &cobra.Command{
	Use:   "serve",
	Short: "Serve query endpoints for accessing tabular data.",
	Long: `Serve query endpoints enabling access to tabular data.
In most cases the access will be achieved via the appropriate database backend.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(os.Stdout, "Not yet implemented.")
	},
}
