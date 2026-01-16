package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var Commit string

var (
	root_verbose bool
)

func init() {
	rootCmd.Flags().BoolVarP(&root_verbose, "verbose", "v", false, "verbose output to stderr")
	rootCmd.Flags().StringVarP(&nats_config.URLS, "nats-urls", "n", "", "NATS server URLs")
	rootCmd.Flags().StringVarP(&nats_config.Creds, "nats-creds", "c", "", "NATS credentials")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:   "eddt",
	Short: "eddt provides a model of the event-drive digital twin design.",
	Long: `The event-driven digital twin that leverages NATS for information transport and communication.`,
	TraverseChildren: true, // note traversal can only be set on the root
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(os.Stderr, "Please select a subcommand.\n")
		fmt.Fprintf(os.Stderr, `

	For help please use: eddt --help

	For command line completion, please:
	- Ensure that eddt is available on your PATH.
	- Add the following to your ~/.bashrc

	source <(which eddt > /dev/null && eddt completion bash)

`)
	},
}
