package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(onCmd)

	onCmd.Flags().StringVarP(&nats_config.URLS, "nats-urls", "n", "", "NATS server URLs")
}

var onCmd = &cobra.Command{
	Use:   "on",
	Short: "Handle different events",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(os.Stdout, "Please choose a subcommand for the event to handle.\n")
	},
}
