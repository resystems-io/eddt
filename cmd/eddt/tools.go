package main

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"net"
	"os"

	"github.com/spf13/cobra"

	sim "go.resystems.io/eddt/internal/simulation"
)

func init() {
	rootCmd.AddCommand(toolsCmd)

	toolsCmd.AddCommand(toolsIPCmd)
	toolsIPCmd.AddCommand(toolsIPToBase64Cmd)
	toolsIPCmd.AddCommand(toolsIPFromBase64Cmd)

	toolsCmd.AddCommand(toolsRelationsCmd)
	toolsRelationsCmd.AddCommand(relateDecodeCmd)
	toolsRelationsCmd.AddCommand(relateEncodeCmd)
}

var toolsCmd = &cobra.Command{
	Use:   "tools",
	Short: "event tools",
	Long:  `Various tools to help work with event data.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(os.Stdout, "Please choose a subcommand to select the tool.\n")
	},
}

var toolsIPCmd = &cobra.Command{
	Use:   "ip",
	Short: "IP related tools",
	Long:  `Tools to work with IP addresses`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(os.Stdout, "Please choose a subcommand to select the tool.\n")
	},
}

var toolsRelationsCmd = &cobra.Command{
	Use:   "rel",
	Short: "Relations related tools",
	Long:  `Tools to work with element relationships`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(os.Stdout, "Please choose a subcommand to select the tool.\n")
	},
}

var toolsIPToBase64Cmd = &cobra.Command{
	Use:   "tobase64",
	Short: "Convert string IP to base64",
	Long:  `Convert string IP to base64`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			// read IP strings from stdin
			scanner := bufio.NewScanner(os.Stdin)
			for scanner.Scan() {
				ipstring := scanner.Text()
				ip := net.ParseIP(ipstring)
				fmt.Fprintf(os.Stdout, "%s\n", sim.IPBase64(ip))
			}
			if err := scanner.Err(); err != nil {
				fmt.Fprintf(os.Stderr, "Error reading standard input: %v\n", err)
			}
			return
		}
		// convert string args
		for _, ipstring := range args {
			ip := net.ParseIP(ipstring)
			fmt.Fprintf(os.Stdout, "%s\n", sim.IPBase64(ip))
		}
	},
}

var toolsIPFromBase64Cmd = &cobra.Command{
	Use:   "frombase64",
	Short: "Convert base64 IP to string",
	Long:  `Convert base64 IP to string`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			// read base 64 from stdin
			scanner := bufio.NewScanner(os.Stdin)
			for scanner.Scan() {
				ipbase64 := scanner.Text()
				ipbytes, err := base64.URLEncoding.DecodeString(ipbase64)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to decode <%s>: %v\n", ipbase64, err)
					continue
				}
				ip := net.IP(ipbytes)
				fmt.Fprintf(os.Stdout, "%s\n", ip.String())
			}
			if err := scanner.Err(); err != nil {
				fmt.Fprintf(os.Stderr, "Error reading standard input: %v\n", err)
			}
			return
		}
		// convert base64 args
		for _, ipbase64 := range args {
			ipbytes, err := base64.URLEncoding.DecodeString(ipbase64)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to decode <%s>: %v\n", ipbase64, err)
				continue
			}
			ip := net.IP(ipbytes)
			fmt.Fprintf(os.Stdout, "%s\n", ip.String())
		}
	},
}
