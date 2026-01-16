package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"

	"go.resystems.io/eddt/contract"
	"go.resystems.io/eddt/internal/route"
)

// The routing commands will manage the process of rerouting payloads relative to relationship mappings.
func init() {
	rootCmd.AddCommand(routeCmd)

	routeCmd.AddCommand(routeOverviewCmd)
	routeCmd.AddCommand(routeMonitorCmd)
	routeMonitorCmd.Flags().StringVarP(&route_config.RouteFile, "routes", "r", "", "File containing JSON route definitions.")
	routeMonitorCmd.Flags().StringVarP(&route_config.Group, "group", "g", "", "Router queue group to join (blank to remain independent of any group).")
	routeMonitorCmd.Flags().BoolVar(&route_config.Verbose, "verbose", false, "Log all relation set activity.")
	routeMonitorCmd.Flags().DurationVar(&route_config.ReadyTimeout, "ready", 5 * time.Second, "Timeout waiting for start-up readiness.")
}

type RouteConfig struct {
	RouteFile string
	Group     string
	Verbose   bool
	ReadyTimeout time.Duration
}

var route_config RouteConfig

var routeCmd = &cobra.Command{
	Use:   "route",
	Short: "Route elements",
	Long:  `Route provides mechanisms to monitor relationships and reroute payloads relative to route mappings.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(os.Stdout, "Please choose a subcommand for routing.\n")
	},
}

var routeOverviewCmd = &cobra.Command{
	Use:   "overview",
	Short: "Overview provides a summary of how routing works.",
	Long:  `Routing overview explains where routing fits into the event-driven digital twin architecture.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(os.Stdout, `# EDDT Routing

		Routing enables both fan-out (broadcast) and fan-in (coalescing) of upstream payloads based
		on dynamically learnt relationships between elements within the system-of-interest.

		In this way information arising from one part of the system can be surfaced relative to
		related system areas within the digital twin.

		Routing is achieved by tokenising the subject names and executing the related rewrite rules
		relative to lookups against routing tables. The routing tables are computed upstream and maintained
		in-memory by each router instance.
`)
	},
}

var routeMonitorCmd = &cobra.Command{
	Use:   "monitor",
	Short: "Monitor tracks relationships and reroutes payloads.",
	Long: `Monitor follows the K-V relationship sets managed by the relationship subsystem
and reroutes payloads by rewriting subjects relative to the relationships.`,
	Run: func(cmd *cobra.Command, args []string) {
		end := end_on_interrupt()
		err := RunFollowAndRoute(end)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
			os.Exit(1)
		}
	},
}

func RunFollowAndRoute(end <-chan struct{}) error {

	// connect to NATS
	nats_config.URLS = nats.DefaultURL

	opts := []nats.Option{nats.Name("EDDT - domain router")}
	opts = setupNATSConnOptions(opts)
	opts = setupNATSConfigOptions(opts)

	nc, err := nats.Connect(nats_config.URLS, opts...)
	if err != nil {
		return err
	}

	// start up the relationship follower
	follower := &route.RouterRelationFollower{
		NC:    nc,
		Noisy: route_config.Verbose,
	}
	fready := make(chan struct{})
	err = follower.Launch(end, fready)
	if err != nil {
		return err
	}
	// wait for ready
	select {
	case <-fready:
	case <-end:
	case <-time.After(route_config.ReadyTimeout):
		panic("Timed out waiting for the RouterRelationFollower to become ready.")
	}

	// start up the router
	router := &route.DomainRouter{
		NC:        nc,
		Relations: follower,
		Group:     route_config.Group,
	}
	rready := make(chan struct{})
	err = router.Launch(end, rready)
	if err != nil {
		return err
	}
	<-rready

	// wait for ready
	select {
	case <-rready:
	case <-end:
	case <-time.After(route_config.ReadyTimeout):
		panic("Timed out waiting for the DomainRouter to become ready.")
	}

	// load routes from file
	var routes []contract.Route

	if route_config.RouteFile != "" {
		jsonFile, err := os.Open(route_config.RouteFile)
		if err != nil {
			return err
		}
		defer jsonFile.Close()

		byteValue, err := io.ReadAll(jsonFile)
		if err != nil {
			return err
		}
		json.Unmarshal(byteValue, &routes)
	}

	// load the routes into the router
	for _, r := range routes {
		router.Routes <- r
	}

	// wait for completion
	<-follower.Done
	<-router.Done

	return nil
}
