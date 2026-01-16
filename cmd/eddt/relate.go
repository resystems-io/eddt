package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	"go.resystems.io/eddt/contract"
)

// The relationship commands will manage the process of recording relationships between elements.
func init() {
	rootCmd.AddCommand(relateCmd)

	relateCmd.AddCommand(relateOverviewCmd)
	relateCmd.AddCommand(relateInitialiseCmd)
	relateCmd.AddCommand(relateCompileCmd)
	relateCmd.AddCommand(relateAssertCmd)
	relateCmd.AddCommand(relateExpireCmd)
	relateCmd.AddCommand(relateConstrainCmd)
	relateCmd.AddCommand(relateRetractCmd)
	relateCmd.AddCommand(relateDecodeCmd)
	relateCmd.AddCommand(relateEncodeCmd)

	relateCompileCmd.Flags().StringVarP(&relate_compiler_config.RulesFile, "rules", "r", "", "File containing JSON rules definitions.")
}

type RelateCompilerConfig struct {
	RulesFile string
}

var relate_compiler_config RelateCompilerConfig

var relateCmd = &cobra.Command{
	Use:   "relate",
	Short: "Relate elements",
	Long:  `Relate provides mechanisms to process observations and draw relationships between elements.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(os.Stdout, "Please choose a subcommand for managing relationships.\n")
	},
}

var relateOverviewCmd = &cobra.Command{
	Use:   "overview",
	Short: "Overview describes how relationships are tracked.",
	Long: `Overview explains the process whereby upstream event notifications are used to maintain
relationship mappings.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(os.Stdout, `# EDDT Relationship Management

		Relationships are the directional edges drawn between two elements within the digital
		twin system. These relationships are in constant flux. The relationship subsystem
		continually tracks and maintains a representation of the connections between elements.
		The elements are represented by their unique domain specific identifiers. The incoming
		event notification data is first compiled into a high frequency flow of assertions.
		These assertions are hashed to facilitate dedepulication. Once deduplicated, the
		assertions are used to update mapping sets that link a given element to a number of
		other elements of a given type. These sets are stored as K-V pairs.

		In addition to direct storage of asserted relationships, the individual edges are
		also tracked relative to liveness based on TTL values. Each compilation rule has an
		associated TTL that is assigned to the edge.

		Finally, there are constraint rules that are used to impose eventually consistent
		transitive relationship rules. When a constraint is violated the oldest relationships
		are pruned until the violation is negated. Other violation mitigation strategies may
		be added in the future.

		## Monitoring

		nats subscribe 'resys.sol.*.r.assert.>' -r | xxd
		nats subscribe 'resys.sol.*.r.assert.>' --graph

		## Observe

		nats kv get --raw EDDT-R-SETS resys.sol.test-solution.r.kv.imsi.454961482134993.ip.set | eddt tools rel decode  | jq .
`)
	},
}

var relateInitialiseCmd = &cobra.Command{
	Use:   "initialise",
	Short: "Initialise any resources needed for relationship tracking.",
	Long:  `Initialise provides the mechanisms to initialise NATS resources needed to enable relationship tracking.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(os.Stdout, `# EDDT Relationship Management

		Create a stream for the assertions created based on observations.
		This stream is used to enforce deduplication relative to equivalent asserts.

		Note, assertions for many different element relationships are sent via the
		same subject.

		Note, currently retention is based on interest. However, this may be moved
		to be based on 'work' in the future.

		## Initialise

		nats								\
			stream add EDDT-R-ASSERTS			\
			--subjects "resys.r.asserts"		\
			--storage=memory				\
			--max-bytes=1GiB				\
			--max-msgs=-1					\
			--max-msgs-per-subject=-1		\
			--max-age=10m					\
			--max-msg-size=500				\
			--retention=interest			\
			--discard=old					\
			--dupe-window=1m				\
			--replicas=1					\
			--no-allow-rollup				\
			--no-deny-delete				\
			--deny-purge


		nats kv add EDDT-R-SETS				\
			--history=10					\
			--storage=file					\
			--ttl=24h						\
			--marker-ttl=1h					\
			--replicas=1					\
			--max-value-size=100KiB			\
			--max-bucket-size=10GiB

		## Clear

		nats kv ls EDDT-R-SETS | xargs -I@ -P 100 nats kv del --timeout=10s --force EDDT-R-SETS @
`)
	},
}

var relateCompileCmd = &cobra.Command{
	Use:   "compile",
	Short: "Compile incoming observations into assertions.",
	Long:  `Observation messages are compiled into a stream of simple relationship assertions.`,
	Run: func(cmd *cobra.Command, args []string) {
		end := end_on_interrupt()

		// load rules from file
		var rules []contract.CompilerRule

		if relate_compiler_config.RulesFile != "" {
			jsonFile, err := os.Open(relate_compiler_config.RulesFile)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%v", err)
				os.Exit(1)
			}
			defer jsonFile.Close()

			byteValue, err := io.ReadAll(jsonFile)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%v", err)
				os.Exit(1)
			}
			json.Unmarshal(byteValue, &rules)
		}

		err := RunObservationCompilations(rules, end)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
			os.Exit(1)
		}
	},
}

var relateAssertCmd = &cobra.Command{
	Use:   "assert",
	Short: "Assert relationships between network elements",
	Long: `Assert maintains relationships between elements in the network.
Relationships are encoded as sets stored in NATS K-V.`,
	Run: func(cmd *cobra.Command, args []string) {
		end := end_on_interrupt()
		err := RunAssertionProcessing(end)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
			os.Exit(1)
		}
	},
}

var relateExpireCmd = &cobra.Command{
	Use:   "expire",
	Short: "Expire relationships that reach their TTL",
	Long: `Expire ensure that relationships that have not been refreshed are removed.
This works in conjunction with the underlying NATS TTL process.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(os.Stdout, "Not yet implemented.")
	},
}

var relateConstrainCmd = &cobra.Command{
	Use:   "constrain",
	Short: "Constrain relationships to meet configured predicates.",
	Long: `Constraints are imposed with eventually consistent system semantics.
Newly asserted relationships may temporarily violate the domain constraints.
Any violations that are observed are corrected by issuing retractions of the
oldest relationships, such that subsequently the violation will be avoided.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(os.Stdout, "Not yet implemented.")
	},
}

var relateRetractCmd = &cobra.Command{
	Use:   "retract",
	Short: "Retract relationships if the TTL expiry is reached.",
	Long: `Retract processes any explicit retractions created externally
or those by the constraint and expiry functions.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(os.Stdout, `Not yet implemented.

Note, any assertion with a TTL of zero (or negative) if interpreted as a retraction.
Given this, we may not need this explicit function.
`)
	},
}
