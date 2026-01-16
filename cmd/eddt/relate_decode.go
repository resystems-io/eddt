package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/spf13/cobra"
	"go.resystems.io/eddt/internal/relate/relationset"
)

var relateDecodeCmd = &cobra.Command{
	Use:   "decode",
	Short: "Decode a relation set into JSON.",
	Long: `Decode reads in the relation set flatbuffer from stdin and displays it in JSON.

# Example

nats kv get EDDT-R-SETS resys.sol.sid.r.kv.st.si.dt.set --raw | eddt relate decode | jq .	
`,
	Run: func(cmd *cobra.Command, args []string) {
		// read in the buffer
		buf, err := io.ReadAll(os.Stdin)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to read from stdin: %s\n", err)
			os.Exit(1)
		}

		if len(buf) == 0 {
			return
		}

		// wrap the flatbuffer
		relationSet := relationset.GetRootAsRelationSet(buf, 0)

		// unpack into a struct
		r := relationSet.UnPack()

		// convert to JSON
		text, err := json.MarshalIndent(r, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to encode to JSON: %s\n", err)
			os.Exit(1)
		}

		// print out
		n, err := fmt.Fprintln(os.Stdout, string(text))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to write out all JSON [%d of %d]: %s\n", n, len(text), err)
			os.Exit(1)
		}
	},
}

var relateEncodeCmd = &cobra.Command{
	Use:   "encode",
	Short: "Encode a JSON relation set.",
	Long: `Ecode reads in the JSON relation set and converts it to the flatbuffer representation.

# Example

cat relation-set.json | eddt relate encode
`,
	Run: func(cmd *cobra.Command, args []string) {
		// read in the buffer
		buf, err := io.ReadAll(os.Stdin)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to read from stdin: %s\n", err)
			os.Exit(1)
		}
		if len(buf) == 0 {
			return
		}

		// unmarshal the JSON into RelationSetT
		var rs relationset.RelationSetT
		err = json.Unmarshal(buf, &rs)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to unmarshal from JSON: %s\n", err)
			os.Exit(1)
		}

		// pack the RelationSet into a buffer
		b := flatbuffers.NewBuilder(1024)
		b.Finish(rs.Pack(b))
		buf = b.FinishedBytes()

		// write out the buffer to stdout
		n, err := os.Stdout.Write(buf)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to write out all of the buffer [%d of %d]: %s\n", n, len(buf), err)
			os.Exit(1)
		}
	},
}
