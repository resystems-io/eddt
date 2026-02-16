package deltaflow

import (
	_ "embed"
	"fmt"
	"log"

	"os"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"
)

// __go:generate__ go install github.com/hamba/avro/v2/cmd/avrogen@latest
//go:generate avrogen -pkg deltaflow -o avro_basics_simple.go -tags json:snake,yaml:upper-camel avro_basics.avsc

//go:embed avro_basics.avsc
var avro_basics_schema []byte

// Note, we will avoid using Avro IDL and rely on parsing all schema fragments in order.

func Example_avro_basics() {

	schema, err := avro.Parse(string(avro_basics_schema))
	if err != nil {
		log.Fatal(err)
	}

	in := SimpleRecord{A: 27, B: "foo"}

	data, err := avro.Marshal(schema, in)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(data)

	out := SimpleRecord{}
	err = avro.Unmarshal(schema, data, &out)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(out)

	// Output:
	// [54 6 102 111 111]
	// {27 foo}
}

// Avro OCF
//
// In this example we generate a number of records and write them to an OCF file.
//
// We can then read this using duckdb:
// ```
// INSTALL avro;
// LOAD avro;
// SELECT * FROM read_avro('testdata.avro');
// ```
func Example_avro_ocf() {

	// Create a new OCF file
	f, err := os.Create("testdata.avro")
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove("testdata.avro")

	// Create a new encoder (using the string schema, rather than a pre-parsed schema)
	enc, err := ocf.NewEncoder(string(avro_basics_schema), f)
	if err != nil {
		log.Fatal(err)
	}

	// Write some records
	for i := 0; i < 3; i++ {
		in := SimpleRecord{A: int64(i), B: fmt.Sprintf("foo-%d", i)}
		if err := enc.Encode(in); err != nil {
			log.Fatal(err)
		}
	}

	if err := enc.Close(); err != nil {
		log.Fatal(err)
	}
	f.Close()

	// Read it back to verify
	f, err = os.Open("testdata.avro")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	dec, err := ocf.NewDecoder(f)
	if err != nil {
		log.Fatal(err)
	}

	for dec.HasNext() {
		var out SimpleRecord
		if err := dec.Decode(&out); err != nil {
			log.Fatal(err)
		}
		fmt.Println(out)
	}

	// Output:
	// {0 foo-0}
	// {1 foo-1}
	// {2 foo-2}
}
