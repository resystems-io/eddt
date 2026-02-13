package deltaflow

import (
	_ "embed"
	"fmt"
	"log"

	"github.com/hamba/avro/v2"
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
