package deltaflow

import (
	"database/sql"
	_ "embed"
	"fmt"
	"log"
	"path/filepath"
	"testing"

	"os"

	_ "github.com/duckdb/duckdb-go/v2"
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
//
// See: https://duckdb.org/docs/stable/core_extensions/avro
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

// Avro OCF to Parquet
//
// In this unit test we read an OCF file and write it to a Parquet file using duckdb
// and database/sql.
func Test_avro_ocf_to_parquet(t *testing.T) {
	// Create a temp directory
	tmpDir := t.TempDir()
	avroPath := filepath.Join(tmpDir, "simple-record.avro")
	parquetPath := filepath.Join(tmpDir, "simple-record.parquet")

	// Create a simple Avro OCF file
	f, err := os.Create(avroPath)
	if err != nil {
		t.Fatal(err)
	}

	enc, err := ocf.NewEncoder(string(avro_basics_schema), f)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		in := SimpleRecord{A: int64(i), B: fmt.Sprintf("foo-%d", i)}
		if err := enc.Encode(in); err != nil {
			t.Fatal(err)
		}
	}

	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Open DuckDB connection
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Bootstrap
	if _, err := db.Exec("INSTALL avro;"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("LOAD avro;"); err != nil {
		t.Fatal(err)
	}

	// Convert Avro to Parquet
	// Note: We need to use Query/Exec with string formatting because parameter substitution
	// might not be supported for the file paths in the COPY statement depending on the driver version.
	query := fmt.Sprintf("COPY (SELECT * FROM read_avro('%s')) TO '%s' (FORMAT PARQUET)", avroPath, parquetPath)
	if _, err := db.Exec(query); err != nil {
		t.Fatal(err)
	}

	// Verify Parquet file exists
	if _, err := os.Stat(parquetPath); os.IsNotExist(err) {
		t.Fatalf("parquet file not found at %s", parquetPath)
	}

	// Query the Parquet file to verify contents
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM read_parquet('%s') ORDER BY A", parquetPath))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	i := 0
	for rows.Next() {
		var a int64
		var b string
		if err := rows.Scan(&a, &b); err != nil {
			t.Fatal(err)
		}

		expectedA := int64(i)
		expectedB := fmt.Sprintf("foo-%d", i)

		if a != expectedA {
			t.Errorf("expected A=%d, got %d", expectedA, a)
		}
		if b != expectedB {
			t.Errorf("expected B=%s, got %s", expectedB, b)
		}
		i++
	}

	if i != 3 {
		t.Errorf("expected 3 records, got %d", i)
	}
}
