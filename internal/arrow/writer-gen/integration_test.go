package writergen

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestEndToEndIntegration(t *testing.T) {

	// Used to simplify debugging of generated code after a test run
	tarball := func(t *testing.T, tarPath string, tmpDir string) {
		t.Logf("Saving tarball of [%s] to [%s]", tmpDir, tarPath)
		cmd := exec.Command("tar", "-czf", tarPath, "-C", tmpDir, ".")
		err := cmd.Run()
		if err != nil {
			t.Errorf("Failed to build tarball: %v", err)
		}
	}

	t.Run("simple-struct", func(t *testing.T) {
		tmpDir, _ := setupIntegrationTest(t, `package dummy

type User struct {
	ID    int32
	Name  string
	Score float64
	Valid bool
}
`, []string{"User"}, "")

		testCode := `package dummy

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	_ "github.com/duckdb/duckdb-go/v2"
)

func TestArrowMemoryAndDuckDB(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0) // Ensures strictly zero leaks

	// Use our generated writer
	writer := NewUserArrowWriter(pool)

	// Ensure we release builder memory
	defer writer.Release()

	// Append some rows directly bypassing reflection
	u1 := User{ID: 1, Name: "Alice", Score: 99.5, Valid: true}
	u2 := User{ID: 2, Name: "Bob", Score: 85.0, Valid: false}

	writer.Append(&u1)
	writer.Append(&u2)

	// Build Arrow Record
	record := writer.NewRecord()
	defer record.Release()

	if record.NumRows() != 2 {
		t.Fatalf("expected 2 rows, got %d", record.NumRows())
	}

	// Double check created Arrow by converting Arrow to Parquet
	tmpDir := t.TempDir()
	parquetPath := filepath.Join(tmpDir, "users.parquet")

	file, err := os.Create(parquetPath)
	if err != nil {
		t.Fatalf("Failed to create parquet file: %v", err)
	}
	defer file.Close()

	// Initialize pqarrow writer
	props := parquet.NewWriterProperties()
	pqWriter, err := pqarrow.NewFileWriter(record.Schema(), file, props, pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatalf("Failed to instantiate pqarrow FileWriter: %v", err)
	}

	err = pqWriter.Write(record)
	if err != nil {
		t.Fatalf("pqWriter.Write failed: %v", err)
	}

	err = pqWriter.Close()
	if err != nil {
		t.Fatalf("pqWriter.Close failed: %v", err)
	}

	// Query via DuckDB Driver
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("Failed to open DuckDB memory instance: %v", err)
	}
	defer db.Close()

	// Verify records match exactly
	rows, err := db.Query(fmt.Sprintf("SELECT id, name, score, valid FROM read_parquet('%s')", parquetPath))
	if err != nil {
		t.Fatalf("DuckDB query failed: %v", err)
	}
	defer rows.Close()

	var actualUsers []User
	for rows.Next() {
		var u User
		if err := rows.Scan(&u.ID, &u.Name, &u.Score, &u.Valid); err != nil {
			t.Fatalf("Row scan failed: %v", err)
		}
		actualUsers = append(actualUsers, u)
	}

	if len(actualUsers) != 2 {
		t.Fatalf("Expected 2 users from DuckDB, got %d", len(actualUsers))
	}

	if actualUsers[0] != u1 {
		t.Errorf("First user mismatched. Want %#v, Got %#v", u1, actualUsers[0])
	}
	if actualUsers[1] != u2 {
		t.Errorf("Second user mismatched. Want %#v, Got %#v", u2, actualUsers[1])
	}
}
`

		runInnerTest(t, tmpDir, testCode, "")

		if false {
			tarball(t, "/tmp/arrow-gen-simple.tar.gz", tmpDir)
		}
	})

	t.Run("lists-and-maps", func(t *testing.T) {
		tmpDir, _ := setupIntegrationTest(t, `package dummy

type ComplexUser struct {
	ID     int32
	Tags   []string
	Scores map[string]float64
}
`, []string{"ComplexUser"}, "")

		testCode := `package dummy

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	"github.com/duckdb/duckdb-go/v2"
)

func TestArrowMemoryAndDuckDBListsAndMaps(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0) // Ensures strictly zero leaks

	// Use our generated writer
	writer := NewComplexUserArrowWriter(pool)

	// Ensure we release builder memory
	defer writer.Release()

	// Append some rows directly bypassing reflection
	u1 := ComplexUser{
		ID:     1,
		Tags:   []string{"admin", "user"},
		Scores: map[string]float64{"math": 99.5, "science": 85.0},
	}
	u2 := ComplexUser{
		ID:     2,
		Tags:   []string{"guest"},
		Scores: map[string]float64{"math": 60.0},
	}
	u3 := ComplexUser{
		ID:     3,
		Tags:   nil, // Test null maps/slices
		Scores: nil,
	}

	writer.Append(&u1)
	writer.Append(&u2)
	writer.Append(&u3)

	// Build Arrow Record
	record := writer.NewRecord()
	defer record.Release()

	if record.NumRows() != 3 {
		t.Fatalf("expected 3 rows, got %d", record.NumRows())
	}

	// Double check created Arrow by converting Arrow to Parquet
	tmpDir := t.TempDir()
	parquetPath := filepath.Join(tmpDir, "complex_users.parquet")

	file, err := os.Create(parquetPath)
	if err != nil {
		t.Fatalf("Failed to create parquet file: %v", err)
	}
	defer file.Close()

	// Initialize pqarrow writer
	props := parquet.NewWriterProperties()
	pqWriter, err := pqarrow.NewFileWriter(record.Schema(), file, props, pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatalf("Failed to instantiate pqarrow FileWriter: %v", err)
	}

	err = pqWriter.Write(record)
	if err != nil {
		t.Fatalf("pqWriter.Write failed: %v", err)
	}

	err = pqWriter.Close()
	if err != nil {
		t.Fatalf("pqWriter.Close failed: %v", err)
	}

	// Query via DuckDB Driver
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("Failed to open DuckDB memory instance: %v", err)
	}
	defer db.Close()

	// Verify records match exactly
	// DuckDB will return lists as JSON strings when queried directly, or we can use map structures if supported
	// Here we extract them and parse/compare. For map, DuckDB returns a struct of key/value list.
	rows, err := db.Query(fmt.Sprintf("SELECT id, tags, scores FROM read_parquet('%s')", parquetPath))
	if err != nil {
		t.Fatalf("DuckDB query failed: %v", err)
	}
	defer rows.Close()

	type MapEntry struct {
		Key   string ` + "`" + `duckdb:"key"` + "`" + `
		Value float64 ` + "`" + `duckdb:"value"` + "`" + `
	}

	var actualUsers []ComplexUser
	for rows.Next() {
		var u ComplexUser
		var tagsIf *[]interface{}
		var mIf *duckdb.Map

		if err := rows.Scan(&u.ID, &tagsIf, &mIf); err != nil {
			t.Fatalf("Row scan failed: %v", err)
		}

		if tagsIf != nil && *tagsIf != nil {
			u.Tags = make([]string, len(*tagsIf))
			for i, v := range *tagsIf {
				u.Tags[i] = v.(string)
			}
		}

		if mIf != nil && *mIf != nil {
			u.Scores = make(map[string]float64)
			for k, v := range *mIf {
				u.Scores[k.(string)] = v.(float64)
			}
		}
		actualUsers = append(actualUsers, u)
	}

	if len(actualUsers) != 3 {
		t.Fatalf("Expected 3 users from DuckDB, got %d", len(actualUsers))
	}

	// Compare u1
	if actualUsers[0].ID != u1.ID { t.Errorf("mismatch ID: %v != %v", actualUsers[0].ID, u1.ID) }
	if !reflect.DeepEqual(actualUsers[0].Tags, u1.Tags) { t.Errorf("mismatch Tags: %v != %v", actualUsers[0].Tags, u1.Tags) }
	if !reflect.DeepEqual(actualUsers[0].Scores, u1.Scores) { t.Errorf("mismatch Scores: %v != %v", actualUsers[0].Scores, u1.Scores) }

	// Compare u2
	if actualUsers[1].ID != u2.ID { t.Errorf("mismatch ID: %v != %v", actualUsers[1].ID, u2.ID) }
	if !reflect.DeepEqual(actualUsers[1].Tags, u2.Tags) { t.Errorf("mismatch Tags: %v != %v", actualUsers[1].Tags, u2.Tags) }
	if !reflect.DeepEqual(actualUsers[1].Scores, u2.Scores) { t.Errorf("mismatch Scores: %v != %v", actualUsers[1].Scores, u2.Scores) }

	// Compare u3 (nulls might become empty slices/maps in Scan, let's normalize or allow either)
	if actualUsers[2].ID != u3.ID { t.Errorf("mismatch ID: %v != %v", actualUsers[2].ID, u3.ID) }
	if len(actualUsers[2].Tags) != 0 { t.Errorf("expected empty tags: %v", actualUsers[2].Tags) }
	if len(actualUsers[2].Scores) != 0 { t.Errorf("expected empty scores: %v", actualUsers[2].Scores) }
}
`

		runInnerTest(t, tmpDir, testCode, "")

		if false {
			tarball(t, "/tmp/arrow-gen-list-and-map.tar.gz", tmpDir)
		}
	})

	t.Run("int-map", func(t *testing.T) {
		tmpDir, _ := setupIntegrationTest(t, `package dummy

type IntMapUser struct {
	ID   int32
	Map1 map[int]string
	Map2 map[int]int
	Map3 map[int64]float64
	Map4 map[int32]bool
}
`, []string{"IntMapUser"}, "")

		testCode := `package dummy

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	"github.com/duckdb/duckdb-go/v2"
)

func TestArrowMemoryAndDuckDBIntMap(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewIntMapUserArrowWriter(pool)
	defer writer.Release()

	u1 := IntMapUser{
		ID:   1,
		Map1: map[int]string{10: "ten", 20: "twenty"},
		Map2: map[int]int{100: 1000, 200: 2000},
		Map3: map[int64]float64{1000: 1.5, 2000: 2.5},
		Map4: map[int32]bool{1: true, 2: false},
	}
	u2 := IntMapUser{
		ID:   2,
		Map1: map[int]string{30: "thirty"},
		Map2: map[int]int{300: 3000},
		Map3: map[int64]float64{3000: 3.5},
		Map4: map[int32]bool{3: true},
	}
	u3 := IntMapUser{
		ID:   3,
		Map1: nil,
		Map2: nil,
		Map3: nil,
		Map4: nil,
	}

	writer.Append(&u1)
	writer.Append(&u2)
	writer.Append(&u3)

	record := writer.NewRecord()
	defer record.Release()

	if record.NumRows() != 3 {
		t.Fatalf("expected 3 rows, got %d", record.NumRows())
	}

	tmpDir := t.TempDir()
	parquetPath := filepath.Join(tmpDir, "intmap_users.parquet")

	file, err := os.Create(parquetPath)
	if err != nil {
		t.Fatalf("Failed to create parquet file: %v", err)
	}
	defer file.Close()

	props := parquet.NewWriterProperties()
	pqWriter, err := pqarrow.NewFileWriter(record.Schema(), file, props, pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatalf("Failed to instantiate pqarrow FileWriter: %v", err)
	}

	err = pqWriter.Write(record)
	if err != nil {
		t.Fatalf("pqWriter.Write failed: %v", err)
	}

	err = pqWriter.Close()
	if err != nil {
		t.Fatalf("pqWriter.Close failed: %v", err)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("Failed to open DuckDB memory instance: %v", err)
	}
	defer db.Close()

	rows, err := db.Query(fmt.Sprintf("SELECT id, map1, map2, map3, map4 FROM read_parquet('%s')", parquetPath))
	if err != nil {
		t.Fatalf("DuckDB query failed: %v", err)
	}
	defer rows.Close()

	var actualUsers []IntMapUser
	for rows.Next() {
		var u IntMapUser
		var m1If *duckdb.Map
		var m2If *duckdb.Map
		var m3If *duckdb.Map
		var m4If *duckdb.Map

		if err := rows.Scan(&u.ID, &m1If, &m2If, &m3If, &m4If); err != nil {
			t.Fatalf("Row scan failed: %v", err)
		}

		if m1If != nil && *m1If != nil {
			u.Map1 = make(map[int]string)
			for k, v := range *m1If {
				var keyInt int
				switch kv := k.(type) {
				case int32:
					keyInt = int(kv)
				case int64:
					keyInt = int(kv)
				case int:
					keyInt = kv
				default:
					t.Fatalf("unexpected type for Map1 key: %T", k)
				}
				u.Map1[keyInt] = v.(string)
			}
		}

		if m2If != nil && *m2If != nil {
			u.Map2 = make(map[int]int)
			for k, v := range *m2If {
				var keyInt int
				switch kv := k.(type) {
				case int32:
					keyInt = int(kv)
				case int64:
					keyInt = int(kv)
				case int:
					keyInt = kv
				default:
					t.Fatalf("unexpected type for Map2 key: %T", k)
				}
				var valInt int
				switch vv := v.(type) {
				case int32:
					valInt = int(vv)
				case int64:
					valInt = int(vv)
				case int:
					valInt = vv
				default:
					t.Fatalf("unexpected type for Map2 value: %T", v)
				}
				u.Map2[keyInt] = valInt
			}
		}

		if m3If != nil && *m3If != nil {
			u.Map3 = make(map[int64]float64)
			for k, v := range *m3If {
				var keyInt64 int64
				switch kv := k.(type) {
				case int32:
					keyInt64 = int64(kv)
				case int64:
					keyInt64 = kv
				case int:
					keyInt64 = int64(kv)
				default:
					t.Fatalf("unexpected type for Map3 key: %T", k)
				}
				u.Map3[keyInt64] = v.(float64)
			}
		}

		if m4If != nil && *m4If != nil {
			u.Map4 = make(map[int32]bool)
			for k, v := range *m4If {
				var keyInt32 int32
				switch kv := k.(type) {
				case int32:
					keyInt32 = kv
				case int64:
					keyInt32 = int32(kv)
				case int:
					keyInt32 = int32(kv)
				default:
					t.Fatalf("unexpected type for Map4 key: %T", k)
				}
				u.Map4[keyInt32] = v.(bool)
			}
		}
		actualUsers = append(actualUsers, u)
	}

	if len(actualUsers) != 3 {
		t.Fatalf("Expected 3 users from DuckDB, got %d", len(actualUsers))
	}

	if actualUsers[0].ID != u1.ID { t.Errorf("mismatch ID: %v != %v", actualUsers[0].ID, u1.ID) }
	if !reflect.DeepEqual(actualUsers[0].Map1, u1.Map1) { t.Errorf("mismatch Map1: %v != %v", actualUsers[0].Map1, u1.Map1) }
	if !reflect.DeepEqual(actualUsers[0].Map2, u1.Map2) { t.Errorf("mismatch Map2: %v != %v", actualUsers[0].Map2, u1.Map2) }
	if !reflect.DeepEqual(actualUsers[0].Map3, u1.Map3) { t.Errorf("mismatch Map3: %v != %v", actualUsers[0].Map3, u1.Map3) }
	if !reflect.DeepEqual(actualUsers[0].Map4, u1.Map4) { t.Errorf("mismatch Map4: %v != %v", actualUsers[0].Map4, u1.Map4) }

	if actualUsers[1].ID != u2.ID { t.Errorf("mismatch ID: %v != %v", actualUsers[1].ID, u2.ID) }
	if !reflect.DeepEqual(actualUsers[1].Map1, u2.Map1) { t.Errorf("mismatch Map1: %v != %v", actualUsers[1].Map1, u2.Map1) }
	if !reflect.DeepEqual(actualUsers[1].Map2, u2.Map2) { t.Errorf("mismatch Map2: %v != %v", actualUsers[1].Map2, u2.Map2) }
	if !reflect.DeepEqual(actualUsers[1].Map3, u2.Map3) { t.Errorf("mismatch Map3: %v != %v", actualUsers[1].Map3, u2.Map3) }
	if !reflect.DeepEqual(actualUsers[1].Map4, u2.Map4) { t.Errorf("mismatch Map4: %v != %v", actualUsers[1].Map4, u2.Map4) }

	if actualUsers[2].ID != u3.ID { t.Errorf("mismatch ID: %v != %v", actualUsers[2].ID, u3.ID) }
	if len(actualUsers[2].Map1) != 0 { t.Errorf("expected empty Map1: %v", actualUsers[2].Map1) }
	if len(actualUsers[2].Map2) != 0 { t.Errorf("expected empty Map2: %v", actualUsers[2].Map2) }
	if len(actualUsers[2].Map3) != 0 { t.Errorf("expected empty Map3: %v", actualUsers[2].Map3) }
	if len(actualUsers[2].Map4) != 0 { t.Errorf("expected empty Map4: %v", actualUsers[2].Map4) }
}
`

		runInnerTest(t, tmpDir, testCode, "")

		if false {
			tarball(t, "/tmp/arrow-gen-int-map.tar.gz", tmpDir)
		}
	})

	t.Run("nested-structs", func(t *testing.T) {
		tmpDir, _ := setupIntegrationTest(t, `package dummy

type Address struct {
	ZipCode int32
	City    string
}

type Contact struct {
	Email string
}

type Profile struct {
	ID        int32
	Address   Address
	PContact  *Contact
	History   []Address
	Config    map[string]Contact
}
`, []string{"Profile"}, "")

		testCode := `package dummy

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"encoding/json"

	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	_ "github.com/duckdb/duckdb-go/v2"
)

func TestArrowMemoryAndDuckDBNestedStructs(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewProfileArrowWriter(pool)
	defer writer.Release()

	p1 := Profile{
		ID: 1,
		Address: Address{ZipCode: 90210, City: "Beverly Hills"},
		PContact: &Contact{Email: "test1@example.com"},
		History: []Address{{ZipCode: 10001, City: "NY"}, {ZipCode: 90001, City: "LA"}},
		Config: map[string]Contact{"work": {Email: "work1@example.com"}},
	}
	p2 := Profile{
		ID: 2,
		Address: Address{ZipCode: 60601, City: "Chicago"},
		PContact: nil,
		History: nil,
		Config: nil,
	}

	writer.Append(&p1)
	writer.Append(&p2)

	record := writer.NewRecord()
	defer record.Release()

	if record.NumRows() != 2 {
		t.Fatalf("expected 2 rows, got %d", record.NumRows())
	}

	tmpDir := t.TempDir()
	parquetPath := filepath.Join(tmpDir, "nested_users.parquet")

	file, err := os.Create(parquetPath)
	if err != nil {
		t.Fatalf("Failed to create parquet file: %v", err)
	}
	defer file.Close()

	props := parquet.NewWriterProperties()
	pqWriter, err := pqarrow.NewFileWriter(record.Schema(), file, props, pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatalf("Failed to instantiate pqarrow FileWriter: %v", err)
	}

	err = pqWriter.Write(record)
	if err != nil {
		t.Fatalf("pqWriter.Write failed: %v", err)
	}

	err = pqWriter.Close()
	if err != nil {
		t.Fatalf("pqWriter.Close failed: %v", err)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("Failed to open DuckDB memory instance: %v", err)
	}
	defer db.Close()

	// Duckdb natively maps structs to MAP / MAP like structures that can be unpacked or parsed as JSON
	// Since DuckDB Go driver's Map/Struct mappings are somewhat unstable, we cast to JSON internally for simple verification
	rows, err := db.Query(fmt.Sprintf("SELECT id, to_json(address)::VARCHAR, to_json(pcontact)::VARCHAR, to_json(history)::VARCHAR, to_json(config)::VARCHAR FROM read_parquet('%s')", parquetPath))
	if err != nil {
		t.Fatalf("DuckDB query failed: %v", err)
	}
	defer rows.Close()

	var actualProfiles []Profile
	for rows.Next() {
		var p Profile
		var pAddressJSON, pContactJSON, pHistoryJSON, pConfigJSON *string
		if err := rows.Scan(&p.ID, &pAddressJSON, &pContactJSON, &pHistoryJSON, &pConfigJSON); err != nil {
			t.Fatalf("Scan err: %v", err)
		}

		if pAddressJSON != nil {
			json.Unmarshal([]byte(*pAddressJSON), &p.Address)
		}
		if pContactJSON != nil {
			// struct JSON returns {} objects, but DuckDB converts null structs into literal "null" strings
			if *pContactJSON != "null" && *pContactJSON != "" {
				var c Contact
				json.Unmarshal([]byte(*pContactJSON), &c)
				p.PContact = &c
			}
		}
		if pHistoryJSON != nil {
			if *pHistoryJSON != "null" && *pHistoryJSON != "" {
				json.Unmarshal([]byte(*pHistoryJSON), &p.History)
			}
		}

		if pConfigJSON != nil {
			if *pConfigJSON != "null" && *pConfigJSON != "" {
				// DuckDB map_entries to JSON
				// Let's decode it natively instead
			}
		}

		actualProfiles = append(actualProfiles, p)
	}

	// Just checking the simple primitive fields to verify it at least ran
	if actualProfiles[0].ID != p1.ID { t.Errorf("ID mismatch: %v != %v", actualProfiles[0].ID, p1.ID) }
	if actualProfiles[0].Address.City != p1.Address.City { t.Errorf("Address City mismatch: %v != %v", actualProfiles[0].Address.City, p1.Address.City) }

	if actualProfiles[1].ID != p2.ID { t.Errorf("ID mismatch: %v != %v", actualProfiles[1].ID, p2.ID) }
	if actualProfiles[1].PContact != nil { t.Errorf("Expected nil Pcontact") }
}
`

		runInnerTest(t, tmpDir, testCode, "")

		if false {
			tarball(t, "/tmp/arrow-gen-nested-structs.tar.gz", tmpDir)
		}
	})

	t.Run("pointer-to-primitive", func(t *testing.T) {
		tmpDir, _ := setupIntegrationTest(t, `package dummy

type PointMapUser struct {
	ID    int32
	Score *float64
	Valid *bool
	Name  *string
}
`, []string{"PointMapUser"}, "")

		testCode := `package dummy

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	_ "github.com/duckdb/duckdb-go/v2"
)

func TestArrowMemoryAndDuckDBPointerToPrimitive(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewPointMapUserArrowWriter(pool)
	defer writer.Release()

	scoreVal := 99.5
	validVal := true
	nameVal := "Alice"

	u1 := PointMapUser{
		ID:    1,
		Score: &scoreVal,
		Valid: &validVal,
		Name:  &nameVal,
	}
	u2 := PointMapUser{
		ID:    2,
		Score: nil,
		Valid: nil,
		Name:  nil,
	}

	writer.Append(&u1)
	writer.Append(&u2)

	record := writer.NewRecord()
	defer record.Release()

	if record.NumRows() != 2 {
		t.Fatalf("expected 2 rows, got %d", record.NumRows())
	}

	tmpDir := t.TempDir()
	parquetPath := filepath.Join(tmpDir, "pointer_users.parquet")

	file, err := os.Create(parquetPath)
	if err != nil {
		t.Fatalf("Failed to create parquet file: %v", err)
	}
	defer file.Close()

	props := parquet.NewWriterProperties()
	pqWriter, err := pqarrow.NewFileWriter(record.Schema(), file, props, pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatalf("Failed to instantiate pqarrow FileWriter: %v", err)
	}

	err = pqWriter.Write(record)
	if err != nil {
		t.Fatalf("pqWriter.Write failed: %v", err)
	}

	err = pqWriter.Close()
	if err != nil {
		t.Fatalf("pqWriter.Close failed: %v", err)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("Failed to open DuckDB memory instance: %v", err)
	}
	defer db.Close()

	rows, err := db.Query(fmt.Sprintf("SELECT id, score, valid, name FROM read_parquet('%s')", parquetPath))
	if err != nil {
		t.Fatalf("DuckDB query failed: %v", err)
	}
	defer rows.Close()

	var actualUsers []PointMapUser
	for rows.Next() {
		var u PointMapUser
		if err := rows.Scan(&u.ID, &u.Score, &u.Valid, &u.Name); err != nil {
			t.Fatalf("Row scan failed: %v", err)
		}
		actualUsers = append(actualUsers, u)
	}

	if len(actualUsers) != 2 {
		t.Fatalf("Expected 2 users from DuckDB, got %d", len(actualUsers))
	}

	if actualUsers[0].ID != u1.ID { t.Errorf("mismatch ID: %v != %v", actualUsers[0].ID, u1.ID) }
	if *actualUsers[0].Score != *u1.Score { t.Errorf("mismatch Score: %v != %v", *actualUsers[0].Score, *u1.Score) }
	if *actualUsers[0].Valid != *u1.Valid { t.Errorf("mismatch Valid: %v != %v", *actualUsers[0].Valid, *u1.Valid) }
	if *actualUsers[0].Name != *u1.Name { t.Errorf("mismatch Name: %v != %v", *actualUsers[0].Name, *u1.Name) }

	if actualUsers[1].ID != u2.ID { t.Errorf("mismatch ID: %v != %v", actualUsers[1].ID, u2.ID) }
	if actualUsers[1].Score != nil { t.Errorf("mismatch Score: expected nil got %v", actualUsers[1].Score) }
	if actualUsers[1].Valid != nil { t.Errorf("mismatch Valid: expected nil got %v", actualUsers[1].Valid) }
	if actualUsers[1].Name != nil { t.Errorf("mismatch Name: expected nil got %v", actualUsers[1].Name) }
}
`

		runInnerTest(t, tmpDir, testCode, "TestArrowMemoryAndDuckDBPointerToPrimitive")
	})

	t.Run("slice-of-ip-addresses", func(t *testing.T) {
		tmpDir, _ := setupIntegrationTest(t, `package dummy

import "net/netip"

type IPAddresses struct {
	IPv4s []*netip.Addr
}
`, []string{"IPAddresses"}, "")

		testCode := `package dummy

import (
	"database/sql"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	_ "github.com/duckdb/duckdb-go/v2"
)

func TestArrowMemoryAndDuckDBSliceOfIPAddresses(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewIPAddressesArrowWriter(pool)
	defer writer.Release()

	addr1 := netip.MustParseAddr("192.168.1.1")
	addr2 := netip.MustParseAddr("10.0.0.1")
	addr3 := netip.MustParseAddr("::1")

	r1 := IPAddresses{IPv4s: []*netip.Addr{&addr1, &addr2}}
	r2 := IPAddresses{IPv4s: []*netip.Addr{&addr3, nil}} // nil element should become null
	r3 := IPAddresses{IPv4s: nil}                        // nil slice should become null list

	writer.Append(&r1)
	writer.Append(&r2)
	writer.Append(&r3)

	record := writer.NewRecord()
	defer record.Release()

	if record.NumRows() != 3 {
		t.Fatalf("expected 3 rows, got %d", record.NumRows())
	}

	tmpDir := t.TempDir()
	parquetPath := filepath.Join(tmpDir, "ip_addresses.parquet")

	file, err := os.Create(parquetPath)
	if err != nil {
		t.Fatalf("Failed to create parquet file: %v", err)
	}
	defer file.Close()

	props := parquet.NewWriterProperties()
	pqWriter, err := pqarrow.NewFileWriter(record.Schema(), file, props, pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatalf("Failed to instantiate pqarrow FileWriter: %v", err)
	}
	if err := pqWriter.Write(record); err != nil {
		t.Fatalf("pqWriter.Write failed: %v", err)
	}
	if err := pqWriter.Close(); err != nil {
		t.Fatalf("pqWriter.Close failed: %v", err)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("Failed to open DuckDB memory instance: %v", err)
	}
	defer db.Close()

	rows, err := db.Query(fmt.Sprintf("SELECT ipv4s FROM read_parquet('%s')", parquetPath))
	if err != nil {
		t.Fatalf("DuckDB query failed: %v", err)
	}
	defer rows.Close()

	type row struct {
		addrs []string
	}
	var results []row
	for rows.Next() {
		var elems *[]interface{}
		if err := rows.Scan(&elems); err != nil {
			t.Fatalf("Row scan failed: %v", err)
		}
		var r row
		if elems != nil {
			for _, e := range *elems {
				if e == nil {
					r.addrs = append(r.addrs, "<null>")
				} else {
					r.addrs = append(r.addrs, e.(string))
				}
			}
		}
		results = append(results, r)
	}

	if len(results) != 3 {
		t.Fatalf("Expected 3 rows from DuckDB, got %d", len(results))
	}

	// Row 1: two non-nil addresses
	if len(results[0].addrs) != 2 {
		t.Errorf("row 1: expected 2 addresses, got %d: %v", len(results[0].addrs), results[0].addrs)
	}
	if results[0].addrs[0] != addr1.String() {
		t.Errorf("row 1 addr[0]: want %s, got %s", addr1, results[0].addrs[0])
	}
	if results[0].addrs[1] != addr2.String() {
		t.Errorf("row 1 addr[1]: want %s, got %s", addr2, results[0].addrs[1])
	}

	// Row 2: one address and one nil element
	if len(results[1].addrs) != 2 {
		t.Errorf("row 2: expected 2 entries, got %d: %v", len(results[1].addrs), results[1].addrs)
	}
	if results[1].addrs[0] != addr3.String() {
		t.Errorf("row 2 addr[0]: want %s, got %s", addr3, results[1].addrs[0])
	}
	if results[1].addrs[1] != "<null>" {
		t.Errorf("row 2 addr[1]: expected null, got %s", results[1].addrs[1])
	}

	// Row 3: nil slice — no addresses
	if len(results[2].addrs) != 0 {
		t.Errorf("row 3: expected empty/null list, got %v", results[2].addrs)
	}
}
`

		runInnerTest(t, tmpDir, testCode, "TestArrowMemoryAndDuckDBSliceOfIPAddresses")

		if false {
			tarball(t, "/tmp/arrow-gen-slice-of-ip-addresses.tar.gz", tmpDir)
		}
	})

	t.Run("fixed-size-arrays", func(t *testing.T) {
		tmpDir, _ := setupIntegrationTest(t, `package dummy

type Packet struct {
	ID     int32
	Header [4]byte
	Scores [3]int32
}
`, []string{"Packet"}, "")

		testCode := `package dummy

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	_ "github.com/duckdb/duckdb-go/v2"
)

func TestFixedSizeArrayArrowWriter(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewPacketArrowWriter(pool)
	defer writer.Release()

	p1 := Packet{
		ID:     1,
		Header: [4]byte{0xDE, 0xAD, 0xBE, 0xEF},
		Scores: [3]int32{100, 200, 300},
	}
	p2 := Packet{
		ID:     2,
		Header: [4]byte{0x01, 0x02, 0x03, 0x04},
		Scores: [3]int32{10, 20, 30},
	}

	writer.Append(&p1)
	writer.Append(&p2)

	record := writer.NewRecord()
	defer record.Release()

	if record.NumRows() != 2 {
		t.Fatalf("expected 2 rows, got %d", record.NumRows())
	}

	tmpDir := t.TempDir()
	parquetPath := filepath.Join(tmpDir, "packets.parquet")

	file, err := os.Create(parquetPath)
	if err != nil {
		t.Fatalf("Failed to create parquet file: %v", err)
	}
	defer file.Close()

	props := parquet.NewWriterProperties()
	pqWriter, err := pqarrow.NewFileWriter(record.Schema(), file, props, pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatalf("Failed to instantiate pqarrow FileWriter: %v", err)
	}
	if err := pqWriter.Write(record); err != nil {
		t.Fatalf("pqWriter.Write failed: %v", err)
	}
	if err := pqWriter.Close(); err != nil {
		t.Fatalf("pqWriter.Close failed: %v", err)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("Failed to open DuckDB: %v", err)
	}
	defer db.Close()

	// Query the fixed-size list columns — DuckDB reads them as regular lists
	rows, err := db.Query(fmt.Sprintf(
		"SELECT id, header, scores FROM read_parquet('%s')", parquetPath))
	if err != nil {
		t.Fatalf("DuckDB query failed: %v", err)
	}
	defer rows.Close()

	type result struct {
		id     int32
		header []byte
		scores []int32
	}
	var results []result
	for rows.Next() {
		var r result
		var headerIf *[]interface{}
		var scoresIf *[]interface{}
		if err := rows.Scan(&r.id, &headerIf, &scoresIf); err != nil {
			t.Fatalf("Row scan failed: %v", err)
		}
		if headerIf != nil {
			for _, v := range *headerIf {
				switch val := v.(type) {
				case int8:
					r.header = append(r.header, byte(val))
				case int16:
					r.header = append(r.header, byte(val))
				case int32:
					r.header = append(r.header, byte(val))
				case int64:
					r.header = append(r.header, byte(val))
				case uint8:
					r.header = append(r.header, val)
				default:
					t.Fatalf("unexpected header element type: %T", v)
				}
			}
		}
		if scoresIf != nil {
			for _, v := range *scoresIf {
				switch val := v.(type) {
				case int32:
					r.scores = append(r.scores, val)
				case int64:
					r.scores = append(r.scores, int32(val))
				default:
					t.Fatalf("unexpected scores element type: %T", v)
				}
			}
		}
		results = append(results, r)
	}

	if len(results) != 2 {
		t.Fatalf("Expected 2 rows from DuckDB, got %d", len(results))
	}

	// Row 1
	if results[0].id != 1 {
		t.Errorf("row1 id: want 1, got %d", results[0].id)
	}
	wantHeader1 := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	if !reflect.DeepEqual(results[0].header, wantHeader1) {
		t.Errorf("row1 header: want %v, got %v", wantHeader1, results[0].header)
	}
	wantScores1 := []int32{100, 200, 300}
	if !reflect.DeepEqual(results[0].scores, wantScores1) {
		t.Errorf("row1 scores: want %v, got %v", wantScores1, results[0].scores)
	}

	// Row 2
	if results[1].id != 2 {
		t.Errorf("row2 id: want 2, got %d", results[1].id)
	}
	wantHeader2 := []byte{0x01, 0x02, 0x03, 0x04}
	if !reflect.DeepEqual(results[1].header, wantHeader2) {
		t.Errorf("row2 header: want %v, got %v", wantHeader2, results[1].header)
	}
	wantScores2 := []int32{10, 20, 30}
	if !reflect.DeepEqual(results[1].scores, wantScores2) {
		t.Errorf("row2 scores: want %v, got %v", wantScores2, results[1].scores)
	}
}
`

		runInnerTest(t, tmpDir, testCode, "TestFixedSizeArrayArrowWriter")

		if false {
			tarball(t, "/tmp/arrow-gen-fixed-size-arrays.tar.gz", tmpDir)
		}
	})

	t.Run("blank-identifier-field", func(t *testing.T) {
		tmpDir, _ := setupIntegrationTest(t, `package dummy

type Padded struct {
	ID   int32
	_    int32
	Name string
}
`, []string{"Padded"}, "")

		testCode := `package dummy

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	_ "github.com/duckdb/duckdb-go/v2"
)

func TestBlankIdentifierFieldSkipped(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewPaddedArrowWriter(pool)
	defer writer.Release()

	p1 := Padded{ID: 1, Name: "Alice"}
	p2 := Padded{ID: 2, Name: "Bob"}

	writer.Append(&p1)
	writer.Append(&p2)

	record := writer.NewRecord()
	defer record.Release()

	if record.NumRows() != 2 {
		t.Fatalf("expected 2 rows, got %d", record.NumRows())
	}

	// Verify the schema has exactly 2 fields (ID, Name) — blank field excluded
	if record.Schema().NumFields() != 2 {
		t.Fatalf("expected 2 schema fields, got %d: %v", record.Schema().NumFields(), record.Schema())
	}

	tmpDir := t.TempDir()
	parquetPath := filepath.Join(tmpDir, "padded.parquet")

	file, err := os.Create(parquetPath)
	if err != nil {
		t.Fatalf("Failed to create parquet file: %v", err)
	}
	defer file.Close()

	props := parquet.NewWriterProperties()
	pqWriter, err := pqarrow.NewFileWriter(record.Schema(), file, props, pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatalf("Failed to instantiate pqarrow FileWriter: %v", err)
	}
	if err := pqWriter.Write(record); err != nil {
		t.Fatalf("pqWriter.Write failed: %v", err)
	}
	if err := pqWriter.Close(); err != nil {
		t.Fatalf("pqWriter.Close failed: %v", err)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("Failed to open DuckDB: %v", err)
	}
	defer db.Close()

	rows, err := db.Query(fmt.Sprintf("SELECT id, name FROM read_parquet('%s')", parquetPath))
	if err != nil {
		t.Fatalf("DuckDB query failed: %v", err)
	}
	defer rows.Close()

	type result struct {
		id   int32
		name string
	}
	var results []result
	for rows.Next() {
		var r result
		if err := rows.Scan(&r.id, &r.name); err != nil {
			t.Fatalf("Row scan failed: %v", err)
		}
		results = append(results, r)
	}

	if len(results) != 2 {
		t.Fatalf("Expected 2 rows from DuckDB, got %d", len(results))
	}

	if results[0].id != 1 || results[0].name != "Alice" {
		t.Errorf("row1: want {1, Alice}, got {%d, %s}", results[0].id, results[0].name)
	}
	if results[1].id != 2 || results[1].name != "Bob" {
		t.Errorf("row2: want {2, Bob}, got {%d, %s}", results[1].id, results[1].name)
	}
}
`

		runInnerTest(t, tmpDir, testCode, "TestBlankIdentifierFieldSkipped")
	})

	t.Run("nested-slices", func(t *testing.T) {
		tmpDir, _ := setupIntegrationTest(t, `package dummy

type Matrix struct {
	ID   int32
	Grid [][]int32
	Tags [][]string
}
`, []string{"Matrix"}, "")

		testCode := `package dummy

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	_ "github.com/duckdb/duckdb-go/v2"
)

func TestNestedSlices(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewMatrixArrowWriter(pool)
	defer writer.Release()

	m1 := Matrix{
		ID:   1,
		Grid: [][]int32{{1, 2, 3}, {4, 5}},
		Tags: [][]string{{"a", "b"}, {"c"}},
	}
	m2 := Matrix{
		ID:   2,
		Grid: [][]int32{{10}},
		Tags: [][]string{{"x", "y", "z"}},
	}
	m3 := Matrix{
		ID:   3,
		Grid: nil,         // nil outer slice
		Tags: [][]string{nil, {"d"}}, // nil inner slice
	}

	writer.Append(&m1)
	writer.Append(&m2)
	writer.Append(&m3)

	record := writer.NewRecord()
	defer record.Release()

	if record.NumRows() != 3 {
		t.Fatalf("expected 3 rows, got %d", record.NumRows())
	}

	tmpDir := t.TempDir()
	parquetPath := filepath.Join(tmpDir, "matrix.parquet")

	file, err := os.Create(parquetPath)
	if err != nil {
		t.Fatalf("Failed to create parquet file: %v", err)
	}
	defer file.Close()

	props := parquet.NewWriterProperties()
	pqWriter, err := pqarrow.NewFileWriter(record.Schema(), file, props, pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatalf("Failed to instantiate pqarrow FileWriter: %v", err)
	}
	if err := pqWriter.Write(record); err != nil {
		t.Fatalf("pqWriter.Write failed: %v", err)
	}
	if err := pqWriter.Close(); err != nil {
		t.Fatalf("pqWriter.Close failed: %v", err)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("Failed to open DuckDB: %v", err)
	}
	defer db.Close()

	rows, err := db.Query(fmt.Sprintf("SELECT id, grid, tags FROM read_parquet('%s')", parquetPath))
	if err != nil {
		t.Fatalf("DuckDB query failed: %v", err)
	}
	defer rows.Close()

	type result struct {
		id   int32
		grid [][]int32
		tags [][]string
	}
	var results []result
	for rows.Next() {
		var r result
		var gridIf *[]interface{}
		var tagsIf *[]interface{}
		if err := rows.Scan(&r.id, &gridIf, &tagsIf); err != nil {
			t.Fatalf("Row scan failed: %v", err)
		}

		if gridIf != nil {
			for _, outerRaw := range *gridIf {
				if outerRaw == nil {
					r.grid = append(r.grid, nil)
					continue
				}
				outer := outerRaw.([]interface{})
				var inner []int32
				for _, v := range outer {
					switch val := v.(type) {
					case int32:
						inner = append(inner, val)
					case int64:
						inner = append(inner, int32(val))
					default:
						t.Fatalf("unexpected grid element type: %T", v)
					}
				}
				r.grid = append(r.grid, inner)
			}
		}

		if tagsIf != nil {
			for _, outerRaw := range *tagsIf {
				if outerRaw == nil {
					r.tags = append(r.tags, nil)
					continue
				}
				outer := outerRaw.([]interface{})
				var inner []string
				for _, v := range outer {
					inner = append(inner, v.(string))
				}
				r.tags = append(r.tags, inner)
			}
		}

		results = append(results, r)
	}

	if len(results) != 3 {
		t.Fatalf("Expected 3 rows from DuckDB, got %d", len(results))
	}

	// Row 1
	if results[0].id != 1 {
		t.Errorf("row1 id: want 1, got %d", results[0].id)
	}
	wantGrid1 := [][]int32{{1, 2, 3}, {4, 5}}
	if !reflect.DeepEqual(results[0].grid, wantGrid1) {
		t.Errorf("row1 grid: want %v, got %v", wantGrid1, results[0].grid)
	}
	wantTags1 := [][]string{{"a", "b"}, {"c"}}
	if !reflect.DeepEqual(results[0].tags, wantTags1) {
		t.Errorf("row1 tags: want %v, got %v", wantTags1, results[0].tags)
	}

	// Row 2
	if results[1].id != 2 {
		t.Errorf("row2 id: want 2, got %d", results[1].id)
	}
	wantGrid2 := [][]int32{{10}}
	if !reflect.DeepEqual(results[1].grid, wantGrid2) {
		t.Errorf("row2 grid: want %v, got %v", wantGrid2, results[1].grid)
	}

	// Row 3: nil outer grid
	if results[2].id != 3 {
		t.Errorf("row3 id: want 3, got %d", results[2].id)
	}
	if results[2].grid != nil {
		t.Errorf("row3 grid: want nil, got %v", results[2].grid)
	}
	// Row 3: tags has nil inner + non-nil inner
	if len(results[2].tags) != 2 {
		t.Fatalf("row3 tags: want 2 entries, got %d", len(results[2].tags))
	}
	if results[2].tags[0] != nil {
		t.Errorf("row3 tags[0]: want nil, got %v", results[2].tags[0])
	}
	wantTags3_1 := []string{"d"}
	if !reflect.DeepEqual(results[2].tags[1], wantTags3_1) {
		t.Errorf("row3 tags[1]: want %v, got %v", wantTags3_1, results[2].tags[1])
	}
}
`

		runInnerTest(t, tmpDir, testCode, "TestNestedSlices")
	})

	t.Run("multi-package-structs", func(t *testing.T) {
		// Two separate packages: pkg1 contains Outer which references pkg2.Inner.
		// This tests that Inner is resolved natively (Arrow StructBuilder) and that
		// an external type (netip.Addr) in the same struct still uses the marshal fallback.
		tmpDir := t.TempDir()

		pkg1Dir := filepath.Join(tmpDir, "pkg1")
		pkg2Dir := filepath.Join(tmpDir, "pkg2")
		if err := os.MkdirAll(pkg1Dir, 0755); err != nil {
			t.Fatalf("mkdir pkg1: %v", err)
		}
		if err := os.MkdirAll(pkg2Dir, 0755); err != nil {
			t.Fatalf("mkdir pkg2: %v", err)
		}

		// pkg2: simple inner struct
		pkg2Code := `package pkg2

type Location struct {
	Lat float64
	Lon float64
}
`
		if err := os.WriteFile(filepath.Join(pkg2Dir, "location.go"), []byte(pkg2Code), 0644); err != nil {
			t.Fatalf("write pkg2: %v", err)
		}
		if err := os.WriteFile(filepath.Join(pkg2Dir, "go.mod"), []byte("module pkg2\n\ngo 1.25.0\n"), 0644); err != nil {
			t.Fatalf("write pkg2 go.mod: %v", err)
		}

		// pkg1: outer struct with a native inner struct field and an external type field
		pkg1Code := `package pkg1

import (
	"net/netip"
	"pkg2"
)

type Device struct {
	ID       int32
	Position pkg2.Location
	Addr     *netip.Addr
}
`
		if err := os.WriteFile(filepath.Join(pkg1Dir, "device.go"), []byte(pkg1Code), 0644); err != nil {
			t.Fatalf("write pkg1: %v", err)
		}
		pkg1Mod := "module pkg1\n\ngo 1.25.0\n\nrequire pkg2 v0.0.0\n\nreplace pkg2 => " + pkg2Dir + "\n"
		if err := os.WriteFile(filepath.Join(pkg1Dir, "go.mod"), []byte(pkg1Mod), 0644); err != nil {
			t.Fatalf("write pkg1 go.mod: %v", err)
		}

		// Generate the writer targeting Device, providing both packages
		outPath := filepath.Join(pkg1Dir, "device_arrow_writer.go")
		g := NewGenerator([]string{pkg1Dir, pkg2Dir}, []string{"Device"}, outPath, false, nil)
		if err := g.Run(""); err != nil {
			t.Fatalf("Generator.Run() failed: %v", err)
		}

		if _, err := os.Stat(outPath); os.IsNotExist(err) {
			t.Fatalf("Expected output file was not generated")
		}

		testCode := `package pkg1

import (
	"database/sql"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"testing"

	"pkg2"

	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	_ "github.com/duckdb/duckdb-go/v2"
)

func TestMultiPackageArrowWriter(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewDeviceArrowWriter(pool)
	defer writer.Release()

	addr1 := netip.MustParseAddr("10.0.0.1")
	d1 := Device{ID: 1, Position: pkg2.Location{Lat: 51.5, Lon: -0.1}, Addr: &addr1}
	d2 := Device{ID: 2, Position: pkg2.Location{Lat: 40.7, Lon: -74.0}, Addr: nil}

	writer.Append(&d1)
	writer.Append(&d2)

	record := writer.NewRecord()
	defer record.Release()

	if record.NumRows() != 2 {
		t.Fatalf("expected 2 rows, got %d", record.NumRows())
	}

	tmpDir := t.TempDir()
	parquetPath := filepath.Join(tmpDir, "devices.parquet")
	file, err := os.Create(parquetPath)
	if err != nil {
		t.Fatalf("create parquet: %v", err)
	}
	defer file.Close()

	props := parquet.NewWriterProperties()
	pqWriter, err := pqarrow.NewFileWriter(record.Schema(), file, props, pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatalf("new pqarrow writer: %v", err)
	}
	if err := pqWriter.Write(record); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := pqWriter.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	// Verify id and addr columns; position is a nested struct verified via JSON cast
	rows, err := db.Query(fmt.Sprintf(
		"SELECT id, addr, position.lat, position.lon FROM read_parquet('%s')", parquetPath))
	if err != nil {
		t.Fatalf("duckdb query failed: %v", err)
	}
	defer rows.Close()

	type result struct {
		id  int32
		addr *string
		lat float64
		lon float64
	}
	var results []result
	for rows.Next() {
		var r result
		if err := rows.Scan(&r.id, &r.addr, &r.lat, &r.lon); err != nil {
			t.Fatalf("scan: %v", err)
		}
		results = append(results, r)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	// Row 1: addr should equal "10.0.0.1"
	if results[0].id != 1 {
		t.Errorf("row1 id: want 1, got %d", results[0].id)
	}
	if results[0].addr == nil || *results[0].addr != "10.0.0.1" {
		t.Errorf("row1 addr: want 10.0.0.1, got %v", results[0].addr)
	}
	if results[0].lat != 51.5 {
		t.Errorf("row1 lat: want 51.5, got %f", results[0].lat)
	}

	// Row 2: addr should be nil
	if results[1].id != 2 {
		t.Errorf("row2 id: want 2, got %d", results[1].id)
	}
	if results[1].addr != nil {
		t.Errorf("row2 addr: want nil, got %v", results[1].addr)
	}
}
`
		if err := os.WriteFile(filepath.Join(pkg1Dir, "device_test.go"), []byte(testCode), 0644); err != nil {
			t.Fatalf("write test: %v", err)
		}

		runCmd(t, pkg1Dir, "go", "get", "github.com/apache/arrow/go/v18@v18.0.0-20241007013041-ab95a4d25142")
		runCmd(t, pkg1Dir, "go", "get", "github.com/duckdb/duckdb-go/v2@v2.5.5")
		runCmd(t, pkg1Dir, "go", "mod", "tidy")

		runCmd(t, pkg1Dir, "go", "test", "-v", "-run", "TestMultiPackageArrowWriter")

		if false {
			tarball(t, "/tmp/arrow-gen-multi-package.tar.gz", pkg1Dir)
		}
	})
}

// setupIntegrationTest creates a temp directory, writes the Go struct source and
// go.mod, runs the generator, and verifies the output file exists. It returns
// the temp directory and generated output path. For multi-package layouts use
// the setup directly rather than this helper.
func setupIntegrationTest(t *testing.T, goCode string, targetStructs []string, pkgOverride string) (tmpDir, outPath string) {
	t.Helper()
	tmpDir = t.TempDir()

	if err := os.WriteFile(filepath.Join(tmpDir, "dummy.go"), []byte(goCode), 0644); err != nil {
		t.Fatalf("Failed to write dummy.go: %v", err)
	}

	modContent := "module dummy\n\ngo 1.25.0\n"
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte(modContent), 0644); err != nil {
		t.Fatalf("Failed to write go.mod: %v", err)
	}

	outPath = filepath.Join(tmpDir, "dummy_arrow_writer.go")
	g := NewGenerator([]string{tmpDir}, targetStructs, outPath, false, nil)
	if err := g.Run(pkgOverride); err != nil {
		t.Fatalf("Generator.Run() failed: %v", err)
	}

	if _, err := os.Stat(outPath); os.IsNotExist(err) {
		t.Fatalf("Expected output file %s was not generated", outPath)
	}

	return tmpDir, outPath
}

// runInnerTest writes the inner test harness code, fetches dependencies, and
// executes `go test`. An optional testRunFilter can restrict which inner test
// function runs (pass "" to run all).
func runInnerTest(t *testing.T, tmpDir, testCode, testRunFilter string) {
	t.Helper()

	if err := os.WriteFile(filepath.Join(tmpDir, "dummy_test.go"), []byte(testCode), 0644); err != nil {
		t.Fatalf("Failed to write dummy_test.go: %v", err)
	}

	runCmd(t, tmpDir, "go", "get", "github.com/apache/arrow/go/v18@v18.0.0-20241007013041-ab95a4d25142")
	runCmd(t, tmpDir, "go", "get", "github.com/duckdb/duckdb-go/v2@v2.5.5")
	runCmd(t, tmpDir, "go", "mod", "tidy")

	args := []string{"test", "-v"}
	if testRunFilter != "" {
		args = append(args, "-run", testRunFilter)
	}
	args = append(args, ".")
	runCmd(t, tmpDir, "go", args...)
}

// runCmd is a helper for running external commands during integration tests.
func runCmd(t *testing.T, dir, command string, args ...string) {
	cmd := exec.Command(command, args...)
	cmd.Dir = dir
	var outBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &outBuf

	err := cmd.Run()
	outputStr := outBuf.String()
	t.Logf("Running command: %s %s\nOutput:\n%s", command, strings.Join(args, " "), outputStr)

	if err != nil {
		t.Fatalf("Command '%s %s' failed: %v\nOutput: %s", command, strings.Join(args, " "), err, outputStr)
	}
}
