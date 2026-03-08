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
		// 1. Create a dummy package directory
		tmpDir := t.TempDir()

		// 2. Write the Go struct definition payload
		dummyCode := `package dummy

type User struct {
	ID    int32
	Name  string
	Score float64
	Valid bool
}
`
		if err := os.WriteFile(filepath.Join(tmpDir, "dummy.go"), []byte(dummyCode), 0644); err != nil {
			t.Fatalf("Failed to write dummy.go: %v", err)
		}

		modContent := "module dummy\n\ngo 1.25.0\n"
		if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte(modContent), 0644); err != nil {
			t.Fatalf("Failed to write go.mod: %v", err)
		}

		// 3. Initiate our Code Generator logic targeting "User"
		outPath := filepath.Join(tmpDir, "dummy_arrow_writer.go")
		g := NewGenerator(tmpDir, []string{"User"}, outPath, false)
		if err := g.Run("dummy"); err != nil {
			t.Fatalf("Generator.Run() failed: %v", err)
		}

		// 4. Validate output file exists
		if _, err := os.Stat(outPath); os.IsNotExist(err) {
			t.Fatalf("Expected output file %s was not generated", outPath)
		}

		// 5. Write the testing harness (dummy_test.go) that incorporates DuckDB & Parquet
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

	writer.Append(u1)
	writer.Append(u2)

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

		if err := os.WriteFile(filepath.Join(tmpDir, "dummy_test.go"), []byte(testCode), 0644); err != nil {
			t.Fatalf("Failed to write dummy_test.go: %v", err)
		}

		// Get supporting libraries
		runCmd(t, tmpDir, "go", "get", "github.com/apache/arrow/go/v18@v18.0.0-20241007013041-ab95a4d25142")
		runCmd(t, tmpDir, "go", "get", "github.com/duckdb/duckdb-go/v2@v2.5.5")
		runCmd(t, tmpDir, "go", "mod", "tidy")

		// Execute test!
		runCmd(t, tmpDir, "go", "test", "-v", ".")

		// Create a tarball of the temp directory for debugging.
		if false {
			tarball(t, "/tmp/arrow-gen-simple.tar.gz", tmpDir)
		}
	})

	t.Run("lists-and-maps", func(t *testing.T) {
		// 1. Create a dummy package directory

		tmpDir := t.TempDir()

		// 2. Write the Go struct definition payload
		dummyCode := `package dummy

type ComplexUser struct {
	ID     int32
	Tags   []string
	Scores map[string]float64
}
`
		if err := os.WriteFile(filepath.Join(tmpDir, "dummy.go"), []byte(dummyCode), 0644); err != nil {
			t.Fatalf("Failed to write dummy.go: %v", err)
		}

		modContent := "module dummy\n\ngo 1.25.0\n"
		if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte(modContent), 0644); err != nil {
			t.Fatalf("Failed to write go.mod: %v", err)
		}

		// 3. Initiate our Code Generator logic targeting "ComplexUser"
		outPath := filepath.Join(tmpDir, "dummy_arrow_writer.go")
		g := NewGenerator(tmpDir, []string{"ComplexUser"}, outPath, false)
		if err := g.Run("dummy"); err != nil {
			t.Fatalf("Generator.Run() failed: %v", err)
		}

		// 4. Validate output file exists
		if _, err := os.Stat(outPath); os.IsNotExist(err) {
			t.Fatalf("Expected output file %s was not generated", outPath)
		}

		// 5. Write the testing harness (dummy_test.go) that incorporates DuckDB & Parquet
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

	writer.Append(u1)
	writer.Append(u2)
	writer.Append(u3)

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
		Key   string  ` + "`" + `duckdb:"key"` + "`" + `
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

		if err := os.WriteFile(filepath.Join(tmpDir, "dummy_test.go"), []byte(testCode), 0644); err != nil {
			t.Fatalf("Failed to write dummy_test.go: %v", err)
		}

		// Get supporting libraries
		runCmd(t, tmpDir, "go", "get", "github.com/apache/arrow/go/v18@v18.0.0-20241007013041-ab95a4d25142")
		runCmd(t, tmpDir, "go", "get", "github.com/duckdb/duckdb-go/v2@v2.5.5")
		runCmd(t, tmpDir, "go", "mod", "tidy")

		// Execute test!
		runCmd(t, tmpDir, "go", "test", "-v", ".")

		// Create a tarball of the temp directory for debugging.
		if false {
			tarball(t, "/tmp/arrow-gen-list-and-map.tar.gz", tmpDir)
		}
	})
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
