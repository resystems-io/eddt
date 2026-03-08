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
			tarPath := "/tmp/dummy_test_debug.tar.gz"
			tarCmd := exec.Command("tar", "-czf", tarPath, "-C", tmpDir, ".")
			if err := tarCmd.Run(); err != nil {
				t.Logf("Failed to create debug tarball: %v", err)
			} else {
				t.Logf("Successfully created debug tarball at %s", tarPath)
			}
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
