package readergen

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	writergen "go.resystems.io/eddt/internal/arrow/writer-gen"
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

	t.Run("primitive-round-trip", func(t *testing.T) {
		goCode := `package dummy

type MyID int32

type SimpleStruct struct {
	ID         int32
	Name       string
	Valid      bool
	Score      float64
	SingleByte byte
	ByteSlice  []byte
	CustomID   MyID
}
`
		tmpDir := setupIntegrationTest(t, goCode, []string{"SimpleStruct"})

		testCode := `package dummy

import (
	"bytes"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestPrimitiveRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewSimpleStructArrowWriter(pool)
	defer writer.Release()

	// Row with non-zero values for all fields
	want := SimpleStruct{
		ID:         42,
		Name:       "hello",
		Valid:      true,
		Score:      3.14,
		SingleByte: 0xFF,
		ByteSlice:  []byte{0xDE, 0xAD},
		CustomID:   MyID(7),
	}
	writer.Append(&want)

	// Row with Go zero values (tests R10 null handling — writer emits non-null
	// Arrow values for Go zero values, so the reader should read them back as zero)
	zero := SimpleStruct{}
	writer.Append(&zero)

	rec := writer.NewRecord()
	defer rec.Release()

	if rec.NumRows() != 2 {
		t.Fatalf("expected 2 rows, got %d", rec.NumRows())
	}

	reader, err := NewSimpleStructArrowReader(rec)
	if err != nil {
		t.Fatalf("NewSimpleStructArrowReader: %v", err)
	}

	// Verify non-zero row
	var got SimpleStruct
	reader.LoadRow(0, &got)

	if got.ID != want.ID {
		t.Errorf("ID: got %d, want %d", got.ID, want.ID)
	}
	if got.Name != want.Name {
		t.Errorf("Name: got %q, want %q", got.Name, want.Name)
	}
	if got.Valid != want.Valid {
		t.Errorf("Valid: got %v, want %v", got.Valid, want.Valid)
	}
	if got.Score != want.Score {
		t.Errorf("Score: got %f, want %f", got.Score, want.Score)
	}
	if got.SingleByte != want.SingleByte {
		t.Errorf("SingleByte: got %d, want %d", got.SingleByte, want.SingleByte)
	}
	if !bytes.Equal(got.ByteSlice, want.ByteSlice) {
		t.Errorf("ByteSlice: got %v, want %v", got.ByteSlice, want.ByteSlice)
	}
	if got.CustomID != want.CustomID {
		t.Errorf("CustomID: got %d, want %d", got.CustomID, want.CustomID)
	}

	// Verify zero-value row
	var gotZero SimpleStruct
	reader.LoadRow(1, &gotZero)

	if gotZero.ID != 0 {
		t.Errorf("zero ID: got %d, want 0", gotZero.ID)
	}
	if gotZero.Name != "" {
		t.Errorf("zero Name: got %q, want \"\"", gotZero.Name)
	}
	if gotZero.Valid != false {
		t.Errorf("zero Valid: got %v, want false", gotZero.Valid)
	}
	if gotZero.Score != 0 {
		t.Errorf("zero Score: got %f, want 0", gotZero.Score)
	}
	if gotZero.SingleByte != 0 {
		t.Errorf("zero SingleByte: got %d, want 0", gotZero.SingleByte)
	}
	// The writer appends []byte(nil) as a non-null empty binary value.
	// Arrow Binary.Value() returns []byte{} (not nil) for empty binary,
	// so the round-trip yields an empty slice rather than nil.
	if len(gotZero.ByteSlice) != 0 {
		t.Errorf("zero ByteSlice: got %v, want empty", gotZero.ByteSlice)
	}
	if gotZero.CustomID != 0 {
		t.Errorf("zero CustomID: got %d, want 0", gotZero.CustomID)
	}
}
`
		runInnerTest(t, tmpDir, testCode, "")

		if false {
			tarball(t, "/tmp/arrow-reader-gen-primitive.tar.gz", tmpDir)
		}
	})

	t.Run("pointer-to-primitive-round-trip", func(t *testing.T) {
		goCode := `package dummy

type MyID int32

type PtrStruct struct {
	ID       int32
	OptScore *float64
	OptValid *bool
	OptName  *string
	OptID    *MyID
}
`
		tmpDir := setupIntegrationTest(t, goCode, []string{"PtrStruct"})

		testCode := `package dummy

import (
	"testing"
	"unsafe"

	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestPointerPrimitiveRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewPtrStructArrowWriter(pool)
	defer writer.Release()

	// Row 0: all pointers non-nil
	score := 3.14
	valid := true
	name := "hello"
	myID := MyID(7)
	row0 := PtrStruct{
		ID:       42,
		OptScore: &score,
		OptValid: &valid,
		OptName:  &name,
		OptID:    &myID,
	}
	writer.Append(&row0)

	// Row 1: all pointers nil
	row1 := PtrStruct{ID: 99}
	writer.Append(&row1)

	rec := writer.NewRecord()
	defer rec.Release()

	if rec.NumRows() != 2 {
		t.Fatalf("expected 2 rows, got %d", rec.NumRows())
	}

	reader, err := NewPtrStructArrowReader(rec)
	if err != nil {
		t.Fatalf("NewPtrStructArrowReader: %v", err)
	}

	// --- Row 0: non-nil pointers ---
	var got PtrStruct
	reader.LoadRow(0, &got)

	if got.ID != 42 {
		t.Errorf("row0 ID: got %d, want 42", got.ID)
	}
	if got.OptScore == nil || *got.OptScore != 3.14 {
		t.Errorf("row0 OptScore: got %v, want 3.14", got.OptScore)
	}
	if got.OptValid == nil || *got.OptValid != true {
		t.Errorf("row0 OptValid: got %v, want true", got.OptValid)
	}
	if got.OptName == nil || *got.OptName != "hello" {
		t.Errorf("row0 OptName: got %v, want hello", got.OptName)
	}
	if got.OptID == nil || *got.OptID != MyID(7) {
		t.Errorf("row0 OptID: got %v, want 7", got.OptID)
	}

	// --- Row 1: nil pointers (loaded into same struct — tests null→nil clears) ---
	reader.LoadRow(1, &got)

	if got.ID != 99 {
		t.Errorf("row1 ID: got %d, want 99", got.ID)
	}
	if got.OptScore != nil {
		t.Errorf("row1 OptScore: got %v, want nil", got.OptScore)
	}
	if got.OptValid != nil {
		t.Errorf("row1 OptValid: got %v, want nil", got.OptValid)
	}
	if got.OptName != nil {
		t.Errorf("row1 OptName: got %v, want nil", got.OptName)
	}
	if got.OptID != nil {
		t.Errorf("row1 OptID: got %v, want nil", got.OptID)
	}

	// --- R6 reuse assertion: dereference-assign reuses allocation ---
	reader.LoadRow(0, &got)
	if got.OptScore == nil {
		t.Fatal("expected OptScore non-nil after reload row 0")
	}
	addr1 := uintptr(unsafe.Pointer(got.OptScore))
	reader.LoadRow(0, &got)
	addr2 := uintptr(unsafe.Pointer(got.OptScore))
	if addr1 != addr2 {
		t.Errorf("R6 reuse: OptScore pointer changed (%x → %x), expected same allocation", addr1, addr2)
	}
}
`
		runInnerTest(t, tmpDir, testCode, "")

		if false {
			tarball(t, "/tmp/arrow-reader-gen-pointer.tar.gz", tmpDir)
		}
	})
}

// setupIntegrationTest creates a temp directory, writes the struct definition,
// runs both writer-gen and reader-gen generators, and writes the go.mod.
func setupIntegrationTest(t *testing.T, goCode string, targetStructs []string) string {
	t.Helper()
	tmpDir := t.TempDir()

	if err := os.WriteFile(filepath.Join(tmpDir, "dummy.go"), []byte(goCode), 0644); err != nil {
		t.Fatalf("Failed to write dummy.go: %v", err)
	}

	modContent := "module dummy\n\ngo 1.25.0\n"
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte(modContent), 0644); err != nil {
		t.Fatalf("Failed to write go.mod: %v", err)
	}

	// Fetch arrow dependency before generation so that packages.Load can
	// resolve imports in both generated files.
	runCmd(t, tmpDir, "go", "get", "github.com/apache/arrow/go/v18@v18.0.0-20241007013041-ab95a4d25142")

	// Generate writer
	writerOut := filepath.Join(tmpDir, "dummy_arrow_writer.go")
	wg := writergen.NewGenerator([]string{tmpDir}, targetStructs, writerOut, false, nil)
	if err := wg.Run(""); err != nil {
		t.Fatalf("writer-gen Run() failed: %v", err)
	}

	// Generate reader
	readerOut := filepath.Join(tmpDir, "dummy_arrow_reader.go")
	rg := NewGenerator([]string{tmpDir}, targetStructs, readerOut, false, nil)
	if err := rg.Run(""); err != nil {
		t.Fatalf("reader-gen Run() failed: %v", err)
	}

	return tmpDir
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
