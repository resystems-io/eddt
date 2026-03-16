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

	t.Run("list-round-trip", func(t *testing.T) {
		goCode := `package dummy

type ListStruct struct {
	ID   int32
	Tags []string
	Nums []int32
}
`
		tmpDir := setupIntegrationTest(t, goCode, []string{"ListStruct"})

		testCode := `package dummy

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestListRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewListStructArrowWriter(pool)
	defer writer.Release()

	// Row 0: non-empty lists
	row0 := ListStruct{
		ID:   1,
		Tags: []string{"admin", "user"},
		Nums: []int32{10, 20, 30},
	}
	writer.Append(&row0)

	// Row 1: nil lists (null)
	row1 := ListStruct{ID: 2}
	writer.Append(&row1)

	// Row 2: empty non-nil lists
	row2 := ListStruct{
		ID:   3,
		Tags: []string{},
		Nums: []int32{},
	}
	writer.Append(&row2)

	rec := writer.NewRecord()
	defer rec.Release()

	if rec.NumRows() != 3 {
		t.Fatalf("expected 3 rows, got %d", rec.NumRows())
	}

	reader, err := NewListStructArrowReader(rec)
	if err != nil {
		t.Fatalf("NewListStructArrowReader: %v", err)
	}

	// --- Row 0: non-empty lists ---
	var got ListStruct
	reader.LoadRow(0, &got)
	if got.ID != 1 {
		t.Errorf("row0 ID: got %d, want 1", got.ID)
	}
	if !reflect.DeepEqual(got.Tags, []string{"admin", "user"}) {
		t.Errorf("row0 Tags: got %v, want [admin user]", got.Tags)
	}
	if !reflect.DeepEqual(got.Nums, []int32{10, 20, 30}) {
		t.Errorf("row0 Nums: got %v, want [10 20 30]", got.Nums)
	}

	// --- R6 reuse: save backing array address, reload, verify same address ---
	tagAddr := uintptr(unsafe.Pointer(&got.Tags[0]))
	reader.LoadRow(0, &got)
	tagAddr2 := uintptr(unsafe.Pointer(&got.Tags[0]))
	if tagAddr != tagAddr2 {
		t.Errorf("R6 reuse: Tags backing array changed (%x -> %x)", tagAddr, tagAddr2)
	}

	// --- Row 1: nil lists ---
	reader.LoadRow(1, &got)
	if got.ID != 2 {
		t.Errorf("row1 ID: got %d, want 2", got.ID)
	}
	if got.Tags != nil {
		t.Errorf("row1 Tags: got %v, want nil", got.Tags)
	}
	if got.Nums != nil {
		t.Errorf("row1 Nums: got %v, want nil", got.Nums)
	}

	// --- Null clearing: load row 0 then row 1 into same struct ---
	reader.LoadRow(0, &got)
	if got.Tags == nil {
		t.Fatal("expected non-nil Tags after loading row 0")
	}
	reader.LoadRow(1, &got)
	if got.Tags != nil {
		t.Errorf("null clearing: Tags should be nil after loading null row, got %v", got.Tags)
	}

	// --- Row 2: empty non-nil lists ---
	reader.LoadRow(2, &got)
	if got.ID != 3 {
		t.Errorf("row2 ID: got %d, want 3", got.ID)
	}
	if got.Tags == nil || len(got.Tags) != 0 {
		t.Errorf("row2 Tags: got %v, want non-nil empty slice", got.Tags)
	}
	if got.Nums == nil || len(got.Nums) != 0 {
		t.Errorf("row2 Nums: got %v, want non-nil empty slice", got.Nums)
	}
}
`
		runInnerTest(t, tmpDir, testCode, "")

		if false {
			tarball(t, "/tmp/arrow-reader-gen-list.tar.gz", tmpDir)
		}
	})

	t.Run("nested-list-round-trip", func(t *testing.T) {
		goCode := `package dummy

type Matrix struct {
	ID   int32
	Grid [][]int32
}
`
		tmpDir := setupIntegrationTest(t, goCode, []string{"Matrix"})

		testCode := `package dummy

import (
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestNestedListRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewMatrixArrowWriter(pool)
	defer writer.Release()

	// Row 0: nested list with values
	row0 := Matrix{
		ID:   1,
		Grid: [][]int32{{1, 2, 3}, {4, 5}},
	}
	writer.Append(&row0)

	// Row 1: null outer list
	row1 := Matrix{ID: 2}
	writer.Append(&row1)

	// Row 2: nil inner + non-nil inner
	row2 := Matrix{
		ID:   3,
		Grid: [][]int32{nil, {7}},
	}
	writer.Append(&row2)

	rec := writer.NewRecord()
	defer rec.Release()

	if rec.NumRows() != 3 {
		t.Fatalf("expected 3 rows, got %d", rec.NumRows())
	}

	reader, err := NewMatrixArrowReader(rec)
	if err != nil {
		t.Fatalf("NewMatrixArrowReader: %v", err)
	}

	// --- Row 0: nested list ---
	var got Matrix
	reader.LoadRow(0, &got)
	if got.ID != 1 {
		t.Errorf("row0 ID: got %d, want 1", got.ID)
	}
	if !reflect.DeepEqual(got.Grid, [][]int32{{1, 2, 3}, {4, 5}}) {
		t.Errorf("row0 Grid: got %v, want [[1 2 3] [4 5]]", got.Grid)
	}

	// --- Row 1: null outer ---
	reader.LoadRow(1, &got)
	if got.ID != 2 {
		t.Errorf("row1 ID: got %d, want 2", got.ID)
	}
	if got.Grid != nil {
		t.Errorf("row1 Grid: got %v, want nil", got.Grid)
	}

	// --- Row 2: nil inner + non-nil inner ---
	reader.LoadRow(2, &got)
	if got.ID != 3 {
		t.Errorf("row2 ID: got %d, want 3", got.ID)
	}
	if got.Grid == nil {
		t.Fatal("row2 Grid should not be nil")
	}
	if len(got.Grid) != 2 {
		t.Fatalf("row2 Grid: got len %d, want 2", len(got.Grid))
	}
	if got.Grid[0] != nil {
		t.Errorf("row2 Grid[0]: got %v, want nil", got.Grid[0])
	}
	if !reflect.DeepEqual(got.Grid[1], []int32{7}) {
		t.Errorf("row2 Grid[1]: got %v, want [7]", got.Grid[1])
	}
}
`
		runInnerTest(t, tmpDir, testCode, "")

		if false {
			tarball(t, "/tmp/arrow-reader-gen-nested-list.tar.gz", tmpDir)
		}
	})

	t.Run("fixed-size-round-trip", func(t *testing.T) {
		goCode := `package dummy

type Packet struct {
	ID     int32
	Header [4]byte
	Scores [3]int32
}
`
		tmpDir := setupIntegrationTest(t, goCode, []string{"Packet"})

		testCode := `package dummy

import (
	"testing"

	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestFixedSizeRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewPacketArrowWriter(pool)
	defer writer.Release()

	// Row 0: non-zero values
	row0 := Packet{
		ID:     1,
		Header: [4]byte{0xDE, 0xAD, 0xBE, 0xEF},
		Scores: [3]int32{10, 20, 30},
	}
	writer.Append(&row0)

	// Row 1: Go zero values
	row1 := Packet{ID: 2}
	writer.Append(&row1)

	rec := writer.NewRecord()
	defer rec.Release()

	if rec.NumRows() != 2 {
		t.Fatalf("expected 2 rows, got %d", rec.NumRows())
	}

	reader, err := NewPacketArrowReader(rec)
	if err != nil {
		t.Fatalf("NewPacketArrowReader: %v", err)
	}

	// --- Row 0: non-zero values ---
	var got Packet
	reader.LoadRow(0, &got)
	if got.ID != 1 {
		t.Errorf("row0 ID: got %d, want 1", got.ID)
	}
	if got.Header != [4]byte{0xDE, 0xAD, 0xBE, 0xEF} {
		t.Errorf("row0 Header: got %v, want [DE AD BE EF]", got.Header)
	}
	if got.Scores != [3]int32{10, 20, 30} {
		t.Errorf("row0 Scores: got %v, want [10 20 30]", got.Scores)
	}

	// --- Row 1: zero values ---
	reader.LoadRow(1, &got)
	if got.ID != 2 {
		t.Errorf("row1 ID: got %d, want 2", got.ID)
	}
	if got.Header != [4]byte{} {
		t.Errorf("row1 Header: got %v, want zero", got.Header)
	}
	if got.Scores != [3]int32{} {
		t.Errorf("row1 Scores: got %v, want zero", got.Scores)
	}

	// --- Zero-value overwrite: load row 0 then row 1, verify no dirty reads ---
	reader.LoadRow(0, &got)
	if got.Header != [4]byte{0xDE, 0xAD, 0xBE, 0xEF} {
		t.Fatal("expected non-zero Header after loading row 0")
	}
	reader.LoadRow(1, &got)
	if got.Header != [4]byte{} {
		t.Errorf("dirty read: Header should be zero after loading row 1, got %v", got.Header)
	}
	if got.Scores != [3]int32{} {
		t.Errorf("dirty read: Scores should be zero after loading row 1, got %v", got.Scores)
	}
}
`
		runInnerTest(t, tmpDir, testCode, "")

		if false {
			tarball(t, "/tmp/arrow-reader-gen-fixed-size.tar.gz", tmpDir)
		}
	})

	t.Run("nested-fixed-size-round-trip", func(t *testing.T) {
		goCode := `package dummy

type Matrix struct {
	ID   int32
	Grid [2][3]int32
}
`
		tmpDir := setupIntegrationTest(t, goCode, []string{"Matrix"})

		testCode := `package dummy

import (
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestNestedFixedSizeRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewMatrixArrowWriter(pool)
	defer writer.Release()

	// Row 0: nested fixed-size values
	row0 := Matrix{
		ID:   1,
		Grid: [2][3]int32{{1, 2, 3}, {4, 5, 6}},
	}
	writer.Append(&row0)

	// Row 1: Go zero value
	row1 := Matrix{ID: 2}
	writer.Append(&row1)

	rec := writer.NewRecord()
	defer rec.Release()

	if rec.NumRows() != 2 {
		t.Fatalf("expected 2 rows, got %d", rec.NumRows())
	}

	reader, err := NewMatrixArrowReader(rec)
	if err != nil {
		t.Fatalf("NewMatrixArrowReader: %v", err)
	}

	// --- Row 0: nested values ---
	var got Matrix
	reader.LoadRow(0, &got)
	if got.ID != 1 {
		t.Errorf("row0 ID: got %d, want 1", got.ID)
	}
	if !reflect.DeepEqual(got.Grid, [2][3]int32{{1, 2, 3}, {4, 5, 6}}) {
		t.Errorf("row0 Grid: got %v, want [[1 2 3] [4 5 6]]", got.Grid)
	}

	// --- Row 1: zero value ---
	reader.LoadRow(1, &got)
	if got.ID != 2 {
		t.Errorf("row1 ID: got %d, want 2", got.ID)
	}
	if got.Grid != [2][3]int32{} {
		t.Errorf("row1 Grid: got %v, want zero", got.Grid)
	}

	// --- Zero-value overwrite: load row 0 then row 1 ---
	reader.LoadRow(0, &got)
	if got.Grid == [2][3]int32{} {
		t.Fatal("expected non-zero Grid after loading row 0")
	}
	reader.LoadRow(1, &got)
	if got.Grid != [2][3]int32{} {
		t.Errorf("dirty read: Grid should be zero after loading row 1, got %v", got.Grid)
	}
}
`
		runInnerTest(t, tmpDir, testCode, "")

		if false {
			tarball(t, "/tmp/arrow-reader-gen-nested-fixed-size.tar.gz", tmpDir)
		}
	})

	t.Run("mixed-list-fixed-round-trip", func(t *testing.T) {
		goCode := `package dummy

type Mixed struct {
	ID   int32
	Rows [][3]int32
}
`
		tmpDir := setupIntegrationTest(t, goCode, []string{"Mixed"})

		testCode := `package dummy

import (
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestMixedListFixedRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewMixedArrowWriter(pool)
	defer writer.Release()

	// Row 0: list of fixed-size arrays
	row0 := Mixed{
		ID:   1,
		Rows: [][3]int32{{1, 2, 3}, {4, 5, 6}},
	}
	writer.Append(&row0)

	// Row 1: nil list (null)
	row1 := Mixed{ID: 2}
	writer.Append(&row1)

	// Row 2: empty non-nil list
	row2 := Mixed{
		ID:   3,
		Rows: [][3]int32{},
	}
	writer.Append(&row2)

	rec := writer.NewRecord()
	defer rec.Release()

	if rec.NumRows() != 3 {
		t.Fatalf("expected 3 rows, got %d", rec.NumRows())
	}

	reader, err := NewMixedArrowReader(rec)
	if err != nil {
		t.Fatalf("NewMixedArrowReader: %v", err)
	}

	// --- Row 0: list of fixed-size arrays ---
	var got Mixed
	reader.LoadRow(0, &got)
	if got.ID != 1 {
		t.Errorf("row0 ID: got %d, want 1", got.ID)
	}
	if !reflect.DeepEqual(got.Rows, [][3]int32{{1, 2, 3}, {4, 5, 6}}) {
		t.Errorf("row0 Rows: got %v, want [[1 2 3] [4 5 6]]", got.Rows)
	}

	// --- Row 1: nil list ---
	reader.LoadRow(1, &got)
	if got.ID != 2 {
		t.Errorf("row1 ID: got %d, want 2", got.ID)
	}
	if got.Rows != nil {
		t.Errorf("row1 Rows: got %v, want nil", got.Rows)
	}

	// --- Row 2: empty non-nil list ---
	reader.LoadRow(2, &got)
	if got.ID != 3 {
		t.Errorf("row2 ID: got %d, want 3", got.ID)
	}
	if got.Rows == nil || len(got.Rows) != 0 {
		t.Errorf("row2 Rows: got %v, want non-nil empty slice", got.Rows)
	}
}
`
		runInnerTest(t, tmpDir, testCode, "")

		if false {
			tarball(t, "/tmp/arrow-reader-gen-mixed-list-fixed.tar.gz", tmpDir)
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
