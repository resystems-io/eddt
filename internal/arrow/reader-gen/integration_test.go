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

	t.Run("map-round-trip", func(t *testing.T) {
		goCode := `package dummy

type MapStruct struct {
	ID     int32
	Scores map[string]float64
}
`
		tmpDir := setupIntegrationTest(t, goCode, []string{"MapStruct"})

		testCode := `package dummy

import (
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestMapRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewMapStructArrowWriter(pool)
	defer writer.Release()

	// Row 0: non-empty map
	row0 := MapStruct{
		ID:     1,
		Scores: map[string]float64{"math": 95.5, "science": 88.0},
	}
	writer.Append(&row0)

	// Row 1: nil map (null)
	row1 := MapStruct{ID: 2}
	writer.Append(&row1)

	// Row 2: empty non-nil map
	row2 := MapStruct{
		ID:     3,
		Scores: map[string]float64{},
	}
	writer.Append(&row2)

	rec := writer.NewRecord()
	defer rec.Release()

	if rec.NumRows() != 3 {
		t.Fatalf("expected 3 rows, got %d", rec.NumRows())
	}

	reader, err := NewMapStructArrowReader(rec)
	if err != nil {
		t.Fatalf("NewMapStructArrowReader: %v", err)
	}

	// --- Row 0: non-empty map ---
	var got MapStruct
	reader.LoadRow(0, &got)
	if got.ID != 1 {
		t.Errorf("row0 ID: got %d, want 1", got.ID)
	}
	if !reflect.DeepEqual(got.Scores, map[string]float64{"math": 95.5, "science": 88.0}) {
		t.Errorf("row0 Scores: got %v, want map[math:95.5 science:88]", got.Scores)
	}

	// --- R6 reuse: reload same row, verify map is reused (not nil between loads) ---
	reader.LoadRow(0, &got)
	if !reflect.DeepEqual(got.Scores, map[string]float64{"math": 95.5, "science": 88.0}) {
		t.Errorf("R6 reuse Scores: got %v", got.Scores)
	}

	// --- Row 1: nil map ---
	reader.LoadRow(1, &got)
	if got.ID != 2 {
		t.Errorf("row1 ID: got %d, want 2", got.ID)
	}
	if got.Scores != nil {
		t.Errorf("row1 Scores: got %v, want nil", got.Scores)
	}

	// --- Null clearing: load row 0 then row 1 ---
	reader.LoadRow(0, &got)
	if got.Scores == nil {
		t.Fatal("expected non-nil Scores after loading row 0")
	}
	reader.LoadRow(1, &got)
	if got.Scores != nil {
		t.Errorf("null clearing: Scores should be nil after loading null row, got %v", got.Scores)
	}

	// --- Row 2: empty non-nil map ---
	reader.LoadRow(2, &got)
	if got.ID != 3 {
		t.Errorf("row2 ID: got %d, want 3", got.ID)
	}
	if got.Scores == nil || len(got.Scores) != 0 {
		t.Errorf("row2 Scores: got %v, want non-nil empty map", got.Scores)
	}
}
`
		runInnerTest(t, tmpDir, testCode, "")

		if false {
			tarball(t, "/tmp/arrow-reader-gen-map.tar.gz", tmpDir)
		}
	})

	t.Run("map-int-keys-round-trip", func(t *testing.T) {
		goCode := `package dummy

type IntKeyMap struct {
	ID   int32
	Data map[int32]string
}
`
		tmpDir := setupIntegrationTest(t, goCode, []string{"IntKeyMap"})

		testCode := `package dummy

import (
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestIntKeyMapRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewIntKeyMapArrowWriter(pool)
	defer writer.Release()

	// Row 0: non-empty map with int keys
	row0 := IntKeyMap{
		ID:   1,
		Data: map[int32]string{1: "one", 2: "two", 3: "three"},
	}
	writer.Append(&row0)

	// Row 1: nil map
	row1 := IntKeyMap{ID: 2}
	writer.Append(&row1)

	rec := writer.NewRecord()
	defer rec.Release()

	reader, err := NewIntKeyMapArrowReader(rec)
	if err != nil {
		t.Fatalf("NewIntKeyMapArrowReader: %v", err)
	}

	var got IntKeyMap
	reader.LoadRow(0, &got)
	if got.ID != 1 {
		t.Errorf("row0 ID: got %d, want 1", got.ID)
	}
	if !reflect.DeepEqual(got.Data, map[int32]string{1: "one", 2: "two", 3: "three"}) {
		t.Errorf("row0 Data: got %v", got.Data)
	}

	reader.LoadRow(1, &got)
	if got.Data != nil {
		t.Errorf("row1 Data: got %v, want nil", got.Data)
	}
}
`
		runInnerTest(t, tmpDir, testCode, "")

		if false {
			tarball(t, "/tmp/arrow-reader-gen-map-int-keys.tar.gz", tmpDir)
		}
	})

	t.Run("nested-map-round-trip", func(t *testing.T) {
		goCode := `package dummy

type NestedMap struct {
	ID       int32
	Settings map[string]map[string]int32
}
`
		tmpDir := setupIntegrationTest(t, goCode, []string{"NestedMap"})

		testCode := `package dummy

import (
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestNestedMapRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewNestedMapArrowWriter(pool)
	defer writer.Release()

	// Row 0: nested map with values
	row0 := NestedMap{
		ID: 1,
		Settings: map[string]map[string]int32{
			"audio":   {"volume": 80, "bass": 50},
			"display": {"brightness": 100},
		},
	}
	writer.Append(&row0)

	// Row 1: nil outer map
	row1 := NestedMap{ID: 2}
	writer.Append(&row1)

	// Row 2: nil inner map value
	row2 := NestedMap{
		ID: 3,
		Settings: map[string]map[string]int32{
			"audio": nil,
			"video": {"fps": 60},
		},
	}
	writer.Append(&row2)

	rec := writer.NewRecord()
	defer rec.Release()

	reader, err := NewNestedMapArrowReader(rec)
	if err != nil {
		t.Fatalf("NewNestedMapArrowReader: %v", err)
	}

	// --- Row 0: nested map ---
	var got NestedMap
	reader.LoadRow(0, &got)
	if got.ID != 1 {
		t.Errorf("row0 ID: got %d, want 1", got.ID)
	}
	if !reflect.DeepEqual(got.Settings, map[string]map[string]int32{
		"audio":   {"volume": 80, "bass": 50},
		"display": {"brightness": 100},
	}) {
		t.Errorf("row0 Settings: got %v", got.Settings)
	}

	// --- Row 1: nil outer map ---
	reader.LoadRow(1, &got)
	if got.Settings != nil {
		t.Errorf("row1 Settings: got %v, want nil", got.Settings)
	}

	// --- Row 2: nil inner map value ---
	reader.LoadRow(2, &got)
	if got.Settings == nil {
		t.Fatal("row2 Settings should not be nil")
	}
	if got.Settings["audio"] != nil {
		t.Errorf("row2 Settings[audio]: got %v, want nil", got.Settings["audio"])
	}
	if !reflect.DeepEqual(got.Settings["video"], map[string]int32{"fps": 60}) {
		t.Errorf("row2 Settings[video]: got %v", got.Settings["video"])
	}
}
`
		runInnerTest(t, tmpDir, testCode, "")

		if false {
			tarball(t, "/tmp/arrow-reader-gen-nested-map.tar.gz", tmpDir)
		}
	})

	t.Run("map-list-values-round-trip", func(t *testing.T) {
		goCode := `package dummy

type MapListVal struct {
	ID   int32
	Data map[string][]int32
}
`
		tmpDir := setupIntegrationTest(t, goCode, []string{"MapListVal"})

		testCode := `package dummy

import (
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestMapListValRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewMapListValArrowWriter(pool)
	defer writer.Release()

	// Row 0: map with list values
	row0 := MapListVal{
		ID: 1,
		Data: map[string][]int32{
			"primes": {2, 3, 5, 7},
			"evens":  {2, 4, 6},
		},
	}
	writer.Append(&row0)

	// Row 1: nil map
	row1 := MapListVal{ID: 2}
	writer.Append(&row1)

	// Row 2: map with nil slice value
	row2 := MapListVal{
		ID: 3,
		Data: map[string][]int32{
			"empty": nil,
			"one":   {1},
		},
	}
	writer.Append(&row2)

	rec := writer.NewRecord()
	defer rec.Release()

	reader, err := NewMapListValArrowReader(rec)
	if err != nil {
		t.Fatalf("NewMapListValArrowReader: %v", err)
	}

	// --- Row 0: map with list values ---
	var got MapListVal
	reader.LoadRow(0, &got)
	if got.ID != 1 {
		t.Errorf("row0 ID: got %d, want 1", got.ID)
	}
	if !reflect.DeepEqual(got.Data, map[string][]int32{
		"primes": {2, 3, 5, 7},
		"evens":  {2, 4, 6},
	}) {
		t.Errorf("row0 Data: got %v", got.Data)
	}

	// --- Row 1: nil map ---
	reader.LoadRow(1, &got)
	if got.Data != nil {
		t.Errorf("row1 Data: got %v, want nil", got.Data)
	}

	// --- Row 2: map with nil slice value ---
	reader.LoadRow(2, &got)
	if got.Data == nil {
		t.Fatal("row2 Data should not be nil")
	}
	if got.Data["empty"] != nil {
		t.Errorf("row2 Data[empty]: got %v, want nil", got.Data["empty"])
	}
	if !reflect.DeepEqual(got.Data["one"], []int32{1}) {
		t.Errorf("row2 Data[one]: got %v", got.Data["one"])
	}
}
`
		runInnerTest(t, tmpDir, testCode, "")

		if false {
			tarball(t, "/tmp/arrow-reader-gen-map-list-values.tar.gz", tmpDir)
		}
	})

	t.Run("named-map-round-trip", func(t *testing.T) {
		goCode := `package dummy

type Settings map[string]int32

type NamedMapStruct struct {
	ID     int32
	Config Settings
}
`
		tmpDir := setupIntegrationTest(t, goCode, []string{"NamedMapStruct"})

		testCode := `package dummy

import (
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestNamedMapRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewNamedMapStructArrowWriter(pool)
	defer writer.Release()

	// Row 0: non-empty named map
	row0 := NamedMapStruct{
		ID:     1,
		Config: Settings{"volume": 80, "brightness": 100},
	}
	writer.Append(&row0)

	// Row 1: nil named map
	row1 := NamedMapStruct{ID: 2}
	writer.Append(&row1)

	rec := writer.NewRecord()
	defer rec.Release()

	reader, err := NewNamedMapStructArrowReader(rec)
	if err != nil {
		t.Fatalf("NewNamedMapStructArrowReader: %v", err)
	}

	var got NamedMapStruct
	reader.LoadRow(0, &got)
	if got.ID != 1 {
		t.Errorf("row0 ID: got %d, want 1", got.ID)
	}
	if !reflect.DeepEqual(got.Config, Settings{"volume": 80, "brightness": 100}) {
		t.Errorf("row0 Config: got %v", got.Config)
	}

	reader.LoadRow(1, &got)
	if got.Config != nil {
		t.Errorf("row1 Config: got %v, want nil", got.Config)
	}
}
`
		runInnerTest(t, tmpDir, testCode, "")

		if false {
			tarball(t, "/tmp/arrow-reader-gen-named-map.tar.gz", tmpDir)
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

	t.Run("struct-round-trip", func(t *testing.T) {
		goCode := `package dummy

type Address struct {
	ZipCode int32
	City    string
}

type Profile struct {
	ID      int32
	Address Address
}
`
		tmpDir := setupIntegrationTest(t, goCode, []string{"Profile"})

		testCode := `package dummy

import (
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestStructRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewProfileArrowWriter(pool)
	defer writer.Release()

	// Row 0: populated struct
	row0 := Profile{
		ID: 1,
		Address: Address{ZipCode: 90210, City: "Beverly Hills"},
	}
	writer.Append(&row0)

	// Row 1: zero-value struct
	row1 := Profile{ID: 2}
	writer.Append(&row1)

	rec := writer.NewRecord()
	defer rec.Release()

	reader, err := NewProfileArrowReader(rec)
	if err != nil {
		t.Fatalf("NewProfileArrowReader: %v", err)
	}

	// Row 0: populated
	var got Profile
	reader.LoadRow(0, &got)
	if !reflect.DeepEqual(got, row0) {
		t.Errorf("row0: got %+v, want %+v", got, row0)
	}

	// Row 1: zero-value struct fields
	reader.LoadRow(1, &got)
	if got.ID != 2 {
		t.Errorf("row1 ID: got %d, want 2", got.ID)
	}
	if got.Address != (Address{}) {
		t.Errorf("row1 Address: got %+v, want zero", got.Address)
	}

	// Overwrite test: load row 0 then row 1 into same struct
	reader.LoadRow(0, &got)
	if got.Address.City != "Beverly Hills" {
		t.Fatal("expected populated Address after loading row 0")
	}
	reader.LoadRow(1, &got)
	if got.Address != (Address{}) {
		t.Errorf("dirty read: Address should be zero after loading row 1, got %+v", got.Address)
	}
}
`
		runInnerTest(t, tmpDir, testCode, "")
	})

	t.Run("pointer-to-struct-round-trip", func(t *testing.T) {
		goCode := `package dummy

type Address struct {
	ZipCode int32
	City    string
}

type Profile struct {
	ID    int32
	PAddr *Address
}
`
		tmpDir := setupIntegrationTest(t, goCode, []string{"Profile"})

		testCode := `package dummy

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestPointerToStructRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewProfileArrowWriter(pool)
	defer writer.Release()

	// Row 0: non-nil pointer
	row0 := Profile{
		ID:    1,
		PAddr: &Address{ZipCode: 90210, City: "Beverly Hills"},
	}
	writer.Append(&row0)

	// Row 1: nil pointer
	row1 := Profile{ID: 2}
	writer.Append(&row1)

	rec := writer.NewRecord()
	defer rec.Release()

	reader, err := NewProfileArrowReader(rec)
	if err != nil {
		t.Fatalf("NewProfileArrowReader: %v", err)
	}

	// Row 0: non-nil
	var got Profile
	reader.LoadRow(0, &got)
	if got.ID != 1 {
		t.Errorf("row0 ID: got %d, want 1", got.ID)
	}
	if got.PAddr == nil {
		t.Fatal("row0 PAddr should not be nil")
	}
	if !reflect.DeepEqual(*got.PAddr, *row0.PAddr) {
		t.Errorf("row0 PAddr: got %+v, want %+v", *got.PAddr, *row0.PAddr)
	}

	// Row 1: nil
	reader.LoadRow(1, &got)
	if got.ID != 2 {
		t.Errorf("row1 ID: got %d, want 2", got.ID)
	}
	if got.PAddr != nil {
		t.Errorf("row1 PAddr: got %+v, want nil", got.PAddr)
	}

	// R6 reuse: load non-nil row, save pointer address, reload, verify same address
	reader.LoadRow(0, &got)
	if got.PAddr == nil {
		t.Fatal("expected non-nil PAddr after reload row 0")
	}
	addr1 := uintptr(unsafe.Pointer(got.PAddr))
	reader.LoadRow(0, &got)
	addr2 := uintptr(unsafe.Pointer(got.PAddr))
	if addr1 != addr2 {
		t.Errorf("R6 reuse: PAddr pointer changed (%x -> %x)", addr1, addr2)
	}

	// Null clearing: load non-nil then nil
	reader.LoadRow(0, &got)
	if got.PAddr == nil {
		t.Fatal("expected non-nil PAddr")
	}
	reader.LoadRow(1, &got)
	if got.PAddr != nil {
		t.Errorf("null clearing: PAddr should be nil, got %+v", got.PAddr)
	}
}
`
		runInnerTest(t, tmpDir, testCode, "")
	})

	t.Run("nested-struct-round-trip", func(t *testing.T) {
		goCode := `package dummy

type Inner struct {
	Value int32
}

type Middle struct {
	Child Inner
}

type Outer struct {
	ID int32
	M  Middle
}
`
		tmpDir := setupIntegrationTest(t, goCode, []string{"Outer"})

		testCode := `package dummy

import (
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestNestedStructRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewOuterArrowWriter(pool)
	defer writer.Release()

	// Row 0: populated through 3 levels
	row0 := Outer{
		ID: 1,
		M:  Middle{Child: Inner{Value: 42}},
	}
	writer.Append(&row0)

	// Row 1: zero values
	row1 := Outer{ID: 2}
	writer.Append(&row1)

	rec := writer.NewRecord()
	defer rec.Release()

	reader, err := NewOuterArrowReader(rec)
	if err != nil {
		t.Fatalf("NewOuterArrowReader: %v", err)
	}

	// Row 0
	var got Outer
	reader.LoadRow(0, &got)
	if !reflect.DeepEqual(got, row0) {
		t.Errorf("row0: got %+v, want %+v", got, row0)
	}

	// Row 1
	reader.LoadRow(1, &got)
	if got.ID != 2 {
		t.Errorf("row1 ID: got %d, want 2", got.ID)
	}
	if got.M != (Middle{}) {
		t.Errorf("row1 M: got %+v, want zero", got.M)
	}
}
`
		runInnerTest(t, tmpDir, testCode, "")
	})

	t.Run("list-of-structs-round-trip", func(t *testing.T) {
		goCode := `package dummy

type Address struct {
	ZipCode int32
	City    string
}

type Profile struct {
	ID    int32
	Addrs []Address
}
`
		tmpDir := setupIntegrationTest(t, goCode, []string{"Profile"})

		testCode := `package dummy

import (
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestListOfStructsRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewProfileArrowWriter(pool)
	defer writer.Release()

	// Row 0: non-empty list of structs
	row0 := Profile{
		ID: 1,
		Addrs: []Address{
			{ZipCode: 90210, City: "Beverly Hills"},
			{ZipCode: 10001, City: "New York"},
		},
	}
	writer.Append(&row0)

	// Row 1: nil list
	row1 := Profile{ID: 2}
	writer.Append(&row1)

	// Row 2: empty non-nil list
	row2 := Profile{
		ID:    3,
		Addrs: []Address{},
	}
	writer.Append(&row2)

	rec := writer.NewRecord()
	defer rec.Release()

	reader, err := NewProfileArrowReader(rec)
	if err != nil {
		t.Fatalf("NewProfileArrowReader: %v", err)
	}

	// Row 0
	var got Profile
	reader.LoadRow(0, &got)
	if !reflect.DeepEqual(got, row0) {
		t.Errorf("row0: got %+v, want %+v", got, row0)
	}

	// Row 1: nil list
	reader.LoadRow(1, &got)
	if got.Addrs != nil {
		t.Errorf("row1 Addrs: got %v, want nil", got.Addrs)
	}

	// Row 2: empty non-nil list
	reader.LoadRow(2, &got)
	if got.Addrs == nil || len(got.Addrs) != 0 {
		t.Errorf("row2 Addrs: got %v, want non-nil empty", got.Addrs)
	}
}
`
		runInnerTest(t, tmpDir, testCode, "")
	})

	t.Run("map-struct-values-round-trip", func(t *testing.T) {
		goCode := `package dummy

type Contact struct {
	Email string
}

type Profile struct {
	ID       int32
	Contacts map[string]Contact
}
`
		tmpDir := setupIntegrationTest(t, goCode, []string{"Profile"})

		testCode := `package dummy

import (
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestMapStructValuesRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewProfileArrowWriter(pool)
	defer writer.Release()

	// Row 0: populated map with struct values
	row0 := Profile{
		ID: 1,
		Contacts: map[string]Contact{
			"work": {Email: "work@example.com"},
			"home": {Email: "home@example.com"},
		},
	}
	writer.Append(&row0)

	// Row 1: nil map
	row1 := Profile{ID: 2}
	writer.Append(&row1)

	rec := writer.NewRecord()
	defer rec.Release()

	reader, err := NewProfileArrowReader(rec)
	if err != nil {
		t.Fatalf("NewProfileArrowReader: %v", err)
	}

	// Row 0
	var got Profile
	reader.LoadRow(0, &got)
	if !reflect.DeepEqual(got, row0) {
		t.Errorf("row0: got %+v, want %+v", got, row0)
	}

	// Row 1: nil map
	reader.LoadRow(1, &got)
	if got.Contacts != nil {
		t.Errorf("row1 Contacts: got %v, want nil", got.Contacts)
	}
}
`
		runInnerTest(t, tmpDir, testCode, "")
	})

	t.Run("struct-with-containers-round-trip", func(t *testing.T) {
		goCode := `package dummy

type Address struct {
	ZipCode int32
	Tags    []string
}

type Profile struct {
	ID   int32
	Addr Address
}
`
		tmpDir := setupIntegrationTest(t, goCode, []string{"Profile"})

		testCode := `package dummy

import (
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestStructWithContainersRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewProfileArrowWriter(pool)
	defer writer.Release()

	// Row 0: struct child with list field
	row0 := Profile{
		ID: 1,
		Addr: Address{
			ZipCode: 90210,
			Tags:    []string{"primary", "billing"},
		},
	}
	writer.Append(&row0)

	rec := writer.NewRecord()
	defer rec.Release()

	reader, err := NewProfileArrowReader(rec)
	if err != nil {
		t.Fatalf("NewProfileArrowReader: %v", err)
	}

	var got Profile
	reader.LoadRow(0, &got)
	if !reflect.DeepEqual(got, row0) {
		t.Errorf("row0: got %+v, want %+v", got, row0)
	}
}
`
		runInnerTest(t, tmpDir, testCode, "")
	})

	t.Run("time-duration-round-trip", func(t *testing.T) {
		goCode := `package dummy

import "time"

type Timing struct {
	Elapsed  time.Duration
	OptDelay *time.Duration
}
`
		tmpDir := setupIntegrationTest(t, goCode, []string{"Timing"})

		testCode := `package dummy

import (
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestTimeDurationRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewTimingArrowWriter(pool)
	defer writer.Release()

	dur := 5 * time.Second
	r1 := Timing{Elapsed: 2 * time.Hour, OptDelay: &dur}
	r2 := Timing{Elapsed: 0, OptDelay: nil}

	writer.Append(&r1)
	writer.Append(&r2)

	rec := writer.NewRecord()
	defer rec.Release()

	reader, err := NewTimingArrowReader(rec)
	if err != nil {
		t.Fatalf("NewTimingArrowReader: %v", err)
	}

	var got Timing
	reader.LoadRow(0, &got)
	if got.Elapsed != r1.Elapsed {
		t.Errorf("row0 Elapsed: got %v, want %v", got.Elapsed, r1.Elapsed)
	}
	if got.OptDelay == nil || *got.OptDelay != *r1.OptDelay {
		t.Errorf("row0 OptDelay: got %v, want %v", got.OptDelay, r1.OptDelay)
	}

	reader.LoadRow(1, &got)
	if got.Elapsed != 0 {
		t.Errorf("row1 Elapsed: got %v, want 0", got.Elapsed)
	}
	if got.OptDelay != nil {
		t.Errorf("row1 OptDelay: got %v, want nil", got.OptDelay)
	}
}
`
		runInnerTest(t, tmpDir, testCode, "TestTimeDurationRoundTrip")
	})

	t.Run("time-time-round-trip", func(t *testing.T) {
		goCode := `package dummy

import "time"

type Event struct {
	When    time.Time
	OptWhen *time.Time
}
`
		tmpDir := setupIntegrationTest(t, goCode, []string{"Event"})

		testCode := `package dummy

import (
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestTimeTimeRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewEventArrowWriter(pool)
	defer writer.Release()

	t1 := time.Date(2025, 3, 15, 12, 0, 0, 0, time.UTC)
	r1 := Event{When: t1, OptWhen: &t1}
	r2 := Event{When: time.Time{}, OptWhen: nil}

	writer.Append(&r1)
	writer.Append(&r2)

	rec := writer.NewRecord()
	defer rec.Release()

	reader, err := NewEventArrowReader(rec)
	if err != nil {
		t.Fatalf("NewEventArrowReader: %v", err)
	}

	var got Event
	reader.LoadRow(0, &got)
	if !got.When.Equal(r1.When) {
		t.Errorf("row0 When: got %v, want %v", got.When, r1.When)
	}
	if got.OptWhen == nil || !got.OptWhen.Equal(*r1.OptWhen) {
		t.Errorf("row0 OptWhen: got %v, want %v", got.OptWhen, r1.OptWhen)
	}

	reader.LoadRow(1, &got)
	if !got.When.Equal(time.Unix(0, 0).UTC()) {
		// time.Time{}.UnixNano() is a large negative number; writer stores UnixNano,
		// reader reconstructs via time.Unix(0, nano). Zero time round-trips to epoch.
		// The writer converts time.Time{} → UnixNano → arrow.Timestamp. The reader
		// converts back via time.Unix(0, int64(v)). So the zero value round-trips to
		// what time.Unix(0, time.Time{}.UnixNano()) produces.
		expected := time.Unix(0, time.Time{}.UnixNano())
		if !got.When.Equal(expected) {
			t.Errorf("row1 When: got %v, want %v", got.When, expected)
		}
	}
	if got.OptWhen != nil {
		t.Errorf("row1 OptWhen: got %v, want nil", got.OptWhen)
	}
}
`
		runInnerTest(t, tmpDir, testCode, "TestTimeTimeRoundTrip")
	})

	t.Run("protobuf-duration-round-trip", func(t *testing.T) {
		tmpDir := t.TempDir()

		goCode := `package dummy

import "google.golang.org/protobuf/types/known/durationpb"

type PBDurEvent struct {
	ID       int32
	Duration *durationpb.Duration
	Timeout  durationpb.Duration
}
`
		if err := os.WriteFile(filepath.Join(tmpDir, "dummy.go"), []byte(goCode), 0644); err != nil {
			t.Fatalf("write dummy.go: %v", err)
		}
		if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module dummy\n\ngo 1.25.0\n"), 0644); err != nil {
			t.Fatalf("write go.mod: %v", err)
		}

		runCmd(t, tmpDir, "go", "get", "github.com/apache/arrow/go/v18@v18.0.0-20241007013041-ab95a4d25142")
		runCmd(t, tmpDir, "go", "get", "google.golang.org/protobuf/types/known/durationpb")

		writerOut := filepath.Join(tmpDir, "dummy_arrow_writer.go")
		wg := writergen.NewGenerator([]string{tmpDir}, []string{"PBDurEvent"}, writerOut, false, nil)
		if err := wg.Run(""); err != nil {
			t.Fatalf("writer-gen Run() failed: %v", err)
		}

		readerOut := filepath.Join(tmpDir, "dummy_arrow_reader.go")
		rg := NewGenerator([]string{tmpDir}, []string{"PBDurEvent"}, readerOut, false, nil)
		if err := rg.Run(""); err != nil {
			t.Fatalf("reader-gen Run() failed: %v", err)
		}

		testCode := `package dummy

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestProtobufDurationRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewPBDurEventArrowWriter(pool)
	defer writer.Release()

	dur2h := durationpb.New(2 * time.Hour)
	r1 := PBDurEvent{
		ID:       1,
		Duration: dur2h,
		Timeout:  *durationpb.New(500 * time.Millisecond),
	}
	r2 := PBDurEvent{
		ID:       2,
		Duration: nil,
		Timeout:  *durationpb.New(150 * time.Millisecond),
	}

	writer.Append(&r1)
	writer.Append(&r2)

	rec := writer.NewRecord()
	defer rec.Release()

	reader, err := NewPBDurEventArrowReader(rec)
	if err != nil {
		t.Fatalf("NewPBDurEventArrowReader: %v", err)
	}

	var got PBDurEvent
	reader.LoadRow(0, &got)
	if got.ID != 1 {
		t.Errorf("row0 ID: got %d, want 1", got.ID)
	}
	if got.Duration == nil || got.Duration.AsDuration() != 2*time.Hour {
		t.Errorf("row0 Duration: got %v, want 2h", got.Duration)
	}
	if got.Timeout.AsDuration() != 500*time.Millisecond {
		t.Errorf("row0 Timeout: got %v, want 500ms", got.Timeout.AsDuration())
	}

	reader.LoadRow(1, &got)
	if got.ID != 2 {
		t.Errorf("row1 ID: got %d, want 2", got.ID)
	}
	if got.Duration != nil {
		t.Errorf("row1 Duration: got %v, want nil", got.Duration)
	}
	if got.Timeout.AsDuration() != 150*time.Millisecond {
		t.Errorf("row1 Timeout: got %v, want 150ms", got.Timeout.AsDuration())
	}
}
`
		runInnerTest(t, tmpDir, testCode, "TestProtobufDurationRoundTrip")
	})

	t.Run("protobuf-timestamp-round-trip", func(t *testing.T) {
		tmpDir := t.TempDir()

		goCode := `package dummy

import "google.golang.org/protobuf/types/known/timestamppb"

type PBTsRecord struct {
	ID        int32
	CreatedAt *timestamppb.Timestamp
	UpdatedAt timestamppb.Timestamp
}
`
		if err := os.WriteFile(filepath.Join(tmpDir, "dummy.go"), []byte(goCode), 0644); err != nil {
			t.Fatalf("write dummy.go: %v", err)
		}
		if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module dummy\n\ngo 1.25.0\n"), 0644); err != nil {
			t.Fatalf("write go.mod: %v", err)
		}

		runCmd(t, tmpDir, "go", "get", "github.com/apache/arrow/go/v18@v18.0.0-20241007013041-ab95a4d25142")
		runCmd(t, tmpDir, "go", "get", "google.golang.org/protobuf/types/known/timestamppb")

		writerOut := filepath.Join(tmpDir, "dummy_arrow_writer.go")
		wg := writergen.NewGenerator([]string{tmpDir}, []string{"PBTsRecord"}, writerOut, false, nil)
		if err := wg.Run(""); err != nil {
			t.Fatalf("writer-gen Run() failed: %v", err)
		}

		readerOut := filepath.Join(tmpDir, "dummy_arrow_reader.go")
		rg := NewGenerator([]string{tmpDir}, []string{"PBTsRecord"}, readerOut, false, nil)
		if err := rg.Run(""); err != nil {
			t.Fatalf("reader-gen Run() failed: %v", err)
		}

		testCode := `package dummy

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestProtobufTimestampRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewPBTsRecordArrowWriter(pool)
	defer writer.Release()

	t1 := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 15, 8, 30, 0, 0, time.UTC)

	created := timestamppb.New(t1)
	r1 := PBTsRecord{
		ID:        1,
		CreatedAt: created,
		UpdatedAt: *timestamppb.New(t2),
	}
	r2 := PBTsRecord{
		ID:        2,
		CreatedAt: nil,
		UpdatedAt: *timestamppb.New(t1),
	}

	writer.Append(&r1)
	writer.Append(&r2)

	rec := writer.NewRecord()
	defer rec.Release()

	reader, err := NewPBTsRecordArrowReader(rec)
	if err != nil {
		t.Fatalf("NewPBTsRecordArrowReader: %v", err)
	}

	var got PBTsRecord
	reader.LoadRow(0, &got)
	if got.ID != 1 {
		t.Errorf("row0 ID: got %d, want 1", got.ID)
	}
	if got.CreatedAt == nil || !got.CreatedAt.AsTime().Equal(t1) {
		t.Errorf("row0 CreatedAt: got %v, want %v", got.CreatedAt, t1)
	}
	if !got.UpdatedAt.AsTime().Equal(t2) {
		t.Errorf("row0 UpdatedAt: got %v, want %v", got.UpdatedAt.AsTime(), t2)
	}

	reader.LoadRow(1, &got)
	if got.ID != 2 {
		t.Errorf("row1 ID: got %d, want 2", got.ID)
	}
	if got.CreatedAt != nil {
		t.Errorf("row1 CreatedAt: got %v, want nil", got.CreatedAt)
	}
	if !got.UpdatedAt.AsTime().Equal(t1) {
		t.Errorf("row1 UpdatedAt: got %v, want %v", got.UpdatedAt.AsTime(), t1)
	}
}
`
		runInnerTest(t, tmpDir, testCode, "TestProtobufTimestampRoundTrip")
	})

	t.Run("list-of-convert-round-trip", func(t *testing.T) {
		goCode := `package dummy

import "time"

type Schedule struct {
	Durations []time.Duration
	Times     []time.Time
}
`
		tmpDir := setupIntegrationTest(t, goCode, []string{"Schedule"})

		testCode := `package dummy

import (
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestListOfConvertRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewScheduleArrowWriter(pool)
	defer writer.Release()

	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	r1 := Schedule{
		Durations: []time.Duration{time.Hour, 2 * time.Minute},
		Times:     []time.Time{t1, t2},
	}
	r2 := Schedule{
		Durations: nil,
		Times:     nil,
	}

	writer.Append(&r1)
	writer.Append(&r2)

	rec := writer.NewRecord()
	defer rec.Release()

	reader, err := NewScheduleArrowReader(rec)
	if err != nil {
		t.Fatalf("NewScheduleArrowReader: %v", err)
	}

	var got Schedule
	reader.LoadRow(0, &got)
	if len(got.Durations) != 2 || got.Durations[0] != time.Hour || got.Durations[1] != 2*time.Minute {
		t.Errorf("row0 Durations: got %v, want [1h 2m0s]", got.Durations)
	}
	if len(got.Times) != 2 || !got.Times[0].Equal(t1) || !got.Times[1].Equal(t2) {
		t.Errorf("row0 Times: got %v, want [%v %v]", got.Times, t1, t2)
	}

	reader.LoadRow(1, &got)
	if got.Durations != nil {
		t.Errorf("row1 Durations: got %v, want nil", got.Durations)
	}
	if got.Times != nil {
		t.Errorf("row1 Times: got %v, want nil", got.Times)
	}
}
`
		runInnerTest(t, tmpDir, testCode, "TestListOfConvertRoundTrip")
	})

	t.Run("text-marshaler-round-trip", func(t *testing.T) {
		goCode := `package dummy

import "net/netip"

type NetRecord struct {
	Addr    netip.Addr
	OptAddr *netip.Addr
}
`
		tmpDir := setupIntegrationTest(t, goCode, []string{"NetRecord"})

		testCode := `package dummy

import (
	"net/netip"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestTextMarshalerRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewNetRecordArrowWriter(pool)
	defer writer.Release()

	addr1 := netip.MustParseAddr("192.168.1.1")
	addr2 := netip.MustParseAddr("10.0.0.1")

	// Row 1: both fields populated
	r1 := NetRecord{Addr: addr1, OptAddr: &addr2}
	writer.Append(&r1)

	// Row 2: pointer nil
	r2 := NetRecord{Addr: addr1, OptAddr: nil}
	writer.Append(&r2)

	// Row 3: zero-value addr
	r3 := NetRecord{}
	writer.Append(&r3)

	rec := writer.NewRecord()
	defer rec.Release()

	reader, err := NewNetRecordArrowReader(rec)
	if err != nil {
		t.Fatalf("NewNetRecordArrowReader: %v", err)
	}

	// Row 1
	var got NetRecord
	reader.LoadRow(0, &got)
	if got.Addr != addr1 {
		t.Errorf("row0 Addr: got %v, want %v", got.Addr, addr1)
	}
	if got.OptAddr == nil || *got.OptAddr != addr2 {
		t.Errorf("row0 OptAddr: got %v, want %v", got.OptAddr, &addr2)
	}

	// Row 1 — reuse into row 2 (test pointer-reuse path)
	reader.LoadRow(1, &got)
	if got.Addr != addr1 {
		t.Errorf("row1 Addr: got %v, want %v", got.Addr, addr1)
	}
	if got.OptAddr != nil {
		t.Errorf("row1 OptAddr: got %v, want nil", got.OptAddr)
	}

	// Row 3 — zero addr
	reader.LoadRow(2, &got)
	zeroAddr := netip.Addr{}
	if got.Addr != zeroAddr {
		t.Errorf("row2 Addr: got %v, want zero", got.Addr)
	}

	// Errors should be empty for valid data
	if errs := reader.Errors(); len(errs) > 0 {
		t.Errorf("unexpected errors: %v", errs)
	}
}
`
		runInnerTest(t, tmpDir, testCode, "TestTextMarshalerRoundTrip")
	})

	t.Run("unmarshal-error-accumulation", func(t *testing.T) {
		goCode := `package dummy

import "net/netip"

type AddrHolder struct {
	Addr netip.Addr
}
`
		tmpDir := setupIntegrationTest(t, goCode, []string{"AddrHolder"})

		testCode := `package dummy

import (
	"net/netip"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestUnmarshalErrorAccumulation(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	// Build a record with an invalid IP address string manually.
	bldr := array.NewRecordBuilder(pool, NewAddrHolderSchema())
	defer bldr.Release()

	addrCol := bldr.Field(0).(*array.StringBuilder)
	addrCol.Append("not-a-valid-ip")  // bad row
	addrCol.Append("192.168.1.1")     // good row

	rec := bldr.NewRecord()
	defer rec.Release()

	reader, err := NewAddrHolderArrowReader(rec)
	if err != nil {
		t.Fatalf("NewAddrHolderArrowReader: %v", err)
	}

	// Load bad row — should accumulate error
	var got AddrHolder
	reader.LoadRow(0, &got)

	errs := reader.Errors()
	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d: %v", len(errs), errs)
	}
	if errs[0].Row != 0 {
		t.Errorf("error Row: got %d, want 0", errs[0].Row)
	}
	if errs[0].Field != "Addr" {
		t.Errorf("error Field: got %q, want %q", errs[0].Field, "Addr")
	}
	// Field should be zeroed on error
	if got.Addr != (netip.Addr{}) {
		t.Errorf("Addr should be zero after error, got %v", got.Addr)
	}

	// Load good row
	reader.ResetErrors()
	reader.LoadRow(1, &got)
	if got.Addr.String() != "192.168.1.1" {
		t.Errorf("row1 Addr: got %v, want 192.168.1.1", got.Addr)
	}
	if len(reader.Errors()) != 0 {
		t.Errorf("expected 0 errors after good row, got %v", reader.Errors())
	}
}
`
		runInnerTest(t, tmpDir, testCode, "TestUnmarshalErrorAccumulation")
	})

	t.Run("list-of-text-marshaler-round-trip", func(t *testing.T) {
		goCode := `package dummy

import "net/netip"

type AddrList struct {
	Addrs []netip.Addr
}
`
		tmpDir := setupIntegrationTest(t, goCode, []string{"AddrList"})

		testCode := `package dummy

import (
	"net/netip"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestListOfTextMarshalerRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewAddrListArrowWriter(pool)
	defer writer.Release()

	addr1 := netip.MustParseAddr("10.0.0.1")
	addr2 := netip.MustParseAddr("10.0.0.2")

	// Row with values
	r1 := AddrList{Addrs: []netip.Addr{addr1, addr2}}
	writer.Append(&r1)

	// Row with nil slice
	r2 := AddrList{Addrs: nil}
	writer.Append(&r2)

	rec := writer.NewRecord()
	defer rec.Release()

	reader, err := NewAddrListArrowReader(rec)
	if err != nil {
		t.Fatalf("NewAddrListArrowReader: %v", err)
	}

	var got AddrList
	reader.LoadRow(0, &got)
	if len(got.Addrs) != 2 {
		t.Fatalf("row0 Addrs len: got %d, want 2", len(got.Addrs))
	}
	if got.Addrs[0] != addr1 {
		t.Errorf("row0 Addrs[0]: got %v, want %v", got.Addrs[0], addr1)
	}
	if got.Addrs[1] != addr2 {
		t.Errorf("row0 Addrs[1]: got %v, want %v", got.Addrs[1], addr2)
	}

	reader.LoadRow(1, &got)
	if got.Addrs != nil {
		t.Errorf("row1 Addrs: got %v, want nil", got.Addrs)
	}

	if len(reader.Errors()) != 0 {
		t.Errorf("unexpected errors: %v", reader.Errors())
	}
}
`
		runInnerTest(t, tmpDir, testCode, "TestListOfTextMarshalerRoundTrip")
	})

	t.Run("stringer-only-skip", func(t *testing.T) {
		// url.URL implements String() but not UnmarshalText — field should be
		// silently skipped in the reader (no column, no error).
		goCode := `package dummy

import "net/url"

type LinkRecord struct {
	Name string
	Link url.URL
}
`
		tmpDir := setupIntegrationTest(t, goCode, []string{"LinkRecord"})

		testCode := `package dummy

import (
	"net/url"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestStringerOnlySkip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewLinkRecordArrowWriter(pool)
	defer writer.Release()

	r1 := LinkRecord{
		Name: "example",
		Link: url.URL{Scheme: "https", Host: "example.com"},
	}
	writer.Append(&r1)

	rec := writer.NewRecord()
	defer rec.Release()

	reader, err := NewLinkRecordArrowReader(rec)
	if err != nil {
		t.Fatalf("NewLinkRecordArrowReader: %v", err)
	}

	var got LinkRecord
	reader.LoadRow(0, &got)

	// Name should round-trip
	if got.Name != "example" {
		t.Errorf("Name: got %q, want %q", got.Name, "example")
	}

	// Link should be zero — reader skips it (no unmarshal inverse)
	if got.Link != (url.URL{}) {
		t.Errorf("Link should be zero (skipped), got %v", got.Link)
	}
}
`
		runInnerTest(t, tmpDir, testCode, "TestStringerOnlySkip")
	})

	t.Run("null-elements-in-convert-list", func(t *testing.T) {
		// Verify that null elements inside a []time.Time list produce the Go
		// zero value (time.Time{}) rather than time.Unix(0,0) which the
		// ConvertBackExpr would yield from a zero int64.
		goCode := `package dummy

import "time"

type EventLog struct {
	Timestamps []time.Time
}
`
		tmpDir := setupIntegrationTest(t, goCode, []string{"EventLog"})

		testCode := `package dummy

import (
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestNullElementsInConvertList(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	// Build an Arrow record manually with a null element inside a list.
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "Timestamps", Type: arrow.ListOf(arrow.FixedWidthTypes.Timestamp_ns), Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(pool, schema)
	defer bldr.Release()

	lb := bldr.Field(0).(*array.ListBuilder)
	vb := lb.ValueBuilder().(*array.TimestampBuilder)

	// Row 0: list with [valid, null, valid]
	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	lb.Append(true)
	vb.Append(arrow.Timestamp(t1.UnixNano()))
	vb.AppendNull()
	vb.Append(arrow.Timestamp(t2.UnixNano()))

	// Row 1: list with [null]
	lb.Append(true)
	vb.AppendNull()

	rec := bldr.NewRecord()
	defer rec.Release()

	reader, err := NewEventLogArrowReader(rec)
	if err != nil {
		t.Fatalf("NewEventLogArrowReader: %v", err)
	}

	var got EventLog

	reader.LoadRow(0, &got)
	if len(got.Timestamps) != 3 {
		t.Fatalf("row0 len: got %d, want 3", len(got.Timestamps))
	}
	if !got.Timestamps[0].Equal(t1) {
		t.Errorf("row0[0]: got %v, want %v", got.Timestamps[0], t1)
	}
	// Null element must be Go zero time, NOT time.Unix(0,0)
	if !got.Timestamps[1].IsZero() {
		t.Errorf("row0[1]: got %v, want zero time (null element)", got.Timestamps[1])
	}
	if !got.Timestamps[2].Equal(t2) {
		t.Errorf("row0[2]: got %v, want %v", got.Timestamps[2], t2)
	}

	reader.LoadRow(1, &got)
	if len(got.Timestamps) != 1 {
		t.Fatalf("row1 len: got %d, want 1", len(got.Timestamps))
	}
	if !got.Timestamps[0].IsZero() {
		t.Errorf("row1[0]: got %v, want zero time (null element)", got.Timestamps[0])
	}
}
`
		runInnerTest(t, tmpDir, testCode, "TestNullElementsInConvertList")
	})

	t.Run("dict-encoded-string-round-trip", func(t *testing.T) {
		goCode := `package dummy

type DictStringStruct struct {
	Name    string
	OptName *string
}
`
		tmpDir := setupIntegrationTest(t, goCode, []string{"DictStringStruct"})

		testCode := `package dummy

import (
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestDictEncodedStringRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	// Build a dictionary-encoded Arrow record manually.
	dictType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int32,
		ValueType: arrow.BinaryTypes.String,
	}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "Name", Type: dictType, Nullable: true},
		{Name: "OptName", Type: dictType, Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(pool, schema)
	defer bldr.Release()

	nameBldr := bldr.Field(0).(*array.BinaryDictionaryBuilder)
	optNameBldr := bldr.Field(1).(*array.BinaryDictionaryBuilder)

	// Row 0: both populated
	nameBldr.AppendString("hello")
	optNameBldr.AppendString("world")

	// Row 1: null pointer, non-null value
	nameBldr.AppendString("foo")
	optNameBldr.AppendNull()

	// Row 2: repeated dict entries (tests dictionary index reuse)
	nameBldr.AppendString("hello")
	optNameBldr.AppendString("world")

	// Row 3: null value type (should produce zero value)
	nameBldr.AppendNull()
	optNameBldr.AppendNull()

	rec := bldr.NewRecord()
	defer rec.Release()

	if rec.NumRows() != 4 {
		t.Fatalf("expected 4 rows, got %d", rec.NumRows())
	}

	reader, err := NewDictStringStructArrowReader(rec)
	if err != nil {
		t.Fatalf("NewDictStringStructArrowReader: %v", err)
	}

	var got DictStringStruct

	// Row 0: both populated
	reader.LoadRow(0, &got)
	if got.Name != "hello" {
		t.Errorf("row0 Name: got %q, want %q", got.Name, "hello")
	}
	if got.OptName == nil || *got.OptName != "world" {
		t.Errorf("row0 OptName: got %v, want \"world\"", got.OptName)
	}

	// Row 1: value present, pointer null
	reader.LoadRow(1, &got)
	if got.Name != "foo" {
		t.Errorf("row1 Name: got %q, want %q", got.Name, "foo")
	}
	if got.OptName != nil {
		t.Errorf("row1 OptName: got %v, want nil", got.OptName)
	}

	// Row 2: repeated dict entries resolve correctly
	reader.LoadRow(2, &got)
	if got.Name != "hello" {
		t.Errorf("row2 Name: got %q, want %q", got.Name, "hello")
	}
	if got.OptName == nil || *got.OptName != "world" {
		t.Errorf("row2 OptName: got %v, want \"world\"", got.OptName)
	}

	// Row 3: null → zero for value type, nil for pointer
	reader.LoadRow(3, &got)
	if got.Name != "" {
		t.Errorf("row3 Name: got %q, want empty", got.Name)
	}
	if got.OptName != nil {
		t.Errorf("row3 OptName: got %v, want nil", got.OptName)
	}
}
`
		runInnerTest(t, tmpDir, testCode, "TestDictEncodedStringRoundTrip")
	})

	t.Run("dict-encoded-binary-round-trip", func(t *testing.T) {
		goCode := `package dummy

type DictBinaryStruct struct {
	Data []byte
}
`
		tmpDir := setupIntegrationTest(t, goCode, []string{"DictBinaryStruct"})

		testCode := `package dummy

import (
	"bytes"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestDictEncodedBinaryRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	dictType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int32,
		ValueType: arrow.BinaryTypes.Binary,
	}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "Data", Type: dictType, Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(pool, schema)
	defer bldr.Release()

	dataBldr := bldr.Field(0).(*array.BinaryDictionaryBuilder)
	dataBldr.Append([]byte{0xDE, 0xAD})
	dataBldr.Append([]byte{0xBE, 0xEF})
	dataBldr.Append([]byte{0xDE, 0xAD}) // repeated dict entry
	dataBldr.AppendNull()

	rec := bldr.NewRecord()
	defer rec.Release()

	if rec.NumRows() != 4 {
		t.Fatalf("expected 4 rows, got %d", rec.NumRows())
	}

	reader, err := NewDictBinaryStructArrowReader(rec)
	if err != nil {
		t.Fatalf("NewDictBinaryStructArrowReader: %v", err)
	}

	var got DictBinaryStruct

	reader.LoadRow(0, &got)
	if !bytes.Equal(got.Data, []byte{0xDE, 0xAD}) {
		t.Errorf("row0 Data: got %x, want DEAD", got.Data)
	}

	reader.LoadRow(1, &got)
	if !bytes.Equal(got.Data, []byte{0xBE, 0xEF}) {
		t.Errorf("row1 Data: got %x, want BEEF", got.Data)
	}

	// Repeated dict entry
	reader.LoadRow(2, &got)
	if !bytes.Equal(got.Data, []byte{0xDE, 0xAD}) {
		t.Errorf("row2 Data: got %x, want DEAD", got.Data)
	}

	// Null → zero value
	reader.LoadRow(3, &got)
	if len(got.Data) != 0 {
		t.Errorf("row3 Data: got %x, want empty", got.Data)
	}
}
`
		runInnerTest(t, tmpDir, testCode, "TestDictEncodedBinaryRoundTrip")
	})

	t.Run("binary-unmarshal-round-trip", func(t *testing.T) {
		// BinVal must live in a separate package so the parser treats it as an
		// external type and falls through to marshal method detection (local
		// struct types take the nested-struct path instead).
		tmpDir := t.TempDir()

		binPkgDir := filepath.Join(tmpDir, "binpkg")
		if err := os.MkdirAll(binPkgDir, 0755); err != nil {
			t.Fatalf("mkdir binpkg: %v", err)
		}

		binValCode := `package binpkg

import "fmt"

// BinVal implements only encoding.BinaryMarshaler/BinaryUnmarshaler (not TextMarshaler).
type BinVal struct {
	Data [4]byte
}

func (b BinVal) MarshalBinary() ([]byte, error) {
	return b.Data[:], nil
}

func (b *BinVal) UnmarshalBinary(data []byte) error {
	if len(data) != 4 {
		return fmt.Errorf("BinVal: expected 4 bytes, got %d", len(data))
	}
	copy(b.Data[:], data)
	return nil
}
`
		if err := os.WriteFile(filepath.Join(binPkgDir, "binval.go"), []byte(binValCode), 0644); err != nil {
			t.Fatalf("write binval.go: %v", err)
		}

		goCode := `package dummy

import "dummy/binpkg"

type BinRecord struct {
	Val    binpkg.BinVal
	OptVal *binpkg.BinVal
}
`
		if err := os.WriteFile(filepath.Join(tmpDir, "dummy.go"), []byte(goCode), 0644); err != nil {
			t.Fatalf("write dummy.go: %v", err)
		}
		if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module dummy\n\ngo 1.25.0\n"), 0644); err != nil {
			t.Fatalf("write go.mod: %v", err)
		}

		runCmd(t, tmpDir, "go", "get", "github.com/apache/arrow/go/v18@v18.0.0-20241007013041-ab95a4d25142")

		writerOut := filepath.Join(tmpDir, "dummy_arrow_writer.go")
		wg := writergen.NewGenerator([]string{tmpDir}, []string{"BinRecord"}, writerOut, false, nil)
		if err := wg.Run(""); err != nil {
			t.Fatalf("writer-gen Run() failed: %v", err)
		}

		readerOut := filepath.Join(tmpDir, "dummy_arrow_reader.go")
		rg := NewGenerator([]string{tmpDir}, []string{"BinRecord"}, readerOut, false, nil)
		if err := rg.Run(""); err != nil {
			t.Fatalf("reader-gen Run() failed: %v", err)
		}

		testCode := `package dummy

import (
	"testing"

	"dummy/binpkg"

	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestBinaryUnmarshalRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewBinRecordArrowWriter(pool)
	defer writer.Release()

	v1 := binpkg.BinVal{Data: [4]byte{0xDE, 0xAD, 0xBE, 0xEF}}
	v2 := binpkg.BinVal{Data: [4]byte{0xCA, 0xFE, 0xBA, 0xBE}}

	// Row 0: both populated
	r0 := BinRecord{Val: v1, OptVal: &v2}
	writer.Append(&r0)

	// Row 1: nil pointer
	r1 := BinRecord{Val: v2, OptVal: nil}
	writer.Append(&r1)

	// Row 2: zero value (writer writes MarshalBinary of zero → 4 zero bytes)
	r2 := BinRecord{}
	writer.Append(&r2)

	rec := writer.NewRecord()
	defer rec.Release()

	reader, err := NewBinRecordArrowReader(rec)
	if err != nil {
		t.Fatalf("NewBinRecordArrowReader: %v", err)
	}

	// Row 0: both populated
	var got BinRecord
	reader.LoadRow(0, &got)
	if got.Val != v1 {
		t.Errorf("row0 Val: got %v, want %v", got.Val, v1)
	}
	if got.OptVal == nil || *got.OptVal != v2 {
		t.Errorf("row0 OptVal: got %v, want %v", got.OptVal, &v2)
	}

	// Row 1: nil pointer
	reader.LoadRow(1, &got)
	if got.Val != v2 {
		t.Errorf("row1 Val: got %v, want %v", got.Val, v2)
	}
	if got.OptVal != nil {
		t.Errorf("row1 OptVal: got %v, want nil", got.OptVal)
	}

	// Row 2: zero value
	reader.LoadRow(2, &got)
	if got.Val != (binpkg.BinVal{}) {
		t.Errorf("row2 Val: got %v, want zero", got.Val)
	}

	if errs := reader.Errors(); len(errs) != 0 {
		t.Errorf("unexpected errors: %v", errs)
	}

	// --- Error accumulation: inject invalid binary payload ---
	bldr := array.NewRecordBuilder(pool, NewBinRecordSchema())
	defer bldr.Release()

	valCol := bldr.Field(0).(*array.BinaryBuilder)
	optValCol := bldr.Field(1).(*array.BinaryBuilder)

	// Row with invalid data (2 bytes instead of 4)
	valCol.Append([]byte{0x01, 0x02})
	optValCol.AppendNull()

	// Row with valid data
	valCol.Append([]byte{0xDE, 0xAD, 0xBE, 0xEF})
	optValCol.AppendNull()

	badRec := bldr.NewRecord()
	defer badRec.Release()

	reader2, err := NewBinRecordArrowReader(badRec)
	if err != nil {
		t.Fatalf("NewBinRecordArrowReader (bad): %v", err)
	}

	var got2 BinRecord
	reader2.LoadRow(0, &got2)

	errs := reader2.Errors()
	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d: %v", len(errs), errs)
	}
	if errs[0].Row != 0 {
		t.Errorf("error Row: got %d, want 0", errs[0].Row)
	}
	if errs[0].Field != "Val" {
		t.Errorf("error Field: got %q, want %q", errs[0].Field, "Val")
	}
	// Field should be zeroed on error
	if got2.Val != (binpkg.BinVal{}) {
		t.Errorf("Val should be zero after error, got %v", got2.Val)
	}

	// Good row after reset
	reader2.ResetErrors()
	reader2.LoadRow(1, &got2)
	if got2.Val != v1 {
		t.Errorf("row1 Val: got %v, want %v", got2.Val, v1)
	}
	if len(reader2.Errors()) != 0 {
		t.Errorf("expected 0 errors after good row, got %v", reader2.Errors())
	}
}
`
		runInnerTest(t, tmpDir, testCode, "TestBinaryUnmarshalRoundTrip")
	})

	t.Run("init-error-paths", func(t *testing.T) {
		goCode := `package dummy

type ErrorPathStruct struct {
	ID   int32
	Name string
}
`
		tmpDir := setupIntegrationTest(t, goCode, []string{"ErrorPathStruct"})

		testCode := `package dummy

import (
	"strings"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestInitErrorPaths(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	t.Run("wrong-type-non-dict-column", func(t *testing.T) {
		// ID expects *array.Int32, give it *array.String
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "ID", Type: arrow.BinaryTypes.String, Nullable: true},
		}, nil)
		bldr := array.NewRecordBuilder(pool, schema)
		bldr.Field(0).(*array.StringBuilder).Append("oops")
		rec := bldr.NewRecord()
		defer rec.Release()
		defer bldr.Release()

		_, err := NewErrorPathStructArrowReader(rec)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), ` + "`" + `column "ID": expected *array.Int32` + "`" + `) {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("wrong-type-dict-candidate-column", func(t *testing.T) {
		// Name expects *array.String or *array.Dictionary, give it *array.Int32
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "Name", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		}, nil)
		bldr := array.NewRecordBuilder(pool, schema)
		bldr.Field(0).(*array.Int32Builder).Append(42)
		rec := bldr.NewRecord()
		defer rec.Release()
		defer bldr.Release()

		_, err := NewErrorPathStructArrowReader(rec)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), ` + "`" + `column "Name": expected *array.String or *array.Dictionary` + "`" + `) {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("dict-values-type-mismatch", func(t *testing.T) {
		// Name expects dict values to be *array.String, give it dict of *array.Binary
		dictType := &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Int32,
			ValueType: arrow.BinaryTypes.Binary,
		}
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "Name", Type: dictType, Nullable: true},
		}, nil)
		bldr := array.NewRecordBuilder(pool, schema)
		bldr.Field(0).(*array.BinaryDictionaryBuilder).Append([]byte{0x01})
		rec := bldr.NewRecord()
		defer rec.Release()
		defer bldr.Release()

		_, err := NewErrorPathStructArrowReader(rec)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), ` + "`" + `dictionary values: expected *array.String` + "`" + `) {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("correct-types-succeed", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "ID", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "Name", Type: arrow.BinaryTypes.String, Nullable: true},
		}, nil)
		bldr := array.NewRecordBuilder(pool, schema)
		bldr.Field(0).(*array.Int32Builder).Append(1)
		bldr.Field(1).(*array.StringBuilder).Append("ok")
		rec := bldr.NewRecord()
		defer rec.Release()
		defer bldr.Release()

		_, err := NewErrorPathStructArrowReader(rec)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
	})
}
`
		runInnerTest(t, tmpDir, testCode, "TestInitErrorPaths")
	})

	t.Run("embedded-struct-round-trip", func(t *testing.T) {
		goCode := `package dummy

type Base struct {
	ID        int32
	CreatedAt string
}

type Device struct {
	Base
	Name string
}
`
		tmpDir := setupIntegrationTest(t, goCode, []string{"Device"})

		testCode := `package dummy

import (
	"testing"

	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestEmbeddedStructRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewDeviceArrowWriter(pool)
	defer writer.Release()

	// Row 0: all populated
	r0 := Device{
		Base: Base{ID: 1, CreatedAt: "2026-01-01"},
		Name: "sensor-a",
	}
	writer.Append(&r0)

	// Row 1: zero-value embedded fields
	r1 := Device{
		Base: Base{ID: 2},
		Name: "",
	}
	writer.Append(&r1)

	rec := writer.NewRecord()
	defer rec.Release()

	// Verify flat schema (3 columns, not a nested struct)
	if rec.Schema().NumFields() != 3 {
		t.Fatalf("expected 3 flat columns, got %d", rec.Schema().NumFields())
	}

	reader, err := NewDeviceArrowReader(rec)
	if err != nil {
		t.Fatalf("NewDeviceArrowReader: %v", err)
	}

	// Row 0: all populated
	var got Device
	reader.LoadRow(0, &got)
	if got.ID != 1 {
		t.Errorf("row0 ID (promoted): got %d, want 1", got.ID)
	}
	if got.Base.ID != 1 {
		t.Errorf("row0 Base.ID (explicit): got %d, want 1", got.Base.ID)
	}
	if got.CreatedAt != "2026-01-01" {
		t.Errorf("row0 CreatedAt: got %q, want %q", got.CreatedAt, "2026-01-01")
	}
	if got.Base.CreatedAt != "2026-01-01" {
		t.Errorf("row0 Base.CreatedAt: got %q, want %q", got.Base.CreatedAt, "2026-01-01")
	}
	if got.Name != "sensor-a" {
		t.Errorf("row0 Name: got %q, want %q", got.Name, "sensor-a")
	}

	// Row 1: zero-value embedded fields
	reader.LoadRow(1, &got)
	if got.ID != 2 {
		t.Errorf("row1 ID: got %d, want 2", got.ID)
	}
	if got.CreatedAt != "" {
		t.Errorf("row1 CreatedAt: got %q, want empty", got.CreatedAt)
	}
	if got.Name != "" {
		t.Errorf("row1 Name: got %q, want empty", got.Name)
	}
}
`
		runInnerTest(t, tmpDir, testCode, "TestEmbeddedStructRoundTrip")
	})

	t.Run("missing-columns", func(t *testing.T) {
		goCode := `package dummy

type FullStruct struct {
	ID    int32
	Name  string
	Score float64
}
`
		tmpDir := setupIntegrationTest(t, goCode, []string{"FullStruct"})

		testCode := `package dummy

import (
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestMissingColumns(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	// Build a record missing the "Score" column.
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "ID", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "Name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(pool, schema)
	defer bldr.Release()

	bldr.Field(0).(*array.Int32Builder).Append(42)
	bldr.Field(1).(*array.StringBuilder).Append("hello")

	rec := bldr.NewRecord()
	defer rec.Release()

	// Init must succeed despite missing column.
	reader, err := NewFullStructArrowReader(rec)
	if err != nil {
		t.Fatalf("NewFullStructArrowReader: %v", err)
	}

	// Load into zero-initialized struct: present fields populated, missing field stays zero.
	var got FullStruct
	reader.LoadRow(0, &got)
	if got.ID != 42 {
		t.Errorf("ID: got %d, want 42", got.ID)
	}
	if got.Name != "hello" {
		t.Errorf("Name: got %q, want %q", got.Name, "hello")
	}
	if got.Score != 0.0 {
		t.Errorf("Score: got %f, want 0 (zero-init)", got.Score)
	}

	// Key assertion: "untouched" semantics.
	// Pre-populate Score with a sentinel value. LoadRow must NOT overwrite it
	// because the column is missing (skip), unlike a null column (which writes zero).
	got.Score = 99.9
	reader.LoadRow(0, &got)
	if got.ID != 42 {
		t.Errorf("ID after reload: got %d, want 42", got.ID)
	}
	if got.Score != 99.9 {
		t.Errorf("Score should be untouched (99.9), got %f — missing column must skip, not zero", got.Score)
	}
}
`
		runInnerTest(t, tmpDir, testCode, "TestMissingColumns")
	})

	// ---- Cross-package qualification bugs ----
	//
	// These tests reproduce compilation failures in generated reader code when
	// structs are defined in one package but the reader is generated into a
	// different output package (e.g. -n outpkg). Three bugs are exercised:
	//
	//   Bug 1: GoType for named primitives (enums) and container GoType strings
	//          are never qualified (e.g. Status instead of pkga.Status, make([]Inner)
	//          instead of make([]pkgb.Inner)).
	//   Bug 2: Null guard for pointer-to-struct slice elements emits a value
	//          literal (Inner{}) instead of a pointer literal (&Inner{}).
	//   Bug 3: LoadRow for pointer-to-struct slice elements takes the address of
	//          an already-pointer element (&out.Items[j] → **Inner).

	t.Run("cross-package-named-types-and-ptr-slices", func(t *testing.T) {
		// Single module with three sub-packages:
		//   pkga/ — Status enum + Outer struct
		//   pkgb/ — Inner struct
		//   outpkg/ — generated writer+reader + test harness
		//
		// The reader is generated with outPkgNameOverride="outpkg", which differs
		// from both pkga and pkgb, forcing package qualifiers on all type references.

		tmpDir := t.TempDir()

		pkgADir := filepath.Join(tmpDir, "pkga")
		pkgBDir := filepath.Join(tmpDir, "pkgb")
		outDir := filepath.Join(tmpDir, "outpkg")

		if err := os.MkdirAll(pkgADir, 0755); err != nil {
			t.Fatalf("mkdir pkga: %v", err)
		}
		if err := os.MkdirAll(pkgBDir, 0755); err != nil {
			t.Fatalf("mkdir pkgb: %v", err)
		}
		if err := os.MkdirAll(outDir, 0755); err != nil {
			t.Fatalf("mkdir outpkg: %v", err)
		}

		// Root go.mod — single module containing all three packages.
		modContent := "module testmod\n\ngo 1.25.0\n"
		if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte(modContent), 0644); err != nil {
			t.Fatalf("write go.mod: %v", err)
		}

		// pkgb: Inner struct (referenced cross-package from pkga).
		pkgBCode := `package pkgb

type Inner struct {
	X int32
	Y string
}
`
		if err := os.WriteFile(filepath.Join(pkgBDir, "types.go"), []byte(pkgBCode), 0644); err != nil {
			t.Fatalf("write pkgb: %v", err)
		}

		// pkga: Status enum (named primitive) and Outer struct.
		// Outer has:
		//   - Status  Status          → named primitive from same pkg (GoType qualification bug)
		//   - Loc     pkgb.Inner      → struct from other pkg (value; make GoType bug)
		//   - Items   []pkgb.Inner    → slice of value-structs (make GoType bug)
		//   - History []*pkgb.Inner   → slice of pointer-structs (make GoType + null guard + LoadRow bugs)
		pkgACode := `package pkga

import "testmod/pkgb"

type Status int32

type Outer struct {
	ID      int32
	Status  Status
	Loc     pkgb.Inner
	Items   []pkgb.Inner
	History []*pkgb.Inner
}
`
		if err := os.WriteFile(filepath.Join(pkgADir, "types.go"), []byte(pkgACode), 0644); err != nil {
			t.Fatalf("write pkga: %v", err)
		}

		// Get arrow dependency.
		runCmd(t, tmpDir, "go", "get", "github.com/apache/arrow/go/v18@v18.0.0-20241007013041-ab95a4d25142")

		// Generate writer into outpkg.
		writerOut := filepath.Join(outDir, "outer_arrow_writer.go")
		wg := writergen.NewGenerator([]string{pkgADir, pkgBDir}, []string{"Outer"}, writerOut, false, nil)
		if err := wg.Run("outpkg"); err != nil {
			t.Fatalf("writer-gen failed: %v", err)
		}

		// Generate reader into outpkg.
		readerOut := filepath.Join(outDir, "outer_arrow_reader.go")
		rg := NewGenerator([]string{pkgADir, pkgBDir}, []string{"Outer"}, readerOut, false, nil)
		if err := rg.Run("outpkg"); err != nil {
			t.Fatalf("reader-gen failed: %v", err)
		}

		// Dump generated reader for debugging on failure.
		if content, err := os.ReadFile(readerOut); err == nil {
			t.Logf("Generated reader:\n%s", content)
		}

		testCode := `package outpkg

import (
	"testing"

	"testmod/pkga"
	"testmod/pkgb"

	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestCrossPackageRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	writer := NewOuterArrowWriter(pool)
	defer writer.Release()

	writer.Append(&pkga.Outer{
		ID:      1,
		Status:  pkga.Status(42),
		Loc:     pkgb.Inner{X: 10, Y: "hello"},
		Items:   []pkgb.Inner{{X: 20, Y: "a"}, {X: 30, Y: "b"}},
		History: []*pkgb.Inner{{X: 1, Y: "x"}, {X: 2, Y: "y"}},
	})

	rec := writer.NewRecord()
	defer rec.Release()

	reader, err := NewOuterArrowReader(rec)
	if err != nil {
		t.Fatal(err)
	}

	var got pkga.Outer
	got.Items = make([]pkgb.Inner, 0, 4)
	got.History = make([]*pkgb.Inner, 0, 4)
	reader.LoadRow(0, &got)

	if got.Status != pkga.Status(42) {
		t.Errorf("Status: got %d, want 42", got.Status)
	}
	if got.Loc.X != 10 || got.Loc.Y != "hello" {
		t.Errorf("Loc: got %+v", got.Loc)
	}
	if len(got.Items) != 2 {
		t.Fatalf("Items: got %d elements, want 2", len(got.Items))
	}
	if got.Items[0].X != 20 || got.Items[1].X != 30 {
		t.Errorf("Items: got %+v", got.Items)
	}
	if len(got.History) != 2 {
		t.Fatalf("History: got %d elements, want 2", len(got.History))
	}
	if got.History[0] == nil || got.History[0].X != 1 || got.History[0].Y != "x" {
		t.Errorf("History[0]: got %+v", got.History[0])
	}
	if got.History[1] == nil || got.History[1].X != 2 || got.History[1].Y != "y" {
		t.Errorf("History[1]: got %+v", got.History[1])
	}
}
`
		if err := os.WriteFile(filepath.Join(outDir, "outer_test.go"), []byte(testCode), 0644); err != nil {
			t.Fatalf("write test: %v", err)
		}

		runCmd(t, tmpDir, "go", "mod", "tidy")
		runCmd(t, tmpDir, "go", "test", "-v", "-run", "TestCrossPackageRoundTrip", "./outpkg/")

		if false {
			tarball(t, "/tmp/arrow-reader-gen-cross-pkg.tar.gz", tmpDir)
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
