package conformance

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// The tests in this file run the generators themselves, at test time, on
// the small fixture packages under testdata. They exist for the shapes the
// checked-in fixtures cannot hold: shapes the generators refuse with an
// error, shapes they skip with a warning, and shapes that once produced
// code that did not compile or failed at runtime and must keep compiling
// and running. Each case copies its fixture into a scratch package under
// testdata, runs a generator into it, compiles the result with go vet, and
// where the fixture ships a probe test (writer_test.go or reader_test.go)
// runs that too.

// generatorCase describes one fixture package and what the generators must
// do with it.
type generatorCase struct {
	name    string // the directory under testdata, which is also the package name
	structs string // the --structs argument

	writerWarnings []string            // substrings the writer must print
	writerError    string              // substring the writer must fail with, in which case nothing else runs
	writerVet      string              // substring go vet must report on the writer's output, empty means it must compile
	writerProbe    []string            // substrings the fixture's writer_test.go must print
	writerProbeOK  bool                // whether writer_test.go must pass
	fields         map[string][]string // struct name to the columns its generated schema must hold

	readerWarnings []string
	readerError    string // substring the reader must fail with, in which case nothing else runs
	readerVet      string
	readerProbe    []string
	readerProbeOK  bool
}

var generatorCases = []generatorCase{
	{
		name:    "emptystruct",
		structs: "EmptyRecord",
		// A struct with no fields has no Arrow representation and Parquet
		// refuses it, so generation stops before any file is written.
		writerError: "struct Empty has no field the generators can map",
	},
	{
		name:           "zeroarray",
		structs:        "ZeroArray",
		fields:         map[string][]string{"ZeroArray": {"K"}},
		writerWarnings: []string{"Skipping field Z in ZeroArray: zero-length arrays are not supported, Arrow rejects a fixed-size list of size 0"},
		writerProbeOK:  true,
	},
	{
		name:    "fixedptr",
		structs: "FixedPtr",
		fields:  map[string][]string{"FixedPtr": {"P"}},
		// The element field is nullable, so a nil element is an honest null
		// that survives Parquet, and the reader has a branch for it.
		writerProbeOK: true,
	},
	{
		name:    "ptrelems",
		structs: "PtrElems",
		fields:  map[string][]string{"PtrElems": {"Ints", "Strs", "Times"}},
	},
	{
		name:          "ptrunmarshal",
		structs:       "PtrUnmarshal",
		fields:        map[string][]string{"PtrUnmarshal": {"Items"}},
		readerProbeOK: true,
	},
	{
		name:    "listofmaps",
		structs: "ListOfMaps",
		fields:  map[string][]string{"ListOfMaps": {"Maps"}},
	},
	{
		name:    "aliases",
		structs: "AliasRecord",
		fields:  map[string][]string{"AliasRecord": {"L", "T", "P"}},
	},
	{
		name:    "recursive",
		structs: "Node",
		// The schema constructor would call itself to describe Children, so
		// generation stops with the cycle spelled out.
		writerError: "struct Node refers to itself through Node -> Node, and an Arrow schema cannot be recursive",
	},
	{
		name:    "marshalonly",
		structs: "MarshalOnlyRecord",
		fields:  map[string][]string{"MarshalOnlyRecord": {"M"}},
		// MarshalText with no UnmarshalText is write-only, and a struct
		// with nothing left to read has no reader.
		readerWarnings: []string{"field MarshalOnlyRecord.M uses MarshalText() with no unmarshal inverse; skipping in reader"},
		readerError:    "struct MarshalOnlyRecord has no field the generators can map",
	},
	{
		name:    "mapkeys",
		structs: "ArrayKey,PtrKey,TextKey,TimeKey,StructKey",
		// Every map keyed by anything but a basic type is skipped with a
		// warning, and the first struct left with no field stops generation.
		writerWarnings: []string{
			"Skipping field M in ArrayKey: map key type [2]byte is not supported, a key must be a basic type",
			"Skipping field M in PtrKey: map key type *string is not supported, a key must be a basic type",
			"Skipping field M in TextKey: map key type netip.Addr is not supported, a key must be a basic type",
			"Skipping field M in TimeKey: map key type time.Time is not supported, a key must be a basic type",
			"Skipping field M in StructKey: map key type Key is not supported, a key must be a basic type",
		},
		writerError: "struct ArrayKey has no field the generators can map",
	},
	{
		name:    "genericnested",
		structs: "NestedGeneric,NamedSliceOfGeneric",
		// The inner instantiation is expanded through the type checker like
		// the outer one, with every field, and derives its own name.
		fields: map[string][]string{"Pair_String_List_Item": {"Key", "Value"}, "List_Item": {"Items"}},
	},
	{
		name:    "genericexternal",
		structs: "ExternalArgs",
	},
	{
		name:    "genericcollision",
		structs: "Colliding",
		// The two instantiations derive names that carry the inner
		// arguments, so neither shadows the other and the inner Pair keeps
		// its fields.
		fields: map[string][]string{
			"Colliding":                    {"A", "B"},
			"Pair_String_Pair_Int32_Int32": {"Key", "Value"},
			"Pair_Int32_Int32":             {"Key", "Value"},
			"Pair_String_String":           {"Key", "Value"},
		},
	},
	{
		name:    "genericsliceargs",
		structs: "SliceArg",
		// A slice type argument derives an identifier the formatter accepts.
		fields: map[string][]string{"SliceArg": {"P"}, "Pair_SliceOfInt32_Int32": {"Key", "Value"}},
	},
	{
		name:    "enumslice",
		structs: "EnumSlice",
		fields:  map[string][]string{"EnumSlice": {"Levels", "Types"}},
		// The writer emits String() per element into a list of names. The
		// reader drops both fields, and a struct with nothing left to read
		// has no reader.
		readerWarnings: []string{
			"field EnumSlice.Levels uses String() with no unmarshal inverse; skipping in reader",
			"field EnumSlice.Types uses String() with no unmarshal inverse; skipping in reader",
		},
		readerError: "struct EnumSlice has no field the generators can map",
	},
	{
		name:           "rejected",
		structs:        "Rejected",
		fields:         map[string][]string{"Rejected": {"Keep", "Keep2"}},
		writerWarnings: []string{"pointer to pointer to struct", "pointer to fixed-size array", "map key type Key is not supported"},
		readerWarnings: []string{"pointer to pointer to struct", "pointer to fixed-size array", "map key type Key is not supported"},
	},
}

func TestGeneratorCases(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the generators and the compiler on every fixture under testdata")
	}
	for _, c := range generatorCases {
		t.Run(c.name, c.run)
	}
}

func (c generatorCase) run(t *testing.T) {
	t.Parallel()

	wdir := copyFixture(t, c.name, "writer")
	out, err := runGenerator(t, "arrow-writer-gen", wdir, c.structs, c.name)
	for _, want := range c.writerWarnings {
		if !strings.Contains(out, want) {
			t.Errorf("writer output lacks %q:\n%s", want, out)
		}
	}
	if c.writerError != "" {
		if err == nil {
			t.Fatalf("writer succeeded, expected it to fail with %q\n%s", c.writerError, out)
		}
		if !strings.Contains(out, c.writerError) {
			t.Fatalf("writer output lacks %q:\n%s", c.writerError, out)
		}
		return
	}
	if err != nil {
		t.Fatalf("writer failed: %v\n%s", err, out)
	}
	for structName, want := range c.fields {
		got := schemaColumns(t, filepath.Join(wdir, c.name+"-arrow-writer.go"), structName)
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("%s columns (-want +got):\n%s", structName, diff)
		}
	}
	checkVet(t, "writer", wdir, c.writerVet)
	if c.writerVet == "" && hasFile(wdir, "writer_test.go") {
		checkProbe(t, "writer", wdir, c.writerProbe, c.writerProbeOK)
	}

	rdir := copyFixture(t, c.name, "reader")
	out, err = runGenerator(t, "arrow-reader-gen", rdir, c.structs, c.name)
	for _, want := range c.readerWarnings {
		if !strings.Contains(out, want) {
			t.Errorf("reader output lacks %q:\n%s", want, out)
		}
	}
	if c.readerError != "" {
		if err == nil {
			t.Fatalf("reader succeeded, expected it to fail with %q\n%s", c.readerError, out)
		}
		if !strings.Contains(out, c.readerError) {
			t.Fatalf("reader output lacks %q:\n%s", c.readerError, out)
		}
		return
	}
	if err != nil {
		t.Fatalf("reader failed: %v\n%s", err, out)
	}
	checkVet(t, "reader", rdir, c.readerVet)
	if c.readerVet == "" && hasFile(rdir, "reader_test.go") {
		checkProbe(t, "reader", rdir, c.readerProbe, c.readerProbeOK)
	}
}

// TestTwoReaderFilesInOnePackage generates readers for two structs in two
// invocations into the same package, the layout a package gets when it
// keeps one go:generate line per record group. The second invocation finds
// ReadError declared by the first and does not redeclare it, so the
// package compiles.
func TestTwoReaderFilesInOnePackage(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the generators and the compiler")
	}
	dir := copyFixture(t, "tworeaders", "reader")
	for _, s := range []string{"First", "Second"} {
		out := filepath.Join(dir, strings.ToLower(s)+"-arrow-reader.go")
		cmd := exec.Command("go", "run", "go.resystems.io/eddt/cmd/arrow-reader-gen", "-p", dir, "-s", s, "-o", out, "-n", "tworeaders")
		if combined, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("reader for %s failed: %v\n%s", s, err, combined)
		}
	}
	checkVet(t, "reader", dir, "")
}

// TestUnexportedFieldsFollowTheOutputPackage shows an unexported field is a
// column when the generated code lands in the struct's own package and is
// dropped, with a warning, when it lands anywhere else.
func TestUnexportedFieldsFollowTheOutputPackage(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the generators")
	}
	fixture, err := filepath.Abs(filepath.Join("testdata", "unexported"))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		pkgName string
		want    []string
		warning string
	}{
		{"unexported", []string{"Exported", "hidden"}, ""},
		{"other", []string{"Exported"}, `Skipping unexported field hidden in Record (inaccessible from output package "other")`},
	} {
		t.Run(tc.pkgName, func(t *testing.T) {
			out := filepath.Join(t.TempDir(), "record-arrow-writer.go")
			cmd := exec.Command("go", "run", "go.resystems.io/eddt/cmd/arrow-writer-gen", "-p", fixture, "-s", "Record", "-o", out, "-n", tc.pkgName)
			combined, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("writer failed: %v\n%s", err, combined)
			}
			if tc.warning != "" && !strings.Contains(string(combined), tc.warning) {
				t.Errorf("writer output lacks %q:\n%s", tc.warning, combined)
			}
			if diff := cmp.Diff(tc.want, schemaColumns(t, out, "Record")); diff != "" {
				t.Errorf("Record columns (-want +got):\n%s", diff)
			}
		})
	}
}

// TestSameStructNameInTwoPackagesIsRejected checks the one collision the
// generator detects up front: two loaded packages declaring a struct with
// the same name stop generation with an error instead of emitting two
// helpers with the same name.
func TestSameStructNameInTwoPackagesIsRejected(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the generators")
	}
	fixture, err := filepath.Abs(filepath.Join("testdata", "collision"))
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "outer-arrow-writer.go")
	cmd := exec.Command("go", "run", "go.resystems.io/eddt/cmd/arrow-writer-gen", "-p", fixture, "-p", filepath.Join(fixture, "other"), "-s", "Outer", "-o", out, "-n", "collision")
	combined, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("writer accepted two packages declaring Inner:\n%s", combined)
	}
	if want := "generated helper function names would collide"; !strings.Contains(string(combined), want) {
		t.Errorf("writer output lacks %q:\n%s", want, combined)
	}
}

// TestSkippedShapesWarnPerField runs the writer on this package's own
// SkippedRecord and pins what the documentation calls "rejected safely" for
// interface{} and "silently skipped" for func, chan and uintptr: every one
// of them is a per-field skip with a warning on stderr and exit code zero,
// interfaces included. Nothing is rejected outright and nothing is silent.
func TestSkippedShapesWarnPerField(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the generators")
	}
	out, combined := generateFromThisPackage(t, "arrow-writer-gen", "SkippedRecord")
	for _, want := range []string{
		"Skipping field Callback in SkippedRecord: unsupported AST expression type: *ast.FuncType",
		"Skipping field Ch in SkippedRecord: unsupported AST expression type: *ast.ChanType",
		"Skipping field Addr in SkippedRecord: unsupported primitive type: uintptr",
		"Skipping field C128 in SkippedRecord: unsupported primitive type: complex128",
		"Skipping field Any in SkippedRecord: unsupported primitive type: any",
		"Skipping field Iface in SkippedRecord: unsupported AST expression type: *ast.InterfaceType",
		"Skipping field Shape in SkippedRecord: unsupported primitive type: Shape",
		"Skipping field Stringer in SkippedRecord: external type fmt.Stringer does not implement TextMarshaler, Stringer, or BinaryMarshaler",
		"Skipping field Err in SkippedRecord: unsupported primitive type: error",
		"Skipping field Anon in SkippedRecord: unsupported AST expression type: *ast.StructType",
		"Skipping field AnyList in SkippedRecord: slice element unsupported primitive type: any",
		"Skipping field AnyMap in SkippedRecord: map value unsupported primitive type: any",
	} {
		if !strings.Contains(combined, want) {
			t.Errorf("writer output lacks %q", want)
		}
	}
	if diff := cmp.Diff([]string{"Keep", "Keep2"}, schemaColumns(t, out, "SkippedRecord")); diff != "" {
		t.Errorf("SkippedRecord columns (-want +got):\n%s", diff)
	}
}

// TestNestedEmbeddingWarns pins the loud half of
// TestNestedEmbeddingDropsInnerLevel: the two fields lost to the second
// level of embedding are named in a warning.
func TestNestedEmbeddingWarns(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the generators")
	}
	out, combined := generateFromThisPackage(t, "arrow-writer-gen", "NestedEmbedRecord")
	if want := "Skipping the fields EmbedBase promotes into EmbedMid: only one level of embedding is flattened"; !strings.Contains(combined, want) {
		t.Fatalf("writer output lacks %q:\n%s", want, combined)
	}
	if diff := cmp.Diff([]string{"MidField", "Top"}, schemaColumns(t, out, "NestedEmbedRecord")); diff != "" {
		t.Errorf("NestedEmbedRecord columns (-want +got):\n%s", diff)
	}
}

// generateFromThisPackage runs a generator over the conformance package
// itself, writing into a temporary file, and returns the output path and
// the generator's combined output.
func generateFromThisPackage(t *testing.T, tool, structs string) (string, string) {
	t.Helper()
	pkgDir, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "probe-arrow-gen.go")
	cmd := exec.Command("go", "run", "go.resystems.io/eddt/cmd/"+tool, "-p", pkgDir, "-s", structs, "-o", out, "-n", "conformance", "-v")
	combined, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s failed: %v\n%s", tool, err, combined)
	}
	return out, string(combined)
}

// copyFixture copies the fixture package testdata/<name> into a fresh
// scratch directory under testdata, taking every non-test Go file and the
// probe test for the given role, and returns the scratch directory. It
// lives under testdata so the go tool builds it inside this module without
// ever matching it from a ./... pattern.
func copyFixture(t *testing.T, name, role string) string {
	t.Helper()
	src := filepath.Join("testdata", name)
	dir, err := os.MkdirTemp("testdata", "gen-"+name+"-"+role+"-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	abs, err := filepath.Abs(dir)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") {
			continue
		}
		if strings.HasSuffix(e.Name(), "_test.go") && e.Name() != role+"_test.go" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(abs, e.Name()), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return abs
}

// runGenerator runs one generator over dir, writing <pkgName>-arrow-writer.go
// or <pkgName>-arrow-reader.go into it, and returns the combined output.
func runGenerator(t *testing.T, tool, dir, structs, pkgName string) (string, error) {
	t.Helper()
	kind := strings.TrimSuffix(strings.TrimPrefix(tool, "arrow-"), "-gen")
	out := filepath.Join(dir, pkgName+"-arrow-"+kind+".go")
	cmd := exec.Command("go", "run", "go.resystems.io/eddt/cmd/"+tool, "-p", dir, "-s", structs, "-o", out, "-n", pkgName, "-v")
	combined, err := cmd.CombinedOutput()
	return string(combined), err
}

// checkVet compiles the scratch package. With want empty it must compile,
// otherwise the compiler's output must contain want.
func checkVet(t *testing.T, role, dir, want string) {
	t.Helper()
	combined, err := exec.Command("go", "vet", dir).CombinedOutput()
	switch {
	case want == "" && err != nil:
		t.Errorf("%s output does not compile: %v\n%s", role, err, combined)
	case want != "" && err == nil:
		t.Errorf("%s output compiles, expected the compiler to report %q", role, want)
	case want != "" && !strings.Contains(string(combined), want):
		t.Errorf("%s compile output lacks %q:\n%s", role, want, combined)
	}
}

// checkProbe runs the probe test copied into the scratch package.
func checkProbe(t *testing.T, role, dir string, want []string, mustPass bool) {
	t.Helper()
	combined, err := exec.Command("go", "test", "-count=1", "-run", ".", "-v", dir).CombinedOutput()
	if mustPass && err != nil {
		t.Errorf("%s probe failed: %v\n%s", role, err, combined)
	}
	if !mustPass && err == nil {
		t.Errorf("%s probe passed, expected it to fail with %q\n%s", role, want, combined)
	}
	for _, w := range want {
		if !strings.Contains(string(combined), w) {
			t.Errorf("%s probe output lacks %q:\n%s", role, w, combined)
		}
	}
}

func hasFile(dir, name string) bool {
	_, err := os.Stat(filepath.Join(dir, name))
	return err == nil
}

var columnLine = regexp.MustCompile(`\{Name: "([^"]+)"`)

// schemaColumns returns the column names in the New<structName>Schema
// function of a generated writer file, in order.
func schemaColumns(t *testing.T, path, structName string) []string {
	t.Helper()
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	fn := regexp.MustCompile(`(?s)func New` + structName + `Schema\(\) \*arrow\.Schema \{.*?\n\}`)
	body := fn.Find(src)
	if body == nil {
		t.Fatalf("no New%sSchema in %s", structName, path)
	}
	cols := []string{}
	for _, m := range columnLine.FindAllSubmatch(body, -1) {
		cols = append(cols, string(m[1]))
	}
	return cols
}
