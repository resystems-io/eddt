package gencommon

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const fixtureGuardsCode = `package model

import (
	"net/netip"
	"time"
)

type Key struct {
	A int64
}

type Guarded struct {
	Keep      string
	Zero      [0]int32
	ArrayKey  map[[2]byte]int64
	PtrKey    map[*string]int64
	TextKey   map[netip.Addr]int64
	TimeKey   map[time.Time]int64
	StructKey map[Key]int64
	Keep2     int64
}
`

// TestParse_SkipsShapesTheOutputCannotCarry checks that a zero-length array
// and the map keys the generated code cannot handle are skipped with a
// warning, and that the fields around them survive.
func TestParse_SkipsShapesTheOutputCannotCarry(t *testing.T) {
	modDir := writeModelFixture(t, fixtureGuardsCode)
	var warnings strings.Builder
	SetWarnOutput(&warnings)
	t.Cleanup(func() { SetWarnOutput(os.Stderr) })

	_, _, structs, err := Parse([]string{modDir}, []string{"Guarded"}, false)
	if err != nil {
		t.Fatalf("Parse() returned error: %v", err)
	}
	var guarded *StructInfo
	for i := range structs {
		if structs[i].Name == "Guarded" {
			guarded = &structs[i]
		}
	}
	if guarded == nil {
		t.Fatal("Guarded not found in Parse() results")
	}
	var names []string
	for _, f := range guarded.Fields {
		names = append(names, f.Name)
	}
	if strings.Join(names, ",") != "Keep,Keep2" {
		t.Errorf("Guarded fields = %v, want Keep and Keep2", names)
	}
	for _, want := range []string{
		"Skipping field Zero in Guarded: zero-length arrays are not supported",
		"Skipping field ArrayKey in Guarded: map key type [2]byte is not supported",
		"Skipping field PtrKey in Guarded: map key type *string is not supported",
		"Skipping field TextKey in Guarded: map key type netip.Addr is not supported",
		"Skipping field TimeKey in Guarded: map key type time.Time is not supported",
		"Skipping field StructKey in Guarded: map key type Key is not supported",
	} {
		if !strings.Contains(warnings.String(), want) {
			t.Errorf("warnings lack %q:\n%s", want, warnings.String())
		}
	}
}

const fixtureRecursiveCode = `package model

type Node struct {
	Name     string
	Children []*Node
}

type Tree struct {
	Root *Node
}
`

// TestParse_RejectsRecursiveStructs checks that a struct referring to itself
// stops generation instead of producing a schema constructor that never
// returns.
func TestParse_RejectsRecursiveStructs(t *testing.T) {
	modDir := writeModelFixture(t, fixtureRecursiveCode)
	_, _, _, err := Parse([]string{modDir}, []string{"Tree"}, false)
	if err == nil {
		t.Fatal("Parse() accepted a self-referential struct")
	}
	if !strings.Contains(err.Error(), "struct Node refers to itself through Tree -> Node -> Node") {
		t.Errorf("error = %q, want the cycle spelled out", err)
	}
}

const fixtureEmptyCode = `package model

type Empty struct{}

type Holder struct {
	ID string
	E  Empty
}
`

// TestCheckEmpty_RejectsEmptyStructs checks that a struct with no fields is
// reported before generation, since Parquet cannot store it and the reader
// for it does not compile. Parse itself still returns it, so resolution can
// be inspected.
func TestCheckEmpty_RejectsEmptyStructs(t *testing.T) {
	modDir := writeModelFixture(t, fixtureEmptyCode)
	_, _, structs, err := Parse([]string{modDir}, []string{"Holder"}, false)
	if err != nil {
		t.Fatalf("Parse() returned error: %v", err)
	}
	err = CheckEmpty(structs)
	if err == nil {
		t.Fatal("CheckEmpty accepted a struct with no fields")
	}
	if !strings.Contains(err.Error(), "struct Empty has no field the generators can map") {
		t.Errorf("error = %q", err)
	}
}

const fixtureMarshalOnlyCode = `package model

import "example.com/guards/model/other"

type Record struct {
	M  other.MarshalOnly
	PM *other.MarshalOnly
}
`

const fixtureMarshalOnlyOther = `package other

type MarshalOnly struct {
	V string
}

func (m MarshalOnly) MarshalText() ([]byte, error) {
	return []byte(m.V), nil
}
`

// TestParse_MarshalWithoutUnmarshalHasNoInverse checks that a type with
// MarshalText and no UnmarshalText is recorded as write-only, so the reader
// skips it instead of calling a method the type does not have.
func TestParse_MarshalWithoutUnmarshalHasNoInverse(t *testing.T) {
	modDir := writeModelFixture(t, fixtureMarshalOnlyCode)
	otherDir := filepath.Join(modDir, "other")
	if err := os.MkdirAll(otherDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(otherDir, "other.go"), []byte(fixtureMarshalOnlyOther), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(modDir, "..", "go.mod"), []byte("module example.com/guards\n\ngo 1.25.0\n"), 0644); err != nil {
		t.Fatal(err)
	}

	_, _, structs, err := Parse([]string{modDir}, []string{"Record"}, false)
	if err != nil {
		t.Fatalf("Parse() returned error: %v", err)
	}
	for _, f := range structs[0].Fields {
		if f.MarshalMethod != "MarshalText" {
			t.Errorf("%s: MarshalMethod = %q, want MarshalText", f.Name, f.MarshalMethod)
		}
		if f.UnmarshalMethod != "" {
			t.Errorf("%s: UnmarshalMethod = %q, want none since the type has no UnmarshalText", f.Name, f.UnmarshalMethod)
		}
		if len(f.UnmarshalImports) != 0 {
			t.Errorf("%s: UnmarshalImports = %v, want none", f.Name, f.UnmarshalImports)
		}
	}
}

const fixtureAliasCode = `package model

import "time"

type Local struct {
	A int64
}

type LocalAlias = Local

type TimeAlias = time.Time

type Aliased struct {
	L LocalAlias
	T TimeAlias
	P *LocalAlias
}
`

// TestParse_ResolvesAliasesToTheirTypes checks that an alias to a struct
// resolves to that struct's helpers and an alias to time.Time to the
// timestamp mapping, instead of to helpers named after the alias that
// nothing generates.
func TestParse_ResolvesAliasesToTheirTypes(t *testing.T) {
	modDir := writeModelFixture(t, fixtureAliasCode)
	_, _, structs, err := Parse([]string{modDir}, []string{"Aliased"}, false)
	if err != nil {
		t.Fatalf("Parse() returned error: %v", err)
	}
	names := map[string]bool{}
	var aliased *StructInfo
	for i := range structs {
		names[structs[i].Name] = true
		if structs[i].Name == "Aliased" {
			aliased = &structs[i]
		}
	}
	if aliased == nil || len(aliased.Fields) != 3 {
		t.Fatalf("Aliased = %+v, want three fields", aliased)
	}
	if f := aliased.Fields[0]; !f.IsStruct || f.StructName != "Local" || f.IsPointer {
		t.Errorf("L = %+v, want a value struct named Local", f)
	}
	if f := aliased.Fields[1]; f.TimeExpr == "" || f.ArrowType != "arrow.FixedWidthTypes.Timestamp_us" {
		t.Errorf("T = %+v, want the Timestamp_us mapping", f)
	}
	if f := aliased.Fields[2]; !f.IsStruct || f.StructName != "Local" || !f.IsPointer {
		t.Errorf("P = %+v, want a pointer struct named Local", f)
	}
	if !names["Local"] || names["LocalAlias"] {
		t.Errorf("structs = %v, want Local and no LocalAlias", names)
	}
}

const fixtureNestedEmbedCode = `package model

type Base struct {
	BaseField string
}

type Mid struct {
	Base
	MidField string
}

type Top struct {
	Mid
	TopField string
}
`

// TestParse_WarnsAboutSecondLevelEmbedding checks that the fields lost to a
// second level of embedding are named in a warning instead of vanishing.
func TestParse_WarnsAboutSecondLevelEmbedding(t *testing.T) {
	modDir := writeModelFixture(t, fixtureNestedEmbedCode)
	var warnings strings.Builder
	SetWarnOutput(&warnings)
	t.Cleanup(func() { SetWarnOutput(os.Stderr) })

	_, _, structs, err := Parse([]string{modDir}, []string{"Top"}, false)
	if err != nil {
		t.Fatalf("Parse() returned error: %v", err)
	}
	var names []string
	for _, f := range structs[0].Fields {
		names = append(names, f.Name)
	}
	if strings.Join(names, ",") != "MidField,TopField" {
		t.Errorf("Top fields = %v, want MidField and TopField", names)
	}
	if want := "Skipping the fields Base promotes into Mid: only one level of embedding is flattened"; !strings.Contains(warnings.String(), want) {
		t.Errorf("warnings lack %q:\n%s", want, warnings.String())
	}
}

const fixtureExternalNamedCode = `package model

import "example.com/genericsfix/model/other"

type Record struct {
	IDs    other.IDs
	Tags   other.Tags
	Codes  other.Codes
	Levels other.Levels
	PIDs   *other.IDs
}
`

const fixtureExternalNamedOther = `package other

type IDs []int64

type Tags map[string]string

type Code int32

type Codes []Code

type Level int32

func (l Level) String() string { return "level" }

type Levels []Level
`

// TestParse_UnwrapsExternalNamedContainers checks that a named slice or map
// from a package the generator has not loaded resolves through its
// underlying type instead of being refused, that an element named in that
// package is qualified, that a Stringer element stays write-only, and that
// the reader learns which package to import.
func TestParse_UnwrapsExternalNamedContainers(t *testing.T) {
	modDir := writeModelFixture(t, fixtureExternalNamedCode)
	otherDir := filepath.Join(modDir, "other")
	if err := os.MkdirAll(otherDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(otherDir, "other.go"), []byte(fixtureExternalNamedOther), 0644); err != nil {
		t.Fatal(err)
	}

	pkgName, _, structs, err := Parse([]string{modDir}, []string{"Record"}, false)
	if err != nil {
		t.Fatalf("Parse() returned error: %v", err)
	}
	if _, _, err := ResolveOutputContext(pkgName, structs, "", nil, map[string]bool{}); err != nil {
		t.Fatalf("ResolveOutputContext: %v", err)
	}
	fields := map[string]FieldInfo{}
	for _, f := range structs[0].Fields {
		fields[f.Name] = f
	}
	if f := fields["IDs"]; !f.IsList || f.QualifiedGoType != "other.IDs" {
		t.Errorf("IDs = %+v, want a list typed other.IDs", f)
	}
	if f := fields["Tags"]; !f.IsMap || f.QualifiedGoType != "other.Tags" {
		t.Errorf("Tags = %+v, want a map typed other.Tags", f)
	}
	if f := fields["Codes"]; !f.IsList || f.EltInfo == nil || f.EltInfo.QualifiedGoType != "other.Code" || f.EltInfo.ArrowType != "arrow.PrimitiveTypes.Int32" {
		t.Errorf("Codes = %+v (elt %+v), want a list of other.Code stored as Int32", f, f.EltInfo)
	}
	if f := fields["Levels"]; !f.IsList || f.EltInfo == nil || f.EltInfo.MarshalMethod != "String" {
		t.Errorf("Levels = %+v (elt %+v), want a list of Stringer elements", f, f.EltInfo)
	}
	if f := fields["PIDs"]; !f.IsList || !f.IsPointer || f.QualifiedGoType != "*other.IDs" {
		t.Errorf("PIDs = %+v, want a pointer to a list typed *other.IDs", f)
	}
	imports := CollectTypePkgImports(structs, pkgName)
	if len(imports) != 1 || imports[0].Path != "example.com/genericsfix/model/other" || imports[0].Name != "other" {
		t.Errorf("CollectTypePkgImports = %v, want the other package once", imports)
	}
}
