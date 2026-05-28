package deltagen

// parse_test.go exercises the Snapshot type parser introduced in G-03,
// reshaped in G-07 (ParseOpts option carrier; walkFields helper), and
// extended in G-04 (parseKeyField; entity-key recognition and validation).
//
// # Group F: Snapshot structural parsing
//
// Tests are structured around the structural responsibilities of parseSnapshot:
//
//   F.1 Happy path — Header found, payload fields enumerated and shaped.
//   F.2 Shape classification — each supported shape is classified correctly.
//   F.3 Header error cases — no Header, multiple Headers, struct not found.
//   F.4 Unsupported field shapes — func, chan, interface each rejected.
//   F.5 Map field — classified as ShapeMap without error (T-02 validates tag).
//   F.6 Cross-package field filtering — unexported fields excluded when
//       ParseOpts.CrossPackage is true.
//   F.7 ParseOpts equivalence — zero value behaves identically to explicit
//       CrossPackage: false; a KeyFieldOverride pointing at the same field
//       as the entity.key tag yields a structurally-equal result.
//
// # Group G: Entity-key recognition (G-04)
//
// Tests cover the parseKeyField responsibilities:
//
//   G.1 Tag-found struct key — happy path via tag scan.
//   G.2 Tag-found scalar key — named-basic types accepted as keys.
//   G.3 Override-OK — KeyFieldOverride supersedes tag-based discovery.
//   G.4 No key — missing tag and missing override → error.
//   G.5 Multiple keys — two entity.key tags → error.
//   G.6 Pointer key — pointer-typed key field → error.
//   G.7 Non-comparable struct key — key struct contains a slice → error
//       naming the offending sub-field.
//   G.8 Override missing — override names a field not in the struct → error.
//   G.9 Override wins over tag — tag and override naming different fields,
//       override is selected, tagged field falls back into payload Fields.
//
// All tests load fixtures from the testdata/parse/ tree. Because the fixtures
// live within the eddt module, packages.Load resolves go.resystems.io/eddt/runtime
// through the module's go.mod without requiring a separate go.work file.

import (
	"go/types"
	"log/slog"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

// ── Group F: Snapshot type parser ─────────────────────────────────────────────

// TestParse_ValidSnapshot verifies the happy path: parseSnapshot finds exactly
// one embedded runtime.Header field and enumerates all payload fields from the
// ValidSnapshot fixture. It does not assert individual shapes (that is
// TestParse_ShapeClassification); it asserts structural correctness only.
// Covers: R-12, R-13
func TestParse_ValidSnapshot(t *testing.T) {
	snap := parseFixture(t, "valid", "ValidSnapshot", ParseOpts{})

	// Header must be found and named "Header" (Go's anonymous-embed convention).
	if snap.HeaderVar == nil {
		t.Fatal("HeaderVar: got nil, want non-nil")
	}
	if snap.HeaderVar.Name() != "Header" {
		t.Errorf("HeaderVar.Name: got %q, want %q", snap.HeaderVar.Name(), "Header")
	}

	// ValidSnapshot has 8 payload fields (Attached, Status, Bearer, TAI, Count,
	// Location, Bearers, LastSeen) — the embedded Header is not counted.
	const wantFields = 8
	if len(snap.Fields) != wantFields {
		t.Errorf("len(Fields): got %d, want %d; fields: %v",
			len(snap.Fields), wantFields, fieldNames(snap))
	}

	// Package metadata must be populated.
	if snap.PkgPath == "" {
		t.Error("PkgPath: got empty string")
	}
	if snap.PkgName != "valid" {
		t.Errorf("PkgName: got %q, want %q", snap.PkgName, "valid")
	}
}

// TestParse_ShapeClassification verifies that each field in ValidSnapshot is
// assigned the expected FieldShape. This is the authoritative test for the
// classifyShape function.
// Covers: R-13
func TestParse_ShapeClassification(t *testing.T) {
	snap := parseFixture(t, "valid", "ValidSnapshot", ParseOpts{})

	// Build a name→shape index for assertion.
	shapes := make(map[string]FieldShape, len(snap.Fields))
	for _, f := range snap.Fields {
		shapes[f.Name] = f.Shape
	}

	cases := []struct {
		field string
		want  FieldShape
	}{
		{"Attached", ShapeScalar},      // bare bool
		{"Status", ShapeScalar},        // named int32
		{"Bearer", ShapeScalar},        // named string
		{"TAI", ShapePointer},          // *TAI (pointer to struct)
		{"Count", ShapePointer},        // *int32 (pointer to basic)
		{"Location", ShapeStructValue}, // struct value
		{"Bearers", ShapeSlice},        // []BearerID
		{"LastSeen", ShapeStructValue}, // time.Time — stdlib named struct → ShapeStructValue
	}

	for _, tc := range cases {
		got, ok := shapes[tc.field]
		if !ok {
			t.Errorf("field %q missing from result", tc.field)
			continue
		}
		if got != tc.want {
			t.Errorf("field %q: shape = %v, want %v", tc.field, got, tc.want)
		}
	}
}

// TestParse_StructNotFound verifies that requesting a struct name that does
// not exist in the loaded package produces a descriptive error.
// Covers: R-12
func TestParse_StructNotFound(t *testing.T) {
	pkgs := loadFixture(t, "valid")

	_, err := parseSnapshot(pkgs, "DoesNotExist", ParseOpts{})
	if err == nil {
		t.Fatal("expected error for missing struct, got nil")
	}
	if !strings.Contains(err.Error(), "DoesNotExist") {
		t.Errorf("error should mention the struct name, got: %v", err)
	}
}

// TestParse_NoHeader verifies that a struct without an embedded runtime.Header
// field produces an error containing "no embedded runtime.Header". The runtime
// package is loaded alongside the fixture so that headerTypeFor succeeds.
// Covers: R-12
func TestParse_NoHeader(t *testing.T) {
	// Load the fixture package together with the runtime package so the Header
	// type is available for comparison even though no_header does not import it.
	pkgs, err := loadPackages([]string{"./testdata/parse/no_header", runtimePkgImportPath}, slog.Default())
	if err != nil {
		t.Fatalf("loadPackages: %v", err)
	}

	_, err = parseSnapshot(pkgs, "PlainStruct", ParseOpts{})
	if err == nil {
		t.Fatal("expected error for struct without Header, got nil")
	}
	if !strings.Contains(err.Error(), "no embedded runtime.Header") {
		t.Errorf("error should mention missing Header, got: %v", err)
	}
}

// TestParse_MultipleHeaders verifies that a struct with two runtime.Header
// fields produces an error containing "multiple".
// Covers: R-12
func TestParse_MultipleHeaders(t *testing.T) {
	pkgs := loadFixture(t, "multi_header")

	_, err := parseSnapshot(pkgs, "DualHeaderSnapshot", ParseOpts{})
	if err == nil {
		t.Fatal("expected error for struct with multiple Headers, got nil")
	}
	if !strings.Contains(err.Error(), "multiple") {
		t.Errorf("error should mention 'multiple', got: %v", err)
	}
}

// TestParse_UnsupportedFieldShapes verifies that func, chan, and interface
// payload fields each produce a generation-time error.
// Covers: R-13
func TestParse_UnsupportedFieldShapes(t *testing.T) {
	cases := []struct {
		fixture    string
		structName string
		wantErr    string
	}{
		{"bad_func", "FuncSnapshot", "function-valued"},
		{"bad_chan", "ChanSnapshot", "channel"},
		{"bad_iface", "IfaceSnapshot", "interface"},
	}

	for _, tc := range cases {
		t.Run(tc.fixture, func(t *testing.T) {
			pkgs := loadFixture(t, tc.fixture)

			_, err := parseSnapshot(pkgs, tc.structName, ParseOpts{})
			if err == nil {
				t.Fatalf("expected error for unsupported field shape in %s, got nil", tc.fixture)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error should contain %q, got: %v", tc.wantErr, err)
			}
		})
	}
}

// TestParse_MapField verifies that a map payload field is classified as
// ShapeMap without a parse-time error. Under the harmonised three-axis
// model (refinements §1.6.3, Errata E-16), untagged maps are admitted
// with the atomic default; no tag-combination constraint applies.
// Covers: R-13, R-17
func TestParse_MapField(t *testing.T) {
	snap := parseFixture(t, "with_map", "MapSnapshot", ParseOpts{})

	if len(snap.Fields) != 1 {
		t.Fatalf("Fields: got %d, want 1", len(snap.Fields))
	}
	if snap.Fields[0].Shape != ShapeMap {
		t.Errorf("Tags field shape: got %v, want ShapeMap", snap.Fields[0].Shape)
	}
}

// TestParse_CrossPackageFiltersUnexported verifies that parseSnapshot with
// crossPackage=true excludes unexported fields from the result. The fixture
// MixedSnapshot has two exported and one unexported payload fields; only the
// two exported ones should appear in cross-package mode.
// Covers: R-10, E-12
func TestParse_CrossPackageFiltersUnexported(t *testing.T) {
	pkgs := loadFixture(t, "mixed_exported")

	// Same-package parse: all three payload fields visible.
	snapSame, err := parseSnapshot(pkgs, "MixedSnapshot", ParseOpts{})
	if err != nil {
		t.Fatalf("parseSnapshot (same-pkg): %v", err)
	}
	if len(snapSame.Fields) != 3 {
		t.Errorf("same-pkg: Fields count = %d, want 3; names: %v",
			len(snapSame.Fields), fieldNames(snapSame))
	}

	// Cross-package parse: only exported fields visible.
	snapCross, err := parseSnapshot(pkgs, "MixedSnapshot", ParseOpts{CrossPackage: true})
	if err != nil {
		t.Fatalf("parseSnapshot (cross-pkg): %v", err)
	}
	if len(snapCross.Fields) != 2 {
		t.Errorf("cross-pkg: Fields count = %d, want 2; names: %v",
			len(snapCross.Fields), fieldNames(snapCross))
	}
	for _, f := range snapCross.Fields {
		if !isExported(f.Name) {
			t.Errorf("cross-pkg: unexported field %q present in result", f.Name)
		}
	}
}

// TestParse_ParseOptsEquivalence verifies that the ParseOpts options carrier
// produces equivalent results across redundant invocations:
//
//   - The zero value `ParseOpts{}` is equivalent to `ParseOpts{CrossPackage: false}`.
//   - A KeyFieldOverride naming the same field as the entity.key tag yields
//     a structurally-identical result (the override path and tag path
//     converge when they pick the same field).
//
// These guarantees keep the call-site signature stable: G-06 can route the
// CLI value through ParseOpts.KeyFieldOverride without changing the parser's
// observable behaviour for tag-conforming Snapshots.
// Covers: R-12, R-13, R-14
func TestParse_ParseOptsEquivalence(t *testing.T) {
	pkgs := loadFixture(t, "valid")

	// Three invocations that must yield structurally-identical ParsedSnapshots:
	//   a) zero value (tag path)
	//   b) explicit same-package mode (tag path)
	//   c) override naming the same field as the eddt:"entity.key" tag
	a, err := parseSnapshot(pkgs, "ValidSnapshot", ParseOpts{})
	if err != nil {
		t.Fatalf("zero opts: %v", err)
	}
	b, err := parseSnapshot(pkgs, "ValidSnapshot", ParseOpts{CrossPackage: false})
	if err != nil {
		t.Fatalf("explicit CrossPackage false: %v", err)
	}
	c, err := parseSnapshot(pkgs, "ValidSnapshot", ParseOpts{KeyFieldOverride: "Key"})
	if err != nil {
		t.Fatalf("with KeyFieldOverride: %v", err)
	}

	for _, snap := range []*ParsedSnapshot{a, b, c} {
		if snap.HeaderVar == nil {
			t.Errorf("HeaderVar nil; expected populated")
		}
		// G-04 contract: KeyVar is always populated for a successful parse.
		if snap.KeyVar == nil {
			t.Errorf("KeyVar nil; expected populated after G-04")
			continue
		}
		if snap.KeyVar.Name() != "Key" {
			t.Errorf("KeyVar.Name = %q, want %q", snap.KeyVar.Name(), "Key")
		}
	}

	// Field counts must match across the three invocations.
	if len(a.Fields) != len(b.Fields) || len(b.Fields) != len(c.Fields) {
		t.Errorf("Fields counts diverge: zero=%d, explicit=%d, override=%d",
			len(a.Fields), len(b.Fields), len(c.Fields))
	}
}

// ── Group G: Entity-key recognition (G-04) ────────────────────────────────────

// TestParse_KeyField_TagFoundStruct verifies the happy path for tag-based
// key-field discovery with a struct-valued key. ValidSnapshot has a `Key UEKey
// \`eddt:"entity.key"\“ field; parseSnapshot must surface UEKey via KeyVar
// and exclude it from Fields (so the payload count stays at 8).
// Covers: R-14, R-18
func TestParse_KeyField_TagFoundStruct(t *testing.T) {
	snap := parseFixture(t, "valid", "ValidSnapshot", ParseOpts{})

	if snap.KeyVar == nil {
		t.Fatal("KeyVar: got nil, want populated")
	}
	if snap.KeyVar.Name() != "Key" {
		t.Errorf("KeyVar.Name: got %q, want %q", snap.KeyVar.Name(), "Key")
	}
	// Key must not appear in Fields — it was moved to KeyVar.
	for _, f := range snap.Fields {
		if f.Name == "Key" {
			t.Errorf("Fields contains key field %q; expected it to be removed", f.Name)
		}
	}
}

// TestParse_KeyField_TagFoundScalar verifies that a named-basic (scalar) type
// is accepted as the entity-key type. The relaxation captured in E-10 is that
// any value-typed comparable type works — not just structs.
// Covers: R-14, R-18
func TestParse_KeyField_TagFoundScalar(t *testing.T) {
	snap := parseFixture(t, "scalar_key", "ScalarKeySnapshot", ParseOpts{})

	if snap.KeyVar == nil {
		t.Fatal("KeyVar: got nil, want populated for scalar key")
	}
	if snap.KeyVar.Name() != "Key" {
		t.Errorf("KeyVar.Name: got %q, want %q", snap.KeyVar.Name(), "Key")
	}
	// Status remains the only payload field after Key is removed.
	if len(snap.Fields) != 1 {
		t.Errorf("len(Fields): got %d, want 1; fields: %v", len(snap.Fields), fieldNames(snap))
	}
}

// TestParse_KeyField_OverrideOK verifies that ParseOpts.KeyFieldOverride
// selects the named field even when it carries no entity.key tag. The
// no_key fixture has no entity.key tag at all; the override path is the
// only way to identify a key for that Snapshot.
// Covers: R-14, R-18, E-13
func TestParse_KeyField_OverrideOK(t *testing.T) {
	snap := parseFixture(t, "no_key", "NoKeySnapshot", ParseOpts{KeyFieldOverride: "Peer"})

	if snap.KeyVar == nil {
		t.Fatal("KeyVar: got nil, want populated via override")
	}
	if snap.KeyVar.Name() != "Peer" {
		t.Errorf("KeyVar.Name: got %q, want %q", snap.KeyVar.Name(), "Peer")
	}
	// Status remains the only payload field after Peer is removed.
	if len(snap.Fields) != 1 {
		t.Errorf("len(Fields): got %d, want 1; fields: %v", len(snap.Fields), fieldNames(snap))
	}
}

// TestParse_KeyField_NoKey verifies that a Snapshot without an entity.key
// tag and without a CLI override is rejected with a descriptive error.
// Covers: R-14
func TestParse_KeyField_NoKey(t *testing.T) {
	pkgs := loadFixture(t, "no_key")

	_, err := parseSnapshot(pkgs, "NoKeySnapshot", ParseOpts{})
	if err == nil {
		t.Fatal("expected error for missing entity.key, got nil")
	}
	if !strings.Contains(err.Error(), "entity.key") {
		t.Errorf("error should mention entity.key, got: %v", err)
	}
}

// TestParse_KeyField_MultiKey verifies that two fields tagged entity.key
// in the same Snapshot produce an error containing "multiple".
// Covers: R-14, R-18
func TestParse_KeyField_MultiKey(t *testing.T) {
	pkgs := loadFixture(t, "multi_key")

	_, err := parseSnapshot(pkgs, "MultiKeySnapshot", ParseOpts{})
	if err == nil {
		t.Fatal("expected error for multiple entity.key fields, got nil")
	}
	if !strings.Contains(err.Error(), "multiple") {
		t.Errorf("error should mention 'multiple', got: %v", err)
	}
}

// TestParse_KeyField_Pointer verifies that a pointer-typed entity.key field
// is rejected. Pointer equality is identity, not value equality, so a
// pointer-typed key would let two Snapshots with equal key contents hash
// to different EntityIDs.
// Covers: R-14
func TestParse_KeyField_Pointer(t *testing.T) {
	pkgs := loadFixture(t, "key_pointer")

	_, err := parseSnapshot(pkgs, "PtrKeySnapshot", ParseOpts{})
	if err == nil {
		t.Fatal("expected error for pointer-typed entity.key, got nil")
	}
	if !strings.Contains(err.Error(), "pointer") {
		t.Errorf("error should mention 'pointer', got: %v", err)
	}
}

// TestParse_KeyField_NonComparable verifies that a struct-valued key whose
// underlying struct contains a non-comparable (slice) field is rejected,
// and that the error names the offending sub-field so the Snapshot author
// can locate the problem quickly.
// Covers: R-14, R-18
func TestParse_KeyField_NonComparable(t *testing.T) {
	pkgs := loadFixture(t, "key_with_slice")

	_, err := parseSnapshot(pkgs, "SliceyKeySnapshot", ParseOpts{})
	if err == nil {
		t.Fatal("expected error for non-comparable key struct, got nil")
	}
	if !strings.Contains(err.Error(), "IDs") {
		t.Errorf("error should name offending sub-field 'IDs', got: %v", err)
	}
	if !strings.Contains(err.Error(), "comparable") {
		t.Errorf("error should mention 'comparable', got: %v", err)
	}
}

// TestParse_KeyField_OverrideMissing verifies that a KeyFieldOverride naming
// a field not present in the struct produces a descriptive error.
// Covers: R-14, E-13
func TestParse_KeyField_OverrideMissing(t *testing.T) {
	pkgs := loadFixture(t, "valid")

	_, err := parseSnapshot(pkgs, "ValidSnapshot", ParseOpts{KeyFieldOverride: "NoSuchField"})
	if err == nil {
		t.Fatal("expected error for unknown override field, got nil")
	}
	if !strings.Contains(err.Error(), "NoSuchField") {
		t.Errorf("error should mention the override field name, got: %v", err)
	}
}

// TestParse_KeyField_OverrideWinsOverTag verifies the precedence rule: when
// both an entity.key tag and a CLI override are present, the override wins.
// The tagged field falls back into payload Fields rather than being silently
// discarded. The parser does not warn; the CLI layer (G-06) emits a
// --verbose warning.
// Covers: R-14, E-13
func TestParse_KeyField_OverrideWinsOverTag(t *testing.T) {
	// ValidSnapshot has `Key UEKey \`eddt:"entity.key"\`` AND a comparable
	// struct field `Location LocationInfo` (no tag). The override picks
	// Location; Key is left in Fields as ordinary payload.
	snap := parseFixture(t, "valid", "ValidSnapshot", ParseOpts{KeyFieldOverride: "Location"})

	if snap.KeyVar == nil {
		t.Fatal("KeyVar: got nil, want populated via override")
	}
	if snap.KeyVar.Name() != "Location" {
		t.Errorf("KeyVar.Name: got %q, want %q (override should win)", snap.KeyVar.Name(), "Location")
	}

	// The tagged Key field must now appear in Fields as ordinary payload —
	// the override silently superseded its tag without dropping the field.
	foundKey := false
	for _, f := range snap.Fields {
		if f.Name == "Key" {
			foundKey = true
			break
		}
	}
	if !foundKey {
		t.Errorf("Fields missing %q; expected the tagged-but-overridden field to fall back to payload: %v",
			"Key", fieldNames(snap))
	}
}

// TestParse_TagWiring verifies that ParsedField.Tag is populated from the
// raw tag string by walkFields (T-02). ValidSnapshot has no delta.* tags,
// so every payload field carries Tag.Kind = TagKindNone.
// Covers: R-15
func TestParse_TagWiring(t *testing.T) {
	snap := parseFixture(t, "valid", "ValidSnapshot", ParseOpts{})
	for _, f := range snap.Fields {
		if f.Tag.Kind != TagKindNone {
			t.Errorf("field %q: Tag.Kind = %v, want TagKindNone",
				f.Name, f.Tag.Kind)
		}
	}
}

// TestParse_TagShapeGate_NestedOK verifies the harmonised granularity-axis
// gate admits delta.nested on every composite shape (struct value, slice,
// map). One Snapshot per shape.
// Covers: R-15, R-17 (E-14)
func TestParse_TagShapeGate_NestedOK(t *testing.T) {
	cases := []struct {
		structName string
	}{
		{"NestedStructSnap"},
		{"NestedSliceSnap"},
		{"NestedMapSnap"},
	}
	for _, tc := range cases {
		t.Run(tc.structName, func(t *testing.T) {
			snap := parseFixture(t, "nested_ok", tc.structName, ParseOpts{})
			if len(snap.Fields) != 1 {
				t.Fatalf("len(Fields): got %d, want 1; fields: %v",
					len(snap.Fields), fieldNames(snap))
			}
			if snap.Fields[0].Tag.Kind != TagKindNested {
				t.Errorf("Tag.Kind: got %v, want TagKindNested",
					snap.Fields[0].Tag.Kind)
			}
		})
	}
}

// TestParse_TagShapeGate_NestedReject verifies the harmonised granularity-
// axis gate rejects delta.nested on non-composite shapes (scalar, pointer).
// Error must mention "composite" so the message is actionable.
// Covers: R-15, R-17 (E-14)
func TestParse_TagShapeGate_NestedReject(t *testing.T) {
	cases := []struct {
		structName string
	}{
		{"NestedScalarSnap"},
		{"NestedPointerSnap"},
	}
	for _, tc := range cases {
		t.Run(tc.structName, func(t *testing.T) {
			pkgs := loadFixture(t, "nested_bad")
			_, err := parseSnapshot(pkgs, tc.structName, ParseOpts{})
			if err == nil {
				t.Fatalf("expected error for nested on non-composite, got nil")
			}
			if !strings.Contains(err.Error(), "composite") {
				t.Errorf("error should mention 'composite', got: %v", err)
			}
		})
	}
}

// TestParse_CombinedTags verifies that a Snapshot carrying entity.key and
// delta.* tags simultaneously produces correct Tag.Kind and Tag.Raw values
// for every payload field — proving all eddt: tags flow through the same
// parsed-tag code path after the T-03 migration.
// Covers: R-15, R-18
func TestParse_CombinedTags(t *testing.T) {
	snap := parseFixture(t, "combined_tags", "CombinedTagsSnapshot", ParseOpts{})

	if snap.KeyVar == nil {
		t.Fatal("KeyVar: got nil, want populated (entity.key recognised via Tag.Kind)")
	}
	if snap.KeyVar.Name() != "Key" {
		t.Errorf("KeyVar.Name: got %q, want %q", snap.KeyVar.Name(), "Key")
	}

	// Build a lookup by field name for the assertions below.
	byName := make(map[string]ParsedField, len(snap.Fields))
	for _, f := range snap.Fields {
		byName[f.Name] = f
	}

	want := []struct {
		name   string
		kind   TagKind
		rawTag string
		optKey string
		optVal string
	}{
		{"Omitted", TagKindOmit, "delta.omit", "", ""},
		{"Legacy", TagKindRetired, "delta.retired,since=2026-05-20", "since", "2026-05-20"},
		{"Sub", TagKindNested, "delta.nested", "", ""},
		{"Plain", TagKindNone, "", "", ""},
	}

	for _, w := range want {
		f, ok := byName[w.name]
		if !ok {
			t.Errorf("field %q missing from payload Fields", w.name)
			continue
		}
		if f.Tag.Kind != w.kind {
			t.Errorf("field %q: Tag.Kind = %v, want %v", w.name, f.Tag.Kind, w.kind)
		}
		if f.Tag.Raw != w.rawTag {
			t.Errorf("field %q: Tag.Raw = %q, want %q", w.name, f.Tag.Raw, w.rawTag)
		}
		if w.optKey != "" {
			if got := f.Tag.Options[w.optKey]; got != w.optVal {
				t.Errorf("field %q: Options[%q] = %q, want %q", w.name, w.optKey, got, w.optVal)
			}
		}
	}
}

// ── Test helpers ──────────────────────────────────────────────────────────────

// loadFixture loads the testdata/parse/<name> fixture package.
func loadFixture(t *testing.T, name string) []*packages.Package {
	t.Helper()
	pkgs, err := loadPackages([]string{"./testdata/parse/" + name}, slog.Default())
	if err != nil {
		t.Fatalf("loadFixture(%q): %v", name, err)
	}
	return pkgs
}

// parseFixture loads then parses the given fixture+struct. Fatals on any error.
// Use the direct loadPackages/parseSnapshot pair when the test asserts on errors.
func parseFixture(t *testing.T, fixture, structName string, opts ParseOpts) *ParsedSnapshot {
	t.Helper()
	pkgs := loadFixture(t, fixture)
	snap, err := parseSnapshot(pkgs, structName, opts)
	if err != nil {
		t.Fatalf("parseFixture(%q, %q): %v", fixture, structName, err)
	}
	return snap
}

// TestParse_NestedFieldEmbedHeader verifies that a delta.nested field whose
// type embeds runtime.Header is rejected at parse time with a §3.3.2 error
// (nested types must be sub-structures, not chain anchors).
// Covers: N-01 req 07
func TestParse_NestedFieldEmbedHeader(t *testing.T) {
	pkgs := loadFixture(t, "nested_header_embed")
	_, err := parseSnapshot(pkgs, "NestedHeaderEmbedSnapshot", ParseOpts{})
	if err == nil {
		t.Fatal("expected §3.3.2 parse error for delta.nested type embedding runtime.Header, got nil")
	}
	if !strings.Contains(err.Error(), "embeds runtime.Header") {
		t.Errorf("error should mention 'embeds runtime.Header', got: %v", err)
	}
	if !strings.Contains(err.Error(), "§3.3.2") {
		t.Errorf("error should mention §3.3.2, got: %v", err)
	}
}

// TestParse_NestedCycleDetected verifies that validateNestedAcyclic reports a
// clear cycle error for a programmatically-constructed cyclic delta.nested graph
// (A.F → B, B.G → A). Struct-value cycles cannot be expressed in valid Go source
// because the compiler rejects invalid recursive types, so this test constructs
// the cycle directly via go/types.
// Covers: N-02
func TestParse_NestedCycleDetected(t *testing.T) {
	pkg := types.NewPackage("test/cycle", "cycle")

	objA := types.NewTypeName(0, pkg, "A", nil)
	typeA := types.NewNamed(objA, nil, nil)

	objB := types.NewTypeName(0, pkg, "B", nil)
	typeB := types.NewNamed(objB, nil, nil)

	// A has one delta.nested field F of type B.
	fieldAF := types.NewVar(0, pkg, "F", typeB)
	structA := types.NewStruct([]*types.Var{fieldAF}, []string{`eddt:"delta.nested"`})
	typeA.SetUnderlying(structA)

	// B has one delta.nested field G of type A — completing the cycle.
	fieldBG := types.NewVar(0, pkg, "G", typeA)
	structB := types.NewStruct([]*types.Var{fieldBG}, []string{`eddt:"delta.nested"`})
	typeB.SetUnderlying(structB)

	err := validateNestedAcyclic(typeA, []string{"Root"}, nil)
	if err == nil {
		t.Fatal("expected cycle error from validateNestedAcyclic, got nil")
	}
	if !strings.Contains(err.Error(), "cycle") {
		t.Errorf("error should mention 'cycle', got: %v", err)
	}
	if !strings.Contains(err.Error(), "§3.3.2") {
		t.Errorf("error should mention §3.3.2, got: %v", err)
	}
}

// fieldNames returns the field names from a ParsedSnapshot for use in error
// messages. It avoids importing fmt in the test output path.
func fieldNames(s *ParsedSnapshot) []string {
	names := make([]string, len(s.Fields))
	for i, f := range s.Fields {
		names[i] = f.Name
	}
	return names
}

// isExported reports whether name is an exported Go identifier.
func isExported(name string) bool {
	if len(name) == 0 {
		return false
	}
	return name[0] >= 'A' && name[0] <= 'Z'
}
