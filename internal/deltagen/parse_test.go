package deltagen

// parse_test.go exercises the Snapshot type parser introduced in G-03 and
// reshaped in G-07 (ParseOpts option carrier; walkFields helper). Tests at
// this refinement level focus on structural parsing only; key-field semantics
// land in G-04 with their own Group G test family.
//
// # Group F: Snapshot type parser
//
// Tests are structured around the three responsibilities of parseSnapshot:
//
//   F.1 Happy path — Header found, payload fields enumerated and shaped.
//   F.2 Shape classification — each supported shape is classified correctly.
//   F.3 Header error cases — no Header, multiple Headers, struct not found.
//   F.4 Unsupported field shapes — func, chan, interface each rejected.
//   F.5 Map field — classified as ShapeMap without error (T-02 validates tag).
//   F.6 Cross-package field filtering — unexported fields excluded when
//       ParseOpts.CrossPackage is true.
//   F.7 Default ParseOpts equivalence — the zero value of ParseOpts behaves
//       identically to explicit CrossPackage: false; KeyFieldOverride is
//       carried through unconsumed (G-04 will consume it).
//
// All tests load fixtures from the testdata/parse/ tree. Because the fixtures
// live within the eddt module, packages.Load resolves go.resystems.io/eddt/runtime
// through the module's go.mod without requiring a separate go.work file.

import (
	"strings"
	"testing"
)

// ── Group F: Snapshot type parser ─────────────────────────────────────────────

// TestParse_ValidSnapshot verifies the happy path: parseSnapshot finds exactly
// one embedded runtime.Header field and enumerates all payload fields from the
// ValidSnapshot fixture. It does not assert individual shapes (that is
// TestParse_ShapeClassification); it asserts structural correctness only.
// Covers: R-12, R-13
func TestParse_ValidSnapshot(t *testing.T) {
	pkgs, err := loadPackages([]string{"./testdata/parse/valid"}, false)
	if err != nil {
		t.Fatalf("loadPackages: %v", err)
	}

	snap, err := parseSnapshot(pkgs, "ValidSnapshot", ParseOpts{})
	if err != nil {
		t.Fatalf("parseSnapshot: unexpected error: %v", err)
	}

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
	pkgs, err := loadPackages([]string{"./testdata/parse/valid"}, false)
	if err != nil {
		t.Fatalf("loadPackages: %v", err)
	}

	snap, err := parseSnapshot(pkgs, "ValidSnapshot", ParseOpts{})
	if err != nil {
		t.Fatalf("parseSnapshot: %v", err)
	}

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
	pkgs, err := loadPackages([]string{"./testdata/parse/valid"}, false)
	if err != nil {
		t.Fatalf("loadPackages: %v", err)
	}

	_, err = parseSnapshot(pkgs, "DoesNotExist", ParseOpts{})
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
	pkgs, err := loadPackages([]string{"./testdata/parse/no_header", runtimePkgImportPath}, false)
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
	pkgs, err := loadPackages([]string{"./testdata/parse/multi_header"}, false)
	if err != nil {
		t.Fatalf("loadPackages: %v", err)
	}

	_, err = parseSnapshot(pkgs, "DualHeaderSnapshot", ParseOpts{})
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
			pkgs, err := loadPackages([]string{"./testdata/parse/" + tc.fixture}, false)
			if err != nil {
				t.Fatalf("loadPackages: %v", err)
			}

			_, err = parseSnapshot(pkgs, tc.structName, ParseOpts{})
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
// ShapeMap without a parse-time error. The tag-combination constraint (map
// fields require delta.omit) is enforced by T-02, not by the parser.
// Covers: R-13
func TestParse_MapField(t *testing.T) {
	pkgs, err := loadPackages([]string{"./testdata/parse/with_map"}, false)
	if err != nil {
		t.Fatalf("loadPackages: %v", err)
	}

	snap, err := parseSnapshot(pkgs, "MapSnapshot", ParseOpts{})
	if err != nil {
		t.Fatalf("parseSnapshot: unexpected error: %v", err)
	}

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
	pkgs, err := loadPackages([]string{"./testdata/parse/mixed_exported"}, false)
	if err != nil {
		t.Fatalf("loadPackages: %v", err)
	}

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
// behaves as designed in G-07:
//
//   - The zero value `ParseOpts{}` is equivalent to `ParseOpts{CrossPackage: false}`.
//   - `KeyFieldOverride` is carried through but ignored at this refinement
//     step (G-04 will consume it). Passing a non-empty override must not
//     affect the result of structural parsing.
//
// These guarantees are what keep the call-site signature stable across the
// G-07 → G-04 → G-06 sequence: G-04 can later interpret KeyFieldOverride
// without changing any existing G-07-era invocation.
// Covers: R-12, R-13
func TestParse_ParseOptsEquivalence(t *testing.T) {
	pkgs, err := loadPackages([]string{"./testdata/parse/valid"}, false)
	if err != nil {
		t.Fatalf("loadPackages: %v", err)
	}

	// Three invocations that must yield structurally-identical ParsedSnapshots:
	//   a) zero value
	//   b) explicit same-package mode
	//   c) explicit same-package mode plus an arbitrary KeyFieldOverride (ignored)
	a, err := parseSnapshot(pkgs, "ValidSnapshot", ParseOpts{})
	if err != nil {
		t.Fatalf("zero opts: %v", err)
	}
	b, err := parseSnapshot(pkgs, "ValidSnapshot", ParseOpts{CrossPackage: false})
	if err != nil {
		t.Fatalf("explicit CrossPackage false: %v", err)
	}
	c, err := parseSnapshot(pkgs, "ValidSnapshot", ParseOpts{KeyFieldOverride: "Bearer"})
	if err != nil {
		t.Fatalf("with KeyFieldOverride: %v", err)
	}

	for _, snap := range []*ParsedSnapshot{a, b, c} {
		if snap.HeaderVar == nil {
			t.Errorf("HeaderVar nil; expected populated")
		}
		// G-07 contract: KeyVar is always nil at this refinement step.
		if snap.KeyVar != nil {
			t.Errorf("KeyVar = %v; expected nil at G-07", snap.KeyVar)
		}
	}

	// Field counts must match across the three invocations.
	if len(a.Fields) != len(b.Fields) || len(b.Fields) != len(c.Fields) {
		t.Errorf("Fields counts diverge: zero=%d, explicit=%d, override=%d",
			len(a.Fields), len(b.Fields), len(c.Fields))
	}
}

// ── Test helpers ──────────────────────────────────────────────────────────────

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
