package deltagen

// conformance_test.go implements C-02 and C-03 corpus conformance property tests.
//
// C-02 (TestConformance_RoundTrip): For each corpus case, generates the delta
// source and injects a testing/quick round-trip property test:
//
//	Apply(a, Diff(a, b)) == b
//
// where a and b are valid sequential chain entries (same EntityID/ChainID,
// b.Sequence = a.Sequence + 1, nil Provenance).  Full snapshot equality is
// the invariant for baseline and struct_key; composite uses set-membership
// equality (toSortedUnique) for the Groups field (N-04, E-15).
//
// C-03 (TestConformance_Identity): For each corpus case, generates the delta
// source and injects a testing/quick identity-diff property test:
//
//	reflect.DeepEqual(Apply(a, Diff(a, aprime)), aprime)
//
// where aprime is a struct copy of a with aprime.Header.Sequence = a.Sequence + 1.
// Because payload(a) == payload(aprime), the diff is zero-payload (all Set*
// nil) and Apply must preserve every field unchanged.  Full snapshot equality
// (reflect.DeepEqual) is the correct invariant for all three corpus cases — no
// toSortedUnique required (identity diff produces zero additions/removals, so
// N-04 slice order is preserved exactly).
//
// Test matrix:
//
//	TestConformance_RoundTrip/BaselineSnapshot  (C-02)
//	TestConformance_RoundTrip/CompositeSnapshot (C-02)
//	TestConformance_RoundTrip/SessionSnapshot   (C-02)
//	TestConformance_Identity/BaselineSnapshot   (C-03)
//	TestConformance_Identity/CompositeSnapshot  (C-03)
//	TestConformance_Identity/SessionSnapshot    (C-03)

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestConformance_RoundTrip is the C-02 property test.
//
// For each corpus case it generates the delta source, injects a
// testing/quick round-trip property test into an isolated temp module,
// and runs go test -run TestRoundTrip_Property.  The invariant tested:
//
//	Apply(a, Diff(a, b)) == b
//
// where a and b are valid sequential chain entries (same EntityID/ChainID,
// b.Sequence = a.Sequence + 1, nil Provenance).  The full snapshot —
// Header and payload — is compared, with the exception of delta.nested
// slice fields (N-04, E-15) for which set-membership equality is the
// correct invariant.
func TestConformance_RoundTrip(t *testing.T) {
	dispatchers := map[string]func(*testing.T, []byte){
		"baseline":   roundTripCheckBaseline,
		"composite":  roundTripCheckComposite,
		"struct_key": roundTripCheckStructKey,
	}
	for _, tc := range corpus {
		t.Run(tc.name, func(t *testing.T) {
			check := dispatchers[tc.dir]
			outPath := filepath.Join(t.TempDir(), "delta.go")
			cfg := Config{
				InputPkgs:     []string{"./testdata/corpus/" + tc.dir},
				TargetStructs: []string{tc.name},
				OutPath:       outPath,
			}
			if err := New(cfg).Run(); err != nil {
				t.Fatalf("Run(): %v", err)
			}
			src, err := os.ReadFile(outPath)
			if err != nil {
				t.Fatalf("read generated file: %v", err)
			}
			check(t, src)
		})
	}
}

// roundTripCheckBaseline runs the C-02 property test for the baseline corpus case.
//
// Injected invariant: reflect.DeepEqual(Apply(a, Diff(a, b)), b) — full equality
// including Header — for 1000 random baselinePayload pairs.
func roundTripCheckBaseline(t *testing.T, generatedSrc []byte) {
	t.Helper()
	roundTripCheckCorpus(t, "baseline", "baseline", generatedSrc, baselineRoundTripTest)
}

// roundTripCheckComposite runs the C-02 property test for the composite corpus case.
//
// Injected invariant: snapshotEqual(Apply(a, Diff(a, b)), b) — full equality
// except Groups (N-04 E-15 set-membership) — for 1000 random compositePayload pairs.
func roundTripCheckComposite(t *testing.T, generatedSrc []byte) {
	t.Helper()
	roundTripCheckCorpus(t, "composite", "composite", generatedSrc, compositeRoundTripTest)
}

// roundTripCheckStructKey runs the C-02 property test for the struct_key corpus case.
//
// Injected invariant: reflect.DeepEqual(Apply(a, Diff(a, b)), b) — full equality
// including Header — for 1000 random sessionPayload pairs.
func roundTripCheckStructKey(t *testing.T, generatedSrc []byte) {
	t.Helper()
	roundTripCheckCorpus(t, "struct_key", "struct_key", generatedSrc, structKeyRoundTripTest)
}

// roundTripCheckCorpus writes the corpus fixture, the generated delta source,
// and an injected testing/quick property test into an isolated temp module and
// runs go test -run TestRoundTrip_Property.
//
// Steps:
//  1. Create a temp directory via t.TempDir().
//  2. Derive the module root (two levels above the package directory).
//  3. Copy the fixture .go source from testdata/corpus/<dir>/ as snapshot.go.
//  4. Write the generated delta source as delta.go; assert it is gofmt-clean.
//  5. Write the round-trip property test source as roundtrip_test.go.
//  6. Write go.mod with a replace directive pointing at the local module root.
//  7. Copy go.sum from the module root.
//  8. Run go test -mod=mod -count=1 -run TestRoundTrip_Property ./...
func roundTripCheckCorpus(t *testing.T, dir, pkgName string, generatedSrc []byte, testSrc string) {
	t.Helper()

	tmpDir := t.TempDir()

	// Two levels up: internal/deltagen → internal → module root.
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	moduleRoot := filepath.Clean(filepath.Join(wd, "..", ".."))

	// Copy the fixture source file as snapshot.go in the temp module.
	fixtureDir := filepath.Join("testdata", "corpus", dir)
	entries, err := os.ReadDir(fixtureDir)
	if err != nil {
		t.Fatalf("readdir %s: %v", fixtureDir, err)
	}
	wroteFixture := false
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".go") {
			fixtureSrc, err := os.ReadFile(filepath.Join(fixtureDir, e.Name()))
			if err != nil {
				t.Fatalf("read fixture %s: %v", e.Name(), err)
			}
			if err := os.WriteFile(filepath.Join(tmpDir, "snapshot.go"), fixtureSrc, 0644); err != nil {
				t.Fatalf("write snapshot.go: %v", err)
			}
			wroteFixture = true
			break
		}
	}
	if !wroteFixture {
		t.Fatalf("no .go file found in %s", fixtureDir)
	}

	// Write the generated delta source and assert it is gofmt-clean.
	deltaPath := filepath.Join(tmpDir, "delta.go")
	if err := os.WriteFile(deltaPath, generatedSrc, 0644); err != nil {
		t.Fatalf("write delta.go: %v", err)
	}
	assertGofmtClean(t, deltaPath)

	// Write the round-trip property test source.
	if err := os.WriteFile(filepath.Join(tmpDir, "roundtrip_test.go"), []byte(testSrc), 0644); err != nil {
		t.Fatalf("write roundtrip_test.go: %v", err)
	}

	// Write go.mod with a replace directive pointing at the local module root.
	modContent := "module " + pkgName + "\n\ngo 1.25.0\n\nrequire go.resystems.io/eddt v0.0.0\n\nreplace go.resystems.io/eddt => " + moduleRoot + "\n"
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte(modContent), 0644); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}

	// Copy go.sum so transitive dependencies resolve locally.
	goSum, err := os.ReadFile(filepath.Join(moduleRoot, "go.sum"))
	if err != nil {
		t.Fatalf("read go.sum: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "go.sum"), goSum, 0644); err != nil {
		t.Fatalf("write go.sum: %v", err)
	}

	runBuildCmd(t, tmpDir, "go", "test", "-mod=mod", "-count=1", "-run", "TestRoundTrip_Property", "./...")
}

// baselineRoundTripTest is the injected round-trip property test for the baseline
// corpus case.  It asserts reflect.DeepEqual(Apply(a, Diff(a, b)), b) — full
// snapshot equality including Header — for 1000 random baselinePayload pairs.
//
// Suppressed fields (Hidden delta.omit, Legacy delta.retired) are excluded from
// the payload struct and left at zero in both a and b so full equality holds
// without complicating the random generator.
const baselineRoundTripTest = `package baseline_test

import (
	"reflect"
	"testing"
	"testing/quick"
	"time"

	"baseline"
	eddt "go.resystems.io/eddt/runtime"
)

// baselinePayload carries only the delta-carrying fields of BaselineSnapshot.
//
// Suppressed fields (Hidden delta.omit, Legacy delta.retired) are excluded:
// they are set to zero in both a and b so reflect.DeepEqual(got, b) holds.
// The entity key (Key) is fixed to "k" in both snapshots.
type baselinePayload struct {
	Name     string
	Priority *int32
	Meta     baseline.MetaInfo
	Tags     []string
	Attrs    map[string]string
	Score    int32
}

// TestRoundTrip_Property asserts reflect.DeepEqual(Apply(a, Diff(a, b)), b)
// for 1000 random baselinePayload pairs (full equality including Header).
// Both a and b are valid sequential chain entries: same EntityID and ChainID,
// b.Sequence = a.Sequence + 1, nil Provenance.
func TestRoundTrip_Property(t *testing.T) {
	fixedID := eddt.EntityID{1}
	now := time.Now()

	prop := func(ap, bp baselinePayload) bool {
		a := baseline.BaselineSnapshot{
			Header: eddt.Header{EntityID: fixedID, ChainID: "c",
				Sequence: 1, EffectiveAt: now},
			Key: "k", Name: ap.Name, Priority: ap.Priority,
			Meta: ap.Meta, Tags: ap.Tags, Attrs: ap.Attrs,
			Score: ap.Score,
		}
		b := baseline.BaselineSnapshot{
			Header: eddt.Header{EntityID: fixedID, ChainID: "c",
				Sequence: 2, EffectiveAt: now},
			Key: "k", Name: bp.Name, Priority: bp.Priority,
			Meta: bp.Meta, Tags: bp.Tags, Attrs: bp.Attrs,
			Score: bp.Score,
		}
		d, err := baseline.Diff(a, b)
		if err != nil {
			return false
		}
		got, err := baseline.Apply(a, d)
		if err != nil {
			return false
		}
		return reflect.DeepEqual(got, b)
	}
	if err := quick.Check(prop, &quick.Config{MaxCount: 1000}); err != nil {
		t.Errorf("round-trip property failed: %v", err)
	}
}
`

const compositeRoundTripTest = `package composite_test

import (
	"reflect"
	"sort"
	"testing"
	"testing/quick"
	"time"

	"composite"
	eddt "go.resystems.io/eddt/runtime"
)

// compositePayload carries the delta-carrying fields of CompositeSnapshot.
// The entity key (Key) is fixed to "k" in both snapshots.
type compositePayload struct {
	Details composite.ContactDetails
	Labels  map[string]string
	Groups  []string
	Rank    int32
}

// toSortedUnique returns a sorted, deduplicated copy of ss.
//
// N-04 set-diff semantics (E-15): Apply(a, Diff(a, b)).Groups contains the
// same unique elements as b.Groups but element order may differ (surviving
// a-elements in a-order; b-additions appended in b-order).  Duplicates in b
// are normalised to one entry by Diff (map-based membership set).
func toSortedUnique(ss []string) []string {
	seen := make(map[string]bool, len(ss))
	out := make([]string, 0, len(ss))
	for _, s := range ss {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

// snapshotEqual reports whether got and b satisfy the correct invariant for
// each field:
//
//   - Header:  reflect.DeepEqual (full; Provenance is nil on both sides).
//   - Key:     == (fixed string, same in both).
//   - Details: == (comparable struct, N-01 exact round-trip).
//   - Labels:  reflect.DeepEqual (exact key-value equality, N-03).
//   - Groups:  toSortedUnique equality (set-membership, N-04 E-15).
//   - Rank:    == (atomic scalar).
func snapshotEqual(got, b composite.CompositeSnapshot) bool {
	return reflect.DeepEqual(got.Header, b.Header) &&
		got.Key == b.Key &&
		got.Details == b.Details &&
		reflect.DeepEqual(got.Labels, b.Labels) &&
		reflect.DeepEqual(toSortedUnique(got.Groups), toSortedUnique(b.Groups)) &&
		got.Rank == b.Rank
}

// TestRoundTrip_Property asserts snapshotEqual(Apply(a, Diff(a, b)), b) for
// 1000 random compositePayload pairs.  Groups uses set-membership equality
// per N-04 E-15; all other fields use strict equality.
func TestRoundTrip_Property(t *testing.T) {
	fixedID := eddt.EntityID{1}
	now := time.Now()

	prop := func(ap, bp compositePayload) bool {
		a := composite.CompositeSnapshot{
			Header: eddt.Header{EntityID: fixedID, ChainID: "c",
				Sequence: 1, EffectiveAt: now},
			Key: "k", Details: ap.Details, Labels: ap.Labels,
			Groups: ap.Groups, Rank: ap.Rank,
		}
		b := composite.CompositeSnapshot{
			Header: eddt.Header{EntityID: fixedID, ChainID: "c",
				Sequence: 2, EffectiveAt: now},
			Key: "k", Details: bp.Details, Labels: bp.Labels,
			Groups: bp.Groups, Rank: bp.Rank,
		}
		d, err := composite.Diff(a, b)
		if err != nil {
			return false
		}
		got, err := composite.Apply(a, d)
		if err != nil {
			return false
		}
		return snapshotEqual(got, b)
	}
	if err := quick.Check(prop, &quick.Config{MaxCount: 1000}); err != nil {
		t.Errorf("round-trip property failed: %v", err)
	}
}
`

// structKeyRoundTripTest is the injected round-trip property test for the
// struct_key corpus case.  It asserts reflect.DeepEqual(Apply(a, Diff(a, b)), b)
// — full snapshot equality including Header — for 1000 random sessionPayload pairs.
//
// The struct-valued Key is fixed to the same value in both a and b, confirming
// that the struct-key EntityID hash (EM-05) does not interfere with Diff/Apply.
const structKeyRoundTripTest = `package struct_key_test

import (
	"reflect"
	"testing"
	"testing/quick"
	"time"

	"struct_key"
	eddt "go.resystems.io/eddt/runtime"
)

// sessionPayload carries the delta-carrying fields of SessionSnapshot.
// The struct-valued Key is fixed to the same value in both a and b.
type sessionPayload struct {
	State string
	Count int32
}

// TestRoundTrip_Property asserts reflect.DeepEqual(Apply(a, Diff(a, b)), b)
// for 1000 random sessionPayload pairs (full equality including Header).
// Both a and b are valid sequential chain entries: same EntityID and ChainID,
// b.Sequence = a.Sequence + 1, nil Provenance.
func TestRoundTrip_Property(t *testing.T) {
	fixedID := eddt.EntityID{1}
	fixedKey := struct_key.SessionKey{TenantID: "t", SessionN: 1}
	now := time.Now()

	prop := func(ap, bp sessionPayload) bool {
		a := struct_key.SessionSnapshot{
			Header: eddt.Header{EntityID: fixedID, ChainID: "c",
				Sequence: 1, EffectiveAt: now},
			Key: fixedKey, State: ap.State, Count: ap.Count,
		}
		b := struct_key.SessionSnapshot{
			Header: eddt.Header{EntityID: fixedID, ChainID: "c",
				Sequence: 2, EffectiveAt: now},
			Key: fixedKey, State: bp.State, Count: bp.Count,
		}
		d, err := struct_key.Diff(a, b)
		if err != nil {
			return false
		}
		got, err := struct_key.Apply(a, d)
		if err != nil {
			return false
		}
		return reflect.DeepEqual(got, b)
	}
	if err := quick.Check(prop, &quick.Config{MaxCount: 1000}); err != nil {
		t.Errorf("round-trip property failed: %v", err)
	}
}
`

// ── C-03: identity-diff property tests ───────────────────────────────────────

// TestConformance_Identity is the C-03 property test.
//
// For each corpus case it generates the delta source, injects a
// testing/quick identity-diff property test into an isolated temp module,
// and runs go test -run TestIdentity_Property.  The invariant tested:
//
//	reflect.DeepEqual(Apply(a, Diff(a, aprime)), aprime)
//
// where aprime is a struct copy of a with aprime.Header.Sequence = a.Sequence + 1.
// The diff produces a zero-payload delta (all Set* nil); Apply must preserve
// every field unchanged.  Full snapshot equality (including Header) is the
// correct assertion for all three corpus cases — no toSortedUnique needed
// because identity diff produces zero additions/removals, so N-04 slice order
// is preserved exactly.
func TestConformance_Identity(t *testing.T) {
	dispatchers := map[string]func(*testing.T, []byte){
		"baseline":   identityCheckBaseline,
		"composite":  identityCheckComposite,
		"struct_key": identityCheckStructKey,
	}
	for _, tc := range corpus {
		t.Run(tc.name, func(t *testing.T) {
			check := dispatchers[tc.dir]
			outPath := filepath.Join(t.TempDir(), "delta.go")
			cfg := Config{
				InputPkgs:     []string{"./testdata/corpus/" + tc.dir},
				TargetStructs: []string{tc.name},
				OutPath:       outPath,
			}
			if err := New(cfg).Run(); err != nil {
				t.Fatalf("Run(): %v", err)
			}
			src, err := os.ReadFile(outPath)
			if err != nil {
				t.Fatalf("read generated file: %v", err)
			}
			check(t, src)
		})
	}
}

// identityCheckBaseline runs the C-03 property test for the baseline corpus case.
//
// Injected invariant: reflect.DeepEqual(Apply(a, Diff(a, aprime)), aprime) —
// full equality including Header — for 1000 random baselinePayload values.
func identityCheckBaseline(t *testing.T, generatedSrc []byte) {
	t.Helper()
	identityCheckCorpus(t, "baseline", "baseline", generatedSrc, baselineIdentityTest)
}

// identityCheckComposite runs the C-03 property test for the composite corpus case.
//
// Injected invariant: reflect.DeepEqual(Apply(a, Diff(a, aprime)), aprime) —
// full equality including Header — for 1000 random compositePayload values.
// No toSortedUnique is needed: identity diff leaves Groups unchanged in order.
func identityCheckComposite(t *testing.T, generatedSrc []byte) {
	t.Helper()
	identityCheckCorpus(t, "composite", "composite", generatedSrc, compositeIdentityTest)
}

// identityCheckStructKey runs the C-03 property test for the struct_key corpus case.
//
// Injected invariant: reflect.DeepEqual(Apply(a, Diff(a, aprime)), aprime) —
// full equality including Header — for 1000 random sessionPayload values.
func identityCheckStructKey(t *testing.T, generatedSrc []byte) {
	t.Helper()
	identityCheckCorpus(t, "struct_key", "struct_key", generatedSrc, structKeyIdentityTest)
}

// identityCheckCorpus writes the corpus fixture, the generated delta source,
// and an injected testing/quick identity-diff property test into an isolated
// temp module and runs go test -run TestIdentity_Property.
//
// Steps mirror roundTripCheckCorpus exactly; only the injected test filename
// and -run pattern differ.
func identityCheckCorpus(t *testing.T, dir, pkgName string, generatedSrc []byte, testSrc string) {
	t.Helper()

	tmpDir := t.TempDir()

	// Two levels up: internal/deltagen → internal → module root.
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	moduleRoot := filepath.Clean(filepath.Join(wd, "..", ".."))

	// Copy the fixture source file as snapshot.go in the temp module.
	fixtureDir := filepath.Join("testdata", "corpus", dir)
	entries, err := os.ReadDir(fixtureDir)
	if err != nil {
		t.Fatalf("readdir %s: %v", fixtureDir, err)
	}
	wroteFixture := false
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".go") {
			fixtureSrc, err := os.ReadFile(filepath.Join(fixtureDir, e.Name()))
			if err != nil {
				t.Fatalf("read fixture %s: %v", e.Name(), err)
			}
			if err := os.WriteFile(filepath.Join(tmpDir, "snapshot.go"), fixtureSrc, 0644); err != nil {
				t.Fatalf("write snapshot.go: %v", err)
			}
			wroteFixture = true
			break
		}
	}
	if !wroteFixture {
		t.Fatalf("no .go file found in %s", fixtureDir)
	}

	// Write the generated delta source and assert it is gofmt-clean.
	deltaPath := filepath.Join(tmpDir, "delta.go")
	if err := os.WriteFile(deltaPath, generatedSrc, 0644); err != nil {
		t.Fatalf("write delta.go: %v", err)
	}
	assertGofmtClean(t, deltaPath)

	// Write the identity property test source.
	if err := os.WriteFile(filepath.Join(tmpDir, "identity_test.go"), []byte(testSrc), 0644); err != nil {
		t.Fatalf("write identity_test.go: %v", err)
	}

	// Write go.mod with a replace directive pointing at the local module root.
	modContent := "module " + pkgName + "\n\ngo 1.25.0\n\nrequire go.resystems.io/eddt v0.0.0\n\nreplace go.resystems.io/eddt => " + moduleRoot + "\n"
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte(modContent), 0644); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}

	// Copy go.sum so transitive dependencies resolve locally.
	goSum, err := os.ReadFile(filepath.Join(moduleRoot, "go.sum"))
	if err != nil {
		t.Fatalf("read go.sum: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "go.sum"), goSum, 0644); err != nil {
		t.Fatalf("write go.sum: %v", err)
	}

	runBuildCmd(t, tmpDir, "go", "test", "-mod=mod", "-count=1", "-run", "TestIdentity_Property", "./...")
}

// baselineIdentityTest is the injected identity-diff property test for the
// baseline corpus case.  It asserts reflect.DeepEqual(Apply(a, Diff(a, aprime)), aprime)
// — full snapshot equality including Header — for 1000 random baselinePayload values.
//
// aprime is a struct copy of a with aprime.Header.Sequence = a.Sequence + 1.
// Suppressed fields (Hidden delta.omit, Legacy delta.retired) are excluded from
// the payload struct and left at zero so full equality holds unconditionally.
const baselineIdentityTest = `package baseline_test

import (
	"reflect"
	"testing"
	"testing/quick"
	"time"

	"baseline"
	eddt "go.resystems.io/eddt/runtime"
)

// baselinePayload carries only delta-carrying fields of BaselineSnapshot.
//
// Hidden (delta.omit) and Legacy (delta.retired) are excluded: they are zero
// in both a and aprime so reflect.DeepEqual(got, aprime) holds unconditionally.
// The entity key (Key) is fixed to "k" in both snapshots.
type baselinePayload struct {
	Name     string
	Priority *int32
	Meta     baseline.MetaInfo
	Tags     []string
	Attrs    map[string]string
	Score    int32
}

// TestIdentity_Property asserts reflect.DeepEqual(Apply(a, Diff(a, aprime)), aprime)
// for 1000 random baselinePayload values (full equality including Header).
// aprime is a struct copy of a with aprime.Header.Sequence = a.Sequence + 1.
// The diff must be minimal (all Set* nil) and Apply must preserve every field.
func TestIdentity_Property(t *testing.T) {
	fixedID := eddt.EntityID{1}
	now := time.Now()

	prop := func(ap baselinePayload) bool {
		a := baseline.BaselineSnapshot{
			Header: eddt.Header{EntityID: fixedID, ChainID: "c",
				Sequence: 1, EffectiveAt: now},
			Key: "k", Name: ap.Name, Priority: ap.Priority,
			Meta: ap.Meta, Tags: ap.Tags, Attrs: ap.Attrs,
			Score: ap.Score,
		}
		aprime := a
		aprime.Header = eddt.Header{EntityID: fixedID, ChainID: "c",
			Sequence: 2, EffectiveAt: now}
		d, err := baseline.Diff(a, aprime)
		if err != nil {
			return false
		}
		got, err := baseline.Apply(a, d)
		if err != nil {
			return false
		}
		return reflect.DeepEqual(got, aprime)
	}
	if err := quick.Check(prop, &quick.Config{MaxCount: 1000}); err != nil {
		t.Errorf("identity property failed: %v", err)
	}
}
`

// compositeIdentityTest is the injected identity-diff property test for the
// composite corpus case.  It asserts reflect.DeepEqual(Apply(a, Diff(a, aprime)), aprime)
// — full snapshot equality including Header — for 1000 random compositePayload values.
//
// No toSortedUnique is needed: identity diff produces nil AddedGroups and
// RemovedGroups, so Apply reconstructs Groups in their original order.
const compositeIdentityTest = `package composite_test

import (
	"reflect"
	"testing"
	"testing/quick"
	"time"

	"composite"
	eddt "go.resystems.io/eddt/runtime"
)

// compositePayload carries the delta-carrying fields of CompositeSnapshot.
// The entity key (Key) is fixed to "k" in both snapshots.
type compositePayload struct {
	Details composite.ContactDetails
	Labels  map[string]string
	Groups  []string
	Rank    int32
}

// TestIdentity_Property asserts reflect.DeepEqual(Apply(a, Diff(a, aprime)), aprime)
// for 1000 random compositePayload values (full equality including Header).
// aprime is a struct copy of a with aprime.Header.Sequence = a.Sequence + 1.
// N-01/N-03/N-04 all produce zero-payload deltas; Apply must preserve every
// field without interference across the three delta.nested shapes.
func TestIdentity_Property(t *testing.T) {
	fixedID := eddt.EntityID{1}
	now := time.Now()

	prop := func(ap compositePayload) bool {
		a := composite.CompositeSnapshot{
			Header: eddt.Header{EntityID: fixedID, ChainID: "c",
				Sequence: 1, EffectiveAt: now},
			Key: "k", Details: ap.Details, Labels: ap.Labels,
			Groups: ap.Groups, Rank: ap.Rank,
		}
		aprime := a
		aprime.Header = eddt.Header{EntityID: fixedID, ChainID: "c",
			Sequence: 2, EffectiveAt: now}
		d, err := composite.Diff(a, aprime)
		if err != nil {
			return false
		}
		got, err := composite.Apply(a, d)
		if err != nil {
			return false
		}
		return reflect.DeepEqual(got, aprime)
	}
	if err := quick.Check(prop, &quick.Config{MaxCount: 1000}); err != nil {
		t.Errorf("identity property failed: %v", err)
	}
}
`

// structKeyIdentityTest is the injected identity-diff property test for the
// struct_key corpus case.  It asserts reflect.DeepEqual(Apply(a, Diff(a, aprime)), aprime)
// — full snapshot equality including Header — for 1000 random sessionPayload values.
//
// The struct-valued Key is fixed in both a and aprime, confirming that the
// struct-key EntityID hash (EM-05) does not interfere with payload Diff/Apply.
const structKeyIdentityTest = `package struct_key_test

import (
	"reflect"
	"testing"
	"testing/quick"
	"time"

	"struct_key"
	eddt "go.resystems.io/eddt/runtime"
)

// sessionPayload carries the delta-carrying fields of SessionSnapshot.
// The struct-valued Key is fixed to the same value in both a and aprime.
type sessionPayload struct {
	State string
	Count int32
}

// TestIdentity_Property asserts reflect.DeepEqual(Apply(a, Diff(a, aprime)), aprime)
// for 1000 random sessionPayload values (full equality including Header).
// aprime is a struct copy of a with aprime.Header.Sequence = a.Sequence + 1.
func TestIdentity_Property(t *testing.T) {
	fixedID  := eddt.EntityID{1}
	fixedKey := struct_key.SessionKey{TenantID: "t", SessionN: 1}
	now := time.Now()

	prop := func(ap sessionPayload) bool {
		a := struct_key.SessionSnapshot{
			Header: eddt.Header{EntityID: fixedID, ChainID: "c",
				Sequence: 1, EffectiveAt: now},
			Key: fixedKey, State: ap.State, Count: ap.Count,
		}
		aprime := a
		aprime.Header = eddt.Header{EntityID: fixedID, ChainID: "c",
			Sequence: 2, EffectiveAt: now}
		d, err := struct_key.Diff(a, aprime)
		if err != nil {
			return false
		}
		got, err := struct_key.Apply(a, d)
		if err != nil {
			return false
		}
		return reflect.DeepEqual(got, aprime)
	}
	if err := quick.Check(prop, &quick.Config{MaxCount: 1000}); err != nil {
		t.Errorf("identity property failed: %v", err)
	}
}
`

// compositeRoundTripTest is the injected round-trip property test for the composite
// corpus case.  It asserts snapshotEqual(Apply(a, Diff(a, b)), b) — full equality
// except Groups (N-04 E-15 set-membership) — for 1000 random compositePayload pairs.
