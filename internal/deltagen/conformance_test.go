package deltagen

// conformance_test.go implements C-02, C-03, and C-04 corpus conformance property tests.
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
// C-04 (TestConformance_Coalesce): For each corpus case, generates the delta
// source and injects a testing/quick coalesce-as-fold property test:
//
//	Coalesce(s0, [d1,d2,d3]) == s3
//
// where s0 is a zero-payload seed and s1/s2/s3 are built from random payloads
// p1/p2/p3.  Both chunkability split points are verified in the same prop
// function.  baseline and struct_key use reflect.DeepEqual; composite uses
// snapshotEqual with toSortedUnique for Groups (N-04 E-15 set-membership).
//
// Test matrix:
//
//	TestConformance_RoundTrip/BaselineSnapshot           (C-02)
//	TestConformance_RoundTrip/ClearableCompositeSnapshot (C-02)
//	TestConformance_RoundTrip/CompositeSnapshot          (C-02)
//	TestConformance_RoundTrip/SessionSnapshot            (C-02)
//	TestConformance_Identity/BaselineSnapshot            (C-03)
//	TestConformance_Identity/ClearableCompositeSnapshot  (C-03)
//	TestConformance_Identity/CompositeSnapshot           (C-03)
//	TestConformance_Identity/SessionSnapshot             (C-03)
//	TestConformance_Coalesce/BaselineSnapshot            (C-04)
//	TestConformance_Coalesce/ClearableCompositeSnapshot  (C-04)
//	TestConformance_Coalesce/CompositeSnapshot           (C-04)
//	TestConformance_Coalesce/SessionSnapshot             (C-04)
//	TestConformance_TruthTable/ClearableCompositeSnapshot (CL-08 §5.4)

import (
	"os"
	"path/filepath"
	"testing"
)

// ── conformanceAxis: 2-D test-source table (HK-13) ───────────────────────────

// conformanceAxis groups the injected test file, run pattern, and per-corpus
// test source strings for one property axis (roundtrip / identity / coalesce /
// truthtable).  Adding a new corpus case is a single srcs map entry.
type conformanceAxis struct {
	testFile   string
	runPattern string
	srcs       map[string]string
}

// conformanceAxes is the (axis → corpus → test source) lookup table.
// It references the const test-source strings declared later in this file.
var conformanceAxes = map[string]conformanceAxis{
	"roundtrip": {
		testFile:   "roundtrip_test.go",
		runPattern: "TestRoundTrip_Property",
		srcs: map[string]string{
			"baseline":              baselineRoundTripTest,
			"clearable_composite":   clearableCompositeRoundTripTest,
			"composite":             compositeRoundTripTest,
			"struct_key":            structKeyRoundTripTest,
			"struct_key_clearable": structKeyClearableRoundTripTest,
		},
	},
	"identity": {
		testFile:   "identity_test.go",
		runPattern: "TestIdentity_Property",
		srcs: map[string]string{
			"baseline":              baselineIdentityTest,
			"clearable_composite":   clearableCompositeIdentityTest,
			"composite":             compositeIdentityTest,
			"struct_key":            structKeyIdentityTest,
			"struct_key_clearable": structKeyClearableIdentityTest,
		},
	},
	"coalesce": {
		testFile:   "coalesce_test.go",
		runPattern: "TestCoalesce_Property",
		srcs: map[string]string{
			"baseline":              baselineCoalesceTest,
			"clearable_composite":   clearableCompositeCoalesceTest,
			"composite":             compositeCoalesceTest,
			"struct_key":            structKeyCoalesceTest,
			"struct_key_clearable": structKeyClearableCoalesceTest,
		},
	},
}

// generateCorpusDelta generates the delta source for tc and returns the bytes.
func generateCorpusDelta(t *testing.T, tc corpusCase) []byte {
	t.Helper()
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
	return src
}

// runCorpusProperty runs the injected property test for one (corpus, axis) cell.
func runCorpusProperty(t *testing.T, tc corpusCase, generatedSrc []byte, axis conformanceAxis) {
	t.Helper()
	runEmittedInModule(t, runOpts{
		pkgName:      tc.dir,
		fixtureDir:   filepath.Join("testdata", "corpus", tc.dir),
		generatedSrc: generatedSrc,
		extraFiles:   map[string]string{axis.testFile: axis.srcs[tc.dir]},
		runArgs:      []string{"-run", axis.runPattern, "./..."},
	})
}

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
	axis := conformanceAxes["roundtrip"]
	for _, tc := range corpus {
		t.Run(tc.name, func(t *testing.T) {
			runCorpusProperty(t, tc, generateCorpusDelta(t, tc), axis)
		})
	}
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
	axis := conformanceAxes["identity"]
	for _, tc := range corpus {
		t.Run(tc.name, func(t *testing.T) {
			runCorpusProperty(t, tc, generateCorpusDelta(t, tc), axis)
		})
	}
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
//
// Priority is reassigned to a fresh *int32 allocation with the same value so
// that a.Priority and aprime.Priority point to different addresses. This closes
// the documented gap (E-02/CL-10): struct-copy aliasing masks pointer-identity
// bugs, and testing/quick never produces equal-value/different-address pointers.
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
		// Reassign Priority to a distinct allocation with the same value so the
		// Diff minimality check below is not masked by pointer aliasing (CL-10).
		if a.Priority != nil {
			v := *a.Priority
			aprime.Priority = &v
		}
		d, err := baseline.Diff(a, aprime)
		if err != nil {
			return false
		}
		// Minimality: equal-value Priority must not produce a non-nil SetPriority.
		if d.SetPriority != nil {
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

// ── C-04: coalesce-as-fold property tests ────────────────────────────────────

// TestConformance_Coalesce is the C-04 property test.
//
// For each corpus case it generates the delta source, injects a
// testing/quick coalesce-as-fold property test into an isolated temp module,
// and runs go test -run TestCoalesce_Property.  Three invariants are tested:
//
//	Coalesce(s0, [d1,d2,d3]) == s3                              (fold equivalence)
//	Coalesce(Coalesce(s0,[d1]), [d2,d3]) == s3                  (chunkability split 1)
//	Coalesce(Coalesce(s0,[d1,d2]), [d3]) == s3                  (chunkability split 2)
//
// s0 is a fixed zero-payload seed; s1/s2/s3 are built from random payloads
// p1/p2/p3.  All three assertions are verified in the same prop function for
// 1000 random (p1,p2,p3) triples per corpus case.
func TestConformance_Coalesce(t *testing.T) {
	axis := conformanceAxes["coalesce"]
	for _, tc := range corpus {
		t.Run(tc.name, func(t *testing.T) {
			runCorpusProperty(t, tc, generateCorpusDelta(t, tc), axis)
		})
	}
}

// baselineCoalesceTest is the injected coalesce property test for the baseline
// corpus case.  It asserts fold equivalence and chunkability at both split points
// for 1000 random (p1, p2, p3) triples.
//
// Suppressed fields (Hidden delta.omit, Legacy delta.retired) are excluded from
// the payload struct and left at zero in every snapshot so reflect.DeepEqual
// holds unconditionally.
const baselineCoalesceTest = `package baseline_test

import (
	"reflect"
	"testing"
	"testing/quick"
	"time"

	"baseline"
	eddt "go.resystems.io/eddt/runtime"
)

// baselinePayload carries only delta-carrying fields of BaselineSnapshot.
// Hidden (delta.omit) and Legacy (delta.retired) are excluded and left at zero
// in every snapshot so reflect.DeepEqual(result, s3) holds unconditionally.
type baselinePayload struct {
	Name     string
	Priority *int32
	Meta     baseline.MetaInfo
	Tags     []string
	Attrs    map[string]string
	Score    int32
}

// snap constructs a sequential snapshot from a payload and sequence number.
func snap(fixedID eddt.EntityID, seq uint64, now time.Time, p baselinePayload) baseline.BaselineSnapshot {
	return baseline.BaselineSnapshot{
		Header: eddt.Header{EntityID: fixedID, ChainID: "c",
			Sequence: seq, EffectiveAt: now},
		Key: "k", Name: p.Name, Priority: p.Priority,
		Meta: p.Meta, Tags: p.Tags, Attrs: p.Attrs,
		Score: p.Score,
	}
}

// TestCoalesce_Property asserts the coalesce-as-fold invariant and chunkability
// across a 3-step chain for 1000 random (p1, p2, p3) triples.
//
// Chain: s0 (zero-payload seed) →d1→ s1 →d2→ s2 →d3→ s3
//
// Fold equivalence:   Coalesce(s0, [d1,d2,d3]) == s3
// Chunkability (1/2): Coalesce(Coalesce(s0,[d1]), [d2,d3]) == s3
// Chunkability (2/2): Coalesce(Coalesce(s0,[d1,d2]), [d3]) == s3
func TestCoalesce_Property(t *testing.T) {
	fixedID := eddt.EntityID{1}
	now := time.Now()

	s0 := baseline.BaselineSnapshot{
		Header: eddt.Header{EntityID: fixedID, ChainID: "c",
			Sequence: 0, EffectiveAt: now},
		Key: "k",
	}

	prop := func(p1, p2, p3 baselinePayload) bool {
		s1 := snap(fixedID, 1, now, p1)
		s2 := snap(fixedID, 2, now, p2)
		s3 := snap(fixedID, 3, now, p3)

		d1, err := baseline.Diff(s0, s1)
		if err != nil {
			return false
		}
		d2, err := baseline.Diff(s1, s2)
		if err != nil {
			return false
		}
		d3, err := baseline.Diff(s2, s3)
		if err != nil {
			return false
		}

		// Fold equivalence.
		full, err := baseline.Coalesce(s0, []baseline.BaselineSnapshotDelta{d1, d2, d3})
		if err != nil || !reflect.DeepEqual(full, s3) {
			return false
		}

		// Chunkability at split point 1.
		mid1, err := baseline.Coalesce(s0, []baseline.BaselineSnapshotDelta{d1})
		if err != nil {
			return false
		}
		chunk1, err := baseline.Coalesce(mid1, []baseline.BaselineSnapshotDelta{d2, d3})
		if err != nil || !reflect.DeepEqual(chunk1, s3) {
			return false
		}

		// Chunkability at split point 2.
		mid2, err := baseline.Coalesce(s0, []baseline.BaselineSnapshotDelta{d1, d2})
		if err != nil {
			return false
		}
		chunk2, err := baseline.Coalesce(mid2, []baseline.BaselineSnapshotDelta{d3})
		if err != nil {
			return false
		}
		return reflect.DeepEqual(chunk2, s3)
	}
	if err := quick.Check(prop, &quick.Config{MaxCount: 1000}); err != nil {
		t.Errorf("coalesce property failed: %v", err)
	}
}
`

// compositeCoalesceTest is the injected coalesce property test for the composite
// corpus case.  It asserts fold equivalence and chunkability at both split points
// for 1000 random (p1, p2, p3) triples.
//
// Groups uses snapshotEqual with toSortedUnique (N-04 E-15 set-membership):
// multi-step set-diffs preserve the group SET but not ORDER.
const compositeCoalesceTest = `package composite_test

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
// The entity key (Key) is fixed to "k" in all snapshots.
type compositePayload struct {
	Details composite.ContactDetails
	Labels  map[string]string
	Groups  []string
	Rank    int32
}

// snap constructs a sequential snapshot from a payload and sequence number.
func snap(fixedID eddt.EntityID, seq uint64, now time.Time, p compositePayload) composite.CompositeSnapshot {
	return composite.CompositeSnapshot{
		Header: eddt.Header{EntityID: fixedID, ChainID: "c",
			Sequence: seq, EffectiveAt: now},
		Key: "k", Details: p.Details, Labels: p.Labels,
		Groups: p.Groups, Rank: p.Rank,
	}
}

// toSortedUnique returns a sorted, deduplicated copy of ss.
// N-04 set-diff semantics: Apply preserves the group SET but not ORDER.
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

// snapshotEqual reports whether got and want satisfy the correct invariant:
// full equality for Header, Key, Details, Labels, Rank; set-membership for Groups.
func snapshotEqual(got, want composite.CompositeSnapshot) bool {
	return reflect.DeepEqual(got.Header, want.Header) &&
		got.Key == want.Key &&
		got.Details == want.Details &&
		reflect.DeepEqual(got.Labels, want.Labels) &&
		reflect.DeepEqual(toSortedUnique(got.Groups), toSortedUnique(want.Groups)) &&
		got.Rank == want.Rank
}

// TestCoalesce_Property asserts fold equivalence and chunkability for 1000
// random (p1, p2, p3) triples.  Groups uses set-membership equality (N-04 E-15).
func TestCoalesce_Property(t *testing.T) {
	fixedID := eddt.EntityID{1}
	now := time.Now()

	s0 := composite.CompositeSnapshot{
		Header: eddt.Header{EntityID: fixedID, ChainID: "c",
			Sequence: 0, EffectiveAt: now},
		Key: "k",
	}

	prop := func(p1, p2, p3 compositePayload) bool {
		s1 := snap(fixedID, 1, now, p1)
		s2 := snap(fixedID, 2, now, p2)
		s3 := snap(fixedID, 3, now, p3)

		d1, err := composite.Diff(s0, s1)
		if err != nil {
			return false
		}
		d2, err := composite.Diff(s1, s2)
		if err != nil {
			return false
		}
		d3, err := composite.Diff(s2, s3)
		if err != nil {
			return false
		}

		// Fold equivalence.
		full, err := composite.Coalesce(s0, []composite.CompositeSnapshotDelta{d1, d2, d3})
		if err != nil || !snapshotEqual(full, s3) {
			return false
		}

		// Chunkability at split point 1.
		mid1, err := composite.Coalesce(s0, []composite.CompositeSnapshotDelta{d1})
		if err != nil {
			return false
		}
		chunk1, err := composite.Coalesce(mid1, []composite.CompositeSnapshotDelta{d2, d3})
		if err != nil || !snapshotEqual(chunk1, s3) {
			return false
		}

		// Chunkability at split point 2.
		mid2, err := composite.Coalesce(s0, []composite.CompositeSnapshotDelta{d1, d2})
		if err != nil {
			return false
		}
		chunk2, err := composite.Coalesce(mid2, []composite.CompositeSnapshotDelta{d3})
		if err != nil {
			return false
		}
		return snapshotEqual(chunk2, s3)
	}
	if err := quick.Check(prop, &quick.Config{MaxCount: 1000}); err != nil {
		t.Errorf("coalesce property failed: %v", err)
	}
}
`

// clearableCompositeRoundTripTest is the injected round-trip property test for
// the clearable_composite corpus case.  It asserts snapshotEqual(Apply(a, Diff(a,b)), b)
// for 1000 random clearableCompositePayload pairs.
//
// Note: testing/quick never generates nil maps or nil slices, so the OpRetract
// row of the §5.4 truth-table is never exercised here.  It is covered
// deterministically by TestConformance_TruthTable.
const clearableCompositeRoundTripTest = `package clearable_composite_test

import (
	"reflect"
	"sort"
	"testing"
	"testing/quick"
	"time"

	"clearable_composite"
	eddt "go.resystems.io/eddt/runtime"
)

// clearableCompositePayload carries the delta-carrying fields of
// ClearableCompositeSnapshot.  The entity key (Key) is fixed to "k" in both
// snapshots.  Header fields are set directly; only payload varies under quick.
type clearableCompositePayload struct {
	Location clearable_composite.Address
	Tags     map[string]string
	Groups   []string
	Count    int32
}

// toSortedUnique returns a sorted, deduplicated copy of ss.
//
// N-04 set-diff semantics (E-15): Apply(a, Diff(a, b)).Groups contains the
// same unique elements as b.Groups but element order may differ.
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

// snapshotEqual reports whether got and b satisfy the correct invariant:
//
//   - Header:   reflect.DeepEqual (full; Provenance is nil on both sides).
//   - Key:      == (fixed string, same in both).
//   - Location: reflect.DeepEqual (struct value; OpAssert carries inner AddressDelta).
//   - Tags:     reflect.DeepEqual (direct; Option A ensures nil-only triggers OpRetract,
//               and testing/quick never generates nil maps, so all quick cases round-trip exactly).
//   - Groups:   toSortedUnique equality (nil/empty-slice equivalent; set-membership N-04 E-15).
//   - Count:    == (atomic scalar).
func snapshotEqual(got, b clearable_composite.ClearableCompositeSnapshot) bool {
	return reflect.DeepEqual(got.Header, b.Header) &&
		got.Key == b.Key &&
		reflect.DeepEqual(got.Location, b.Location) &&
		reflect.DeepEqual(got.Tags, b.Tags) &&
		reflect.DeepEqual(toSortedUnique(got.Groups), toSortedUnique(b.Groups)) &&
		got.Count == b.Count
}

// TestRoundTrip_Property asserts snapshotEqual(Apply(a, Diff(a, b)), b) for
// 1000 random clearableCompositePayload pairs.
func TestRoundTrip_Property(t *testing.T) {
	fixedID := eddt.EntityID{1}
	now := time.Now()

	prop := func(ap, bp clearableCompositePayload) bool {
		a := clearable_composite.ClearableCompositeSnapshot{
			Header: eddt.Header{EntityID: fixedID, ChainID: "c",
				Sequence: 1, EffectiveAt: now},
			Key:      "k",
			Location: ap.Location,
			Tags:     ap.Tags,
			Groups:   ap.Groups,
			Count:    ap.Count,
		}
		b := clearable_composite.ClearableCompositeSnapshot{
			Header: eddt.Header{EntityID: fixedID, ChainID: "c",
				Sequence: 2, EffectiveAt: now},
			Key:      "k",
			Location: bp.Location,
			Tags:     bp.Tags,
			Groups:   bp.Groups,
			Count:    bp.Count,
		}
		d, err := clearable_composite.Diff(a, b)
		if err != nil {
			return false
		}
		got, err := clearable_composite.Apply(a, d)
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

// clearableCompositeIdentityTest is the injected identity-diff property test for
// the clearable_composite corpus case.  It asserts reflect.DeepEqual(Apply(a, Diff(a, aprime)), aprime)
// for 1000 random clearableCompositePayload values.
//
// When payload(a) == payload(aprime), all three FieldDelta fields must have
// Op == OpIgnore (zero value) and Apply must preserve every field unchanged.
const clearableCompositeIdentityTest = `package clearable_composite_test

import (
	"reflect"
	"testing"
	"testing/quick"
	"time"

	"clearable_composite"
	eddt "go.resystems.io/eddt/runtime"
)

// clearableCompositePayload carries the delta-carrying fields.
// Key is fixed to "k"; Header is set directly.
type clearableCompositePayload struct {
	Location clearable_composite.Address
	Tags     map[string]string
	Groups   []string
	Count    int32
}

// TestIdentity_Property asserts reflect.DeepEqual(Apply(a, Diff(a, aprime)), aprime)
// for 1000 random payloads.  aprime is a struct copy of a with Sequence incremented.
// All three FieldDelta fields must be OpIgnore (zero) and Apply must preserve every field.
func TestIdentity_Property(t *testing.T) {
	fixedID := eddt.EntityID{1}
	now := time.Now()

	prop := func(ap clearableCompositePayload) bool {
		a := clearable_composite.ClearableCompositeSnapshot{
			Header: eddt.Header{EntityID: fixedID, ChainID: "c",
				Sequence: 1, EffectiveAt: now},
			Key:      "k",
			Location: ap.Location,
			Tags:     ap.Tags,
			Groups:   ap.Groups,
			Count:    ap.Count,
		}
		aprime := a
		aprime.Header = eddt.Header{EntityID: fixedID, ChainID: "c",
			Sequence: 2, EffectiveAt: now}
		d, err := clearable_composite.Diff(a, aprime)
		if err != nil {
			return false
		}
		got, err := clearable_composite.Apply(a, d)
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

// clearableCompositeCoalesceTest is the injected coalesce property test for the
// clearable_composite corpus case.  It asserts fold equivalence and chunkability
// at both split points for 1000 random (p1, p2, p3) triples.
//
// Groups uses snapshotEqual with toSortedUnique (N-04 E-15 set-membership):
// multi-step set-diffs preserve the group SET but not ORDER.
const clearableCompositeCoalesceTest = `package clearable_composite_test

import (
	"reflect"
	"sort"
	"testing"
	"testing/quick"
	"time"

	"clearable_composite"
	eddt "go.resystems.io/eddt/runtime"
)

// clearableCompositePayload carries the delta-carrying fields.
// Key is fixed to "k"; Header is set directly.
type clearableCompositePayload struct {
	Location clearable_composite.Address
	Tags     map[string]string
	Groups   []string
	Count    int32
}

// snap constructs a sequential snapshot from a payload and sequence number.
func snap(fixedID eddt.EntityID, seq uint64, now time.Time, p clearableCompositePayload) clearable_composite.ClearableCompositeSnapshot {
	return clearable_composite.ClearableCompositeSnapshot{
		Header: eddt.Header{EntityID: fixedID, ChainID: "c",
			Sequence: seq, EffectiveAt: now},
		Key:      "k",
		Location: p.Location,
		Tags:     p.Tags,
		Groups:   p.Groups,
		Count:    p.Count,
	}
}

// toSortedUnique returns a sorted, deduplicated copy of ss.
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

// normTags normalizes an empty-but-non-nil map to nil for comparison.
//
// The coalesce seed s0 has nil Tags.  When all deltas in the fold are OpIgnore
// for Tags (e.g. nil→{}→{}→{} chains produce empty inner diffs), the coalesced
// result preserves nil while sN.Tags is an empty-but-non-nil map from
// testing/quick.  nil and {} are semantically equivalent per E-17, so normTags
// equalizes them for the fold-equivalence check.
func normTags(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	return m
}

// snapshotEqual reports whether got and want satisfy the correct invariant:
// full equality for Header, Key, Location, Count; normTags for Tags (E-17 nil/empty fold edge);
// set-membership for Groups (N-04 E-15).
func snapshotEqual(got, want clearable_composite.ClearableCompositeSnapshot) bool {
	return reflect.DeepEqual(got.Header, want.Header) &&
		got.Key == want.Key &&
		reflect.DeepEqual(got.Location, want.Location) &&
		reflect.DeepEqual(normTags(got.Tags), normTags(want.Tags)) &&
		reflect.DeepEqual(toSortedUnique(got.Groups), toSortedUnique(want.Groups)) &&
		got.Count == want.Count
}

// TestCoalesce_Property asserts fold equivalence and chunkability for 1000
// random (p1, p2, p3) triples.  Groups uses set-membership equality (N-04 E-15).
func TestCoalesce_Property(t *testing.T) {
	fixedID := eddt.EntityID{1}
	now := time.Now()

	s0 := clearable_composite.ClearableCompositeSnapshot{
		Header: eddt.Header{EntityID: fixedID, ChainID: "c",
			Sequence: 0, EffectiveAt: now},
		Key: "k",
	}

	prop := func(p1, p2, p3 clearableCompositePayload) bool {
		s1 := snap(fixedID, 1, now, p1)
		s2 := snap(fixedID, 2, now, p2)
		s3 := snap(fixedID, 3, now, p3)

		d1, err := clearable_composite.Diff(s0, s1)
		if err != nil {
			return false
		}
		d2, err := clearable_composite.Diff(s1, s2)
		if err != nil {
			return false
		}
		d3, err := clearable_composite.Diff(s2, s3)
		if err != nil {
			return false
		}

		// Fold equivalence.
		full, err := clearable_composite.Coalesce(s0, []clearable_composite.ClearableCompositeSnapshotDelta{d1, d2, d3})
		if err != nil || !snapshotEqual(full, s3) {
			return false
		}

		// Chunkability at split point 1.
		mid1, err := clearable_composite.Coalesce(s0, []clearable_composite.ClearableCompositeSnapshotDelta{d1})
		if err != nil {
			return false
		}
		chunk1, err := clearable_composite.Coalesce(mid1, []clearable_composite.ClearableCompositeSnapshotDelta{d2, d3})
		if err != nil || !snapshotEqual(chunk1, s3) {
			return false
		}

		// Chunkability at split point 2.
		mid2, err := clearable_composite.Coalesce(s0, []clearable_composite.ClearableCompositeSnapshotDelta{d1, d2})
		if err != nil {
			return false
		}
		chunk2, err := clearable_composite.Coalesce(mid2, []clearable_composite.ClearableCompositeSnapshotDelta{d3})
		if err != nil {
			return false
		}
		return snapshotEqual(chunk2, s3)
	}
	if err := quick.Check(prop, &quick.Config{MaxCount: 1000}); err != nil {
		t.Errorf("coalesce property failed: %v", err)
	}
}
`

// structKeyCoalesceTest is the injected coalesce property test for the struct_key
// corpus case.  It asserts fold equivalence and chunkability at both split points
// for 1000 random (p1, p2, p3) triples.
//
// The struct-valued Key is fixed across all snapshots, confirming that the
// struct-key EntityID hash (EM-05) does not corrupt payload Diff/Apply across a fold.
const structKeyCoalesceTest = `package struct_key_test

import (
	"reflect"
	"testing"
	"testing/quick"
	"time"

	"struct_key"
	eddt "go.resystems.io/eddt/runtime"
)

// sessionPayload carries the delta-carrying fields of SessionSnapshot.
// The struct-valued Key is fixed to the same value in all snapshots.
type sessionPayload struct {
	State string
	Count int32
}

// snap constructs a sequential snapshot from a payload and sequence number.
func snap(fixedID eddt.EntityID, seq uint64, now time.Time, p sessionPayload, key struct_key.SessionKey) struct_key.SessionSnapshot {
	return struct_key.SessionSnapshot{
		Header: eddt.Header{EntityID: fixedID, ChainID: "c",
			Sequence: seq, EffectiveAt: now},
		Key: key, State: p.State, Count: p.Count,
	}
}

// TestCoalesce_Property asserts fold equivalence and chunkability for 1000
// random (p1, p2, p3) triples.  The struct-valued Key is fixed across all snapshots.
func TestCoalesce_Property(t *testing.T) {
	fixedID := eddt.EntityID{1}
	fixedKey := struct_key.SessionKey{TenantID: "t", SessionN: 1}
	now := time.Now()

	s0 := struct_key.SessionSnapshot{
		Header: eddt.Header{EntityID: fixedID, ChainID: "c",
			Sequence: 0, EffectiveAt: now},
		Key: fixedKey,
	}

	prop := func(p1, p2, p3 sessionPayload) bool {
		s1 := snap(fixedID, 1, now, p1, fixedKey)
		s2 := snap(fixedID, 2, now, p2, fixedKey)
		s3 := snap(fixedID, 3, now, p3, fixedKey)

		d1, err := struct_key.Diff(s0, s1)
		if err != nil {
			return false
		}
		d2, err := struct_key.Diff(s1, s2)
		if err != nil {
			return false
		}
		d3, err := struct_key.Diff(s2, s3)
		if err != nil {
			return false
		}

		// Fold equivalence.
		full, err := struct_key.Coalesce(s0, []struct_key.SessionSnapshotDelta{d1, d2, d3})
		if err != nil || !reflect.DeepEqual(full, s3) {
			return false
		}

		// Chunkability at split point 1.
		mid1, err := struct_key.Coalesce(s0, []struct_key.SessionSnapshotDelta{d1})
		if err != nil {
			return false
		}
		chunk1, err := struct_key.Coalesce(mid1, []struct_key.SessionSnapshotDelta{d2, d3})
		if err != nil || !reflect.DeepEqual(chunk1, s3) {
			return false
		}

		// Chunkability at split point 2.
		mid2, err := struct_key.Coalesce(s0, []struct_key.SessionSnapshotDelta{d1, d2})
		if err != nil {
			return false
		}
		chunk2, err := struct_key.Coalesce(mid2, []struct_key.SessionSnapshotDelta{d3})
		if err != nil {
			return false
		}
		return reflect.DeepEqual(chunk2, s3)
	}
	if err := quick.Check(prop, &quick.Config{MaxCount: 1000}); err != nil {
		t.Errorf("coalesce property failed: %v", err)
	}
}
`

// ── HK-16: struct_key_clearable property tests ───────────────────────────────

// structKeyClearableRoundTripTest is the injected round-trip property test for
// the struct_key_clearable corpus case.  It asserts snapshotEqual(Apply(a, Diff(a,b)), b)
// for 1000 random skcPayload pairs.
//
// The struct-valued Key is fixed across a and b; clearable fields (Home, Labels,
// Tags) exercise the tri-state envelope.  testing/quick never generates nil maps
// or nil slices, so OpRetract for Labels and Tags is not exercised here.
const structKeyClearableRoundTripTest = `package struct_key_clearable_test

import (
	"reflect"
	"sort"
	"testing"
	"testing/quick"
	"time"

	"struct_key_clearable"
	eddt "go.resystems.io/eddt/runtime"
)

// skcPayload carries the delta-carrying fields of StructKeyClearableSnapshot.
// Key is fixed to the same value in both a and b.
type skcPayload struct {
	Home   struct_key_clearable.SkcAddress
	Labels map[string]string
	Tags   []string
	Score  float64
}

// toSortedUnique returns a sorted, deduplicated copy of ss.
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

// snapshotEqual applies correct invariants per field shape.
// Tags uses toSortedUnique (N-04 set-diff semantics, E-15).
func snapshotEqual(got, b struct_key_clearable.StructKeyClearableSnapshot) bool {
	return reflect.DeepEqual(got.Header, b.Header) &&
		got.Key == b.Key &&
		got.Home == b.Home &&
		reflect.DeepEqual(got.Labels, b.Labels) &&
		reflect.DeepEqual(toSortedUnique(got.Tags), toSortedUnique(b.Tags)) &&
		got.Score == b.Score
}

// TestRoundTrip_Property asserts snapshotEqual(Apply(a, Diff(a, b)), b) for
// 1000 random skcPayload pairs.  The struct-valued Key is fixed.
func TestRoundTrip_Property(t *testing.T) {
	fixedID  := eddt.EntityID{1}
	fixedKey := struct_key_clearable.SkcKey{ID: "t1", Shard: 42}
	now := time.Now()

	prop := func(ap, bp skcPayload) bool {
		a := struct_key_clearable.StructKeyClearableSnapshot{
			Header: eddt.Header{EntityID: fixedID, ChainID: "c",
				Sequence: 1, EffectiveAt: now},
			Key: fixedKey, Home: ap.Home, Labels: ap.Labels,
			Tags: ap.Tags, Score: ap.Score,
		}
		b := struct_key_clearable.StructKeyClearableSnapshot{
			Header: eddt.Header{EntityID: fixedID, ChainID: "c",
				Sequence: 2, EffectiveAt: now},
			Key: fixedKey, Home: bp.Home, Labels: bp.Labels,
			Tags: bp.Tags, Score: bp.Score,
		}
		d, err := struct_key_clearable.Diff(a, b)
		if err != nil {
			return false
		}
		got, err := struct_key_clearable.Apply(a, d)
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

// structKeyClearableIdentityTest is the injected identity-diff property test
// for the struct_key_clearable corpus case.
const structKeyClearableIdentityTest = `package struct_key_clearable_test

import (
	"reflect"
	"testing"
	"testing/quick"
	"time"

	"struct_key_clearable"
	eddt "go.resystems.io/eddt/runtime"
)

type skcPayload struct {
	Home   struct_key_clearable.SkcAddress
	Labels map[string]string
	Tags   []string
	Score  float64
}

// TestIdentity_Property asserts reflect.DeepEqual(Apply(a, Diff(a, aprime)), aprime)
// for 1000 random payloads.  aprime is a struct copy of a with Sequence incremented.
func TestIdentity_Property(t *testing.T) {
	fixedID  := eddt.EntityID{1}
	fixedKey := struct_key_clearable.SkcKey{ID: "t1", Shard: 42}
	now := time.Now()

	prop := func(ap skcPayload) bool {
		a := struct_key_clearable.StructKeyClearableSnapshot{
			Header: eddt.Header{EntityID: fixedID, ChainID: "c",
				Sequence: 1, EffectiveAt: now},
			Key: fixedKey, Home: ap.Home, Labels: ap.Labels,
			Tags: ap.Tags, Score: ap.Score,
		}
		aprime := a
		aprime.Header = eddt.Header{EntityID: fixedID, ChainID: "c",
			Sequence: 2, EffectiveAt: now}
		d, err := struct_key_clearable.Diff(a, aprime)
		if err != nil {
			return false
		}
		got, err := struct_key_clearable.Apply(a, d)
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

// structKeyClearableCoalesceTest is the injected coalesce property test for
// the struct_key_clearable corpus case.
const structKeyClearableCoalesceTest = `package struct_key_clearable_test

import (
	"reflect"
	"sort"
	"testing"
	"testing/quick"
	"time"

	"struct_key_clearable"
	eddt "go.resystems.io/eddt/runtime"
)

type skcPayload struct {
	Home   struct_key_clearable.SkcAddress
	Labels map[string]string
	Tags   []string
	Score  float64
}

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

func snapshotEqual(got, b struct_key_clearable.StructKeyClearableSnapshot) bool {
	return reflect.DeepEqual(got.Header, b.Header) &&
		got.Key == b.Key &&
		got.Home == b.Home &&
		reflect.DeepEqual(got.Labels, b.Labels) &&
		reflect.DeepEqual(toSortedUnique(got.Tags), toSortedUnique(b.Tags)) &&
		got.Score == b.Score
}

func snap(fixedID eddt.EntityID, seq uint64, now time.Time, p skcPayload, key struct_key_clearable.SkcKey) struct_key_clearable.StructKeyClearableSnapshot {
	return struct_key_clearable.StructKeyClearableSnapshot{
		Header: eddt.Header{EntityID: fixedID, ChainID: "c",
			Sequence: seq, EffectiveAt: now},
		Key: key, Home: p.Home, Labels: p.Labels,
		Tags: p.Tags, Score: p.Score,
	}
}

// TestCoalesce_Property asserts fold equivalence and chunkability for 1000
// random (p1, p2, p3) triples.  The struct-valued Key is fixed across all snapshots.
func TestCoalesce_Property(t *testing.T) {
	fixedID  := eddt.EntityID{1}
	fixedKey := struct_key_clearable.SkcKey{ID: "t1", Shard: 42}
	now := time.Now()

	s0 := struct_key_clearable.StructKeyClearableSnapshot{
		Header: eddt.Header{EntityID: fixedID, ChainID: "c",
			Sequence: 0, EffectiveAt: now},
		Key: fixedKey,
	}

	prop := func(p1, p2, p3 skcPayload) bool {
		s1 := snap(fixedID, 1, now, p1, fixedKey)
		s2 := snap(fixedID, 2, now, p2, fixedKey)
		s3 := snap(fixedID, 3, now, p3, fixedKey)

		d1, err := struct_key_clearable.Diff(s0, s1)
		if err != nil { return false }
		d2, err := struct_key_clearable.Diff(s1, s2)
		if err != nil { return false }
		d3, err := struct_key_clearable.Diff(s2, s3)
		if err != nil { return false }

		full, err := struct_key_clearable.Coalesce(s0, []struct_key_clearable.StructKeyClearableSnapshotDelta{d1, d2, d3})
		if err != nil || !snapshotEqual(full, s3) { return false }

		mid1, err := struct_key_clearable.Coalesce(s0, []struct_key_clearable.StructKeyClearableSnapshotDelta{d1})
		if err != nil { return false }
		chunk1, err := struct_key_clearable.Coalesce(mid1, []struct_key_clearable.StructKeyClearableSnapshotDelta{d2, d3})
		if err != nil || !snapshotEqual(chunk1, s3) { return false }

		mid2, err := struct_key_clearable.Coalesce(s0, []struct_key_clearable.StructKeyClearableSnapshotDelta{d1, d2})
		if err != nil { return false }
		chunk2, err := struct_key_clearable.Coalesce(mid2, []struct_key_clearable.StructKeyClearableSnapshotDelta{d3})
		if err != nil || !snapshotEqual(chunk2, s3) { return false }

		return true
	}
	if err := quick.Check(prop, &quick.Config{MaxCount: 1000}); err != nil {
		t.Errorf("coalesce property failed: %v", err)
	}
}
`

// ── CL-08 §5.4 truth-table test ──────────────────────────────────────────────

// TestConformance_TruthTable is the CL-08 deterministic §5.4 truth-table test.
//
// For each corpus case that has at least one clearable field it generates the
// delta source, injects a table-driven truth-table test into an isolated temp
// module, and runs go test -run TestTruthTable_All.  The test is deterministic
// (no testing/quick) and is the only place where the OpRetract row is exercised,
// because testing/quick never generates nil maps or nil slices.
//
// Five §5.4 rows tested per shape (Location / Tags / Groups):
//
//	equal composites              → OpIgnore
//	both zero composites          → OpIgnore
//	a zero, b non-zero composite  → OpAssert
//	a non-zero, b zero composite  → OpRetract
//	a non-zero, b different       → OpAssert
func TestConformance_TruthTable(t *testing.T) {
	axis := conformanceAxis{
		testFile:   "truthtable_test.go",
		runPattern: "TestTruthTable_All",
		srcs:       map[string]string{"clearable_composite": clearableCompositeTruthTableTest},
	}
	for _, tc := range corpus {
		if _, ok := axis.srcs[tc.dir]; !ok {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			runCorpusProperty(t, tc, generateCorpusDelta(t, tc), axis)
		})
	}
}

// clearableCompositeTruthTableTest is the injected §5.4 truth-table test for the
// clearable_composite corpus case.  Fully deterministic (no testing/quick).
// It is the only place OpRetract is exercised (testing/quick never generates nil
// maps / slices).
const clearableCompositeTruthTableTest = `package clearable_composite_test

import (
	"reflect"
	"testing"
	"time"

	"clearable_composite"
	eddt "go.resystems.io/eddt/runtime"
)

var (
	baseLocation = clearable_composite.Address{Street: "1 Main St", City: "Anytown"}
	baseTags     = map[string]string{"env": "prod"}
	baseGroups   = []string{"alpha"}
)

func TestTruthTable_All(t *testing.T) {
	now := time.Now()

	mkSnap := func(seq uint64, loc clearable_composite.Address, tags map[string]string, groups []string) clearable_composite.ClearableCompositeSnapshot {
		return clearable_composite.ClearableCompositeSnapshot{
			Header:   eddt.Header{EntityID: eddt.EntityID{1}, ChainID: "c", Sequence: seq, EffectiveAt: now},
			Key:      "k",
			Location: loc,
			Tags:     tags,
			Groups:   groups,
			Count:    42,
		}
	}

	// ── Location (struct clearable) ──────────────────────────────────────────

	t.Run("Location_equal", func(t *testing.T) {
		a := mkSnap(1, baseLocation, baseTags, baseGroups)
		b := mkSnap(2, baseLocation, baseTags, baseGroups)
		d, err := clearable_composite.Diff(a, b)
		if err != nil {
			t.Fatalf("Diff: %v", err)
		}
		if d.Location.Op != eddt.OpIgnore {
			t.Errorf("Location.Op = %v, want OpIgnore", d.Location.Op)
		}
		got, err := clearable_composite.Apply(a, d)
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if !reflect.DeepEqual(got.Location, b.Location) {
			t.Errorf("Apply Location = %v, want %v", got.Location, b.Location)
		}
	})

	t.Run("Location_bothZero", func(t *testing.T) {
		a := mkSnap(1, clearable_composite.Address{}, baseTags, baseGroups)
		b := mkSnap(2, clearable_composite.Address{}, baseTags, baseGroups)
		d, err := clearable_composite.Diff(a, b)
		if err != nil {
			t.Fatalf("Diff: %v", err)
		}
		if d.Location.Op != eddt.OpIgnore {
			t.Errorf("Location.Op = %v, want OpIgnore", d.Location.Op)
		}
	})

	t.Run("Location_zeroToNonZero", func(t *testing.T) {
		a := mkSnap(1, clearable_composite.Address{}, baseTags, baseGroups)
		b := mkSnap(2, baseLocation, baseTags, baseGroups)
		d, err := clearable_composite.Diff(a, b)
		if err != nil {
			t.Fatalf("Diff: %v", err)
		}
		if d.Location.Op != eddt.OpAssert {
			t.Errorf("Location.Op = %v, want OpAssert", d.Location.Op)
		}
		got, err := clearable_composite.Apply(a, d)
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if !reflect.DeepEqual(got.Location, b.Location) {
			t.Errorf("Apply Location = %v, want %v", got.Location, b.Location)
		}
	})

	t.Run("Location_nonZeroToZero", func(t *testing.T) {
		a := mkSnap(1, baseLocation, baseTags, baseGroups)
		b := mkSnap(2, clearable_composite.Address{}, baseTags, baseGroups)
		d, err := clearable_composite.Diff(a, b)
		if err != nil {
			t.Fatalf("Diff: %v", err)
		}
		if d.Location.Op != eddt.OpRetract {
			t.Errorf("Location.Op = %v, want OpRetract", d.Location.Op)
		}
		got, err := clearable_composite.Apply(a, d)
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if got.Location != (clearable_composite.Address{}) {
			t.Errorf("Apply Location = %v, want zero Address", got.Location)
		}
	})

	t.Run("Location_different", func(t *testing.T) {
		other := clearable_composite.Address{Street: "99 Oak Ave", City: "Elsewhere"}
		a := mkSnap(1, baseLocation, baseTags, baseGroups)
		b := mkSnap(2, other, baseTags, baseGroups)
		d, err := clearable_composite.Diff(a, b)
		if err != nil {
			t.Fatalf("Diff: %v", err)
		}
		if d.Location.Op != eddt.OpAssert {
			t.Errorf("Location.Op = %v, want OpAssert", d.Location.Op)
		}
		got, err := clearable_composite.Apply(a, d)
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if !reflect.DeepEqual(got.Location, b.Location) {
			t.Errorf("Apply Location = %v, want %v", got.Location, b.Location)
		}
	})

	// ── Tags (map clearable) ─────────────────────────────────────────────────

	t.Run("Tags_equal", func(t *testing.T) {
		a := mkSnap(1, baseLocation, map[string]string{"env": "prod"}, baseGroups)
		b := mkSnap(2, baseLocation, map[string]string{"env": "prod"}, baseGroups)
		d, err := clearable_composite.Diff(a, b)
		if err != nil {
			t.Fatalf("Diff: %v", err)
		}
		if d.Tags.Op != eddt.OpIgnore {
			t.Errorf("Tags.Op = %v, want OpIgnore", d.Tags.Op)
		}
		got, err := clearable_composite.Apply(a, d)
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if !reflect.DeepEqual(got.Tags, b.Tags) {
			t.Errorf("Apply Tags = %v, want %v", got.Tags, b.Tags)
		}
	})

	t.Run("Tags_bothNil", func(t *testing.T) {
		a := mkSnap(1, baseLocation, nil, baseGroups)
		b := mkSnap(2, baseLocation, nil, baseGroups)
		d, err := clearable_composite.Diff(a, b)
		if err != nil {
			t.Fatalf("Diff: %v", err)
		}
		if d.Tags.Op != eddt.OpIgnore {
			t.Errorf("Tags.Op = %v, want OpIgnore", d.Tags.Op)
		}
	})

	t.Run("Tags_nilToNonNil", func(t *testing.T) {
		a := mkSnap(1, baseLocation, nil, baseGroups)
		b := mkSnap(2, baseLocation, map[string]string{"k": "v"}, baseGroups)
		d, err := clearable_composite.Diff(a, b)
		if err != nil {
			t.Fatalf("Diff: %v", err)
		}
		if d.Tags.Op != eddt.OpAssert {
			t.Errorf("Tags.Op = %v, want OpAssert", d.Tags.Op)
		}
		got, err := clearable_composite.Apply(a, d)
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if !reflect.DeepEqual(got.Tags, b.Tags) {
			t.Errorf("Apply Tags = %v, want %v", got.Tags, b.Tags)
		}
	})

	t.Run("Tags_nonNilToNil", func(t *testing.T) {
		a := mkSnap(1, baseLocation, map[string]string{"k": "v"}, baseGroups)
		b := mkSnap(2, baseLocation, nil, baseGroups)
		d, err := clearable_composite.Diff(a, b)
		if err != nil {
			t.Fatalf("Diff: %v", err)
		}
		if d.Tags.Op != eddt.OpRetract {
			t.Errorf("Tags.Op = %v, want OpRetract", d.Tags.Op)
		}
		got, err := clearable_composite.Apply(a, d)
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if got.Tags != nil {
			t.Errorf("Apply Tags = %v, want nil", got.Tags)
		}
	})

	t.Run("Tags_different", func(t *testing.T) {
		a := mkSnap(1, baseLocation, map[string]string{"k": "old"}, baseGroups)
		b := mkSnap(2, baseLocation, map[string]string{"k": "new"}, baseGroups)
		d, err := clearable_composite.Diff(a, b)
		if err != nil {
			t.Fatalf("Diff: %v", err)
		}
		if d.Tags.Op != eddt.OpAssert {
			t.Errorf("Tags.Op = %v, want OpAssert", d.Tags.Op)
		}
		if d.Tags.Value.IsEmpty() {
			t.Errorf("Tags.Value.IsEmpty() = true, want non-empty inner delta")
		}
		got, err := clearable_composite.Apply(a, d)
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if !reflect.DeepEqual(got.Tags, b.Tags) {
			t.Errorf("Apply Tags = %v, want %v", got.Tags, b.Tags)
		}
	})

	// Option A: empty-but-non-nil is not nil, so {k:v}→{} uses inner diff (OpAssert), not OpRetract.
	t.Run("Tags_nonNilToEmpty", func(t *testing.T) {
		a := mkSnap(1, baseLocation, map[string]string{"k": "v"}, baseGroups)
		b := mkSnap(2, baseLocation, map[string]string{}, baseGroups)
		d, err := clearable_composite.Diff(a, b)
		if err != nil {
			t.Fatalf("Diff: %v", err)
		}
		if d.Tags.Op != eddt.OpAssert {
			t.Errorf("Tags.Op = %v, want OpAssert", d.Tags.Op)
		}
		if d.Tags.Value.IsEmpty() {
			t.Errorf("Tags.Value.IsEmpty() = true, want non-empty inner delta (k removed)")
		}
		got, err := clearable_composite.Apply(a, d)
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if !reflect.DeepEqual(got.Tags, b.Tags) {
			t.Errorf("Apply Tags = %v, want %v (empty non-nil)", got.Tags, b.Tags)
		}
	})

	// Option A: nil→{} produces an empty inner diff → OpIgnore (nil/empty no-op).
	t.Run("Tags_nilToEmpty", func(t *testing.T) {
		a := mkSnap(1, baseLocation, nil, baseGroups)
		b := mkSnap(2, baseLocation, map[string]string{}, baseGroups)
		d, err := clearable_composite.Diff(a, b)
		if err != nil {
			t.Fatalf("Diff: %v", err)
		}
		if d.Tags.Op != eddt.OpIgnore {
			t.Errorf("Tags.Op = %v, want OpIgnore (empty inner diff; nil/empty are equivalent)", d.Tags.Op)
		}
		got, err := clearable_composite.Apply(a, d)
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if got.Tags != nil {
			t.Errorf("Apply Tags = %v, want nil (OpIgnore preserved nil)", got.Tags)
		}
	})

	// ── Groups (slice clearable) ─────────────────────────────────────────────

	t.Run("Groups_equal", func(t *testing.T) {
		a := mkSnap(1, baseLocation, baseTags, []string{"x", "y"})
		b := mkSnap(2, baseLocation, baseTags, []string{"x", "y"})
		d, err := clearable_composite.Diff(a, b)
		if err != nil {
			t.Fatalf("Diff: %v", err)
		}
		if d.Groups.Op != eddt.OpIgnore {
			t.Errorf("Groups.Op = %v, want OpIgnore", d.Groups.Op)
		}
		got, err := clearable_composite.Apply(a, d)
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if !reflect.DeepEqual(got.Groups, a.Groups) {
			t.Errorf("Apply Groups = %v, want %v", got.Groups, a.Groups)
		}
	})

	t.Run("Groups_bothNil", func(t *testing.T) {
		a := mkSnap(1, baseLocation, baseTags, nil)
		b := mkSnap(2, baseLocation, baseTags, nil)
		d, err := clearable_composite.Diff(a, b)
		if err != nil {
			t.Fatalf("Diff: %v", err)
		}
		if d.Groups.Op != eddt.OpIgnore {
			t.Errorf("Groups.Op = %v, want OpIgnore", d.Groups.Op)
		}
	})

	t.Run("Groups_nilToNonNil", func(t *testing.T) {
		a := mkSnap(1, baseLocation, baseTags, nil)
		b := mkSnap(2, baseLocation, baseTags, []string{"new"})
		d, err := clearable_composite.Diff(a, b)
		if err != nil {
			t.Fatalf("Diff: %v", err)
		}
		if d.Groups.Op != eddt.OpAssert {
			t.Errorf("Groups.Op = %v, want OpAssert", d.Groups.Op)
		}
		got, err := clearable_composite.Apply(a, d)
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if !reflect.DeepEqual(got.Groups, b.Groups) {
			t.Errorf("Apply Groups = %v, want %v", got.Groups, b.Groups)
		}
	})

	t.Run("Groups_nonNilToNil", func(t *testing.T) {
		a := mkSnap(1, baseLocation, baseTags, []string{"x"})
		b := mkSnap(2, baseLocation, baseTags, nil)
		d, err := clearable_composite.Diff(a, b)
		if err != nil {
			t.Fatalf("Diff: %v", err)
		}
		if d.Groups.Op != eddt.OpRetract {
			t.Errorf("Groups.Op = %v, want OpRetract", d.Groups.Op)
		}
		got, err := clearable_composite.Apply(a, d)
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if got.Groups != nil {
			t.Errorf("Apply Groups = %v, want nil", got.Groups)
		}
	})

	t.Run("Groups_different", func(t *testing.T) {
		a := mkSnap(1, baseLocation, baseTags, []string{"x"})
		b := mkSnap(2, baseLocation, baseTags, []string{"y"})
		d, err := clearable_composite.Diff(a, b)
		if err != nil {
			t.Fatalf("Diff: %v", err)
		}
		if d.Groups.Op != eddt.OpAssert {
			t.Errorf("Groups.Op = %v, want OpAssert", d.Groups.Op)
		}
		if d.Groups.Value.IsEmpty() {
			t.Errorf("Groups.Value.IsEmpty() = true, want non-empty inner delta")
		}
		got, err := clearable_composite.Apply(a, d)
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		hasY, hasX := false, false
		for _, g := range got.Groups {
			if g == "y" {
				hasY = true
			}
			if g == "x" {
				hasX = true
			}
		}
		if !hasY || hasX {
			t.Errorf("Apply Groups = %v, want {y} (set-membership)", got.Groups)
		}
	})

	// Option A: [x]→[] uses inner diff (removes x → OpAssert), not OpRetract.
	t.Run("Groups_nonNilToEmpty", func(t *testing.T) {
		a := mkSnap(1, baseLocation, baseTags, []string{"x"})
		b := mkSnap(2, baseLocation, baseTags, []string{})
		d, err := clearable_composite.Diff(a, b)
		if err != nil {
			t.Fatalf("Diff: %v", err)
		}
		if d.Groups.Op != eddt.OpAssert {
			t.Errorf("Groups.Op = %v, want OpAssert", d.Groups.Op)
		}
		if d.Groups.Value.IsEmpty() {
			t.Errorf("Groups.Value.IsEmpty() = true, want non-empty inner delta (x removed)")
		}
		got, err := clearable_composite.Apply(a, d)
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		for _, g := range got.Groups {
			if g == "x" {
				t.Errorf("Apply Groups = %v, want empty (x removed)", got.Groups)
				break
			}
		}
	})

	// Option A: nil→[] produces an empty inner diff → OpIgnore (nil/empty no-op).
	t.Run("Groups_nilToEmpty", func(t *testing.T) {
		a := mkSnap(1, baseLocation, baseTags, nil)
		b := mkSnap(2, baseLocation, baseTags, []string{})
		d, err := clearable_composite.Diff(a, b)
		if err != nil {
			t.Fatalf("Diff: %v", err)
		}
		if d.Groups.Op != eddt.OpIgnore {
			t.Errorf("Groups.Op = %v, want OpIgnore (empty inner diff; nil/empty are equivalent)", d.Groups.Op)
		}
		got, err := clearable_composite.Apply(a, d)
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if got.Groups != nil {
			t.Errorf("Apply Groups = %v, want nil (OpIgnore preserved nil)", got.Groups)
		}
	})
}
`

// ── HK-17: deterministic nil-vs-empty coverage ───────────────────────────────

// TestConformance_NilEqualsEmpty is the HK-17 deterministic E-17 nil ≙ empty
// test for nested (non-clearable) map and slice fields.
//
// testing/quick never generates nil maps or slices, so the nil ≙ empty
// invariant for nested shapes is not exercised by the property-test suite.
// This test covers it explicitly.  The clearable variants (Tags, Groups) are
// already covered by TestConformance_TruthTable (Tags_nilToEmpty,
// Groups_nilToEmpty rows).
//
// Test matrix:
//
//	TestConformance_NilEqualsEmpty/CompositeSnapshot/TestNilEqualsEmpty_Property/...
func TestConformance_NilEqualsEmpty(t *testing.T) {
	axis := conformanceAxis{
		testFile:   "nilempty_test.go",
		runPattern: "TestNilEqualsEmpty_Property",
		srcs:       map[string]string{"composite": compositeNilEmptyTest},
	}
	for _, tc := range corpus {
		if _, ok := axis.srcs[tc.dir]; !ok {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			runCorpusProperty(t, tc, generateCorpusDelta(t, tc), axis)
		})
	}
}

// compositeNilEmptyTest is the injected deterministic nil ≙ empty test for the
// composite corpus case.  It exercises all four nil/empty/populated transitions
// for the nested map (Labels, N-03) and nested slice (Groups, N-04) fields.
//
// testing/quick never generates nil maps or slices, so these cases require
// explicit deterministic coverage (HK-17).
const compositeNilEmptyTest = `package composite_test

import (
	"testing"
	"time"

	"composite"
	eddt "go.resystems.io/eddt/runtime"
)

func hdrNE(seq uint64) eddt.Header {
	return eddt.Header{EntityID: eddt.EntityID{1}, ChainID: "c", Sequence: seq, EffectiveAt: time.Now()}
}

// TestNilEqualsEmpty_Property exercises E-17 nil ≙ empty for the nested
// map (Labels) and nested slice (Groups) fields.
func TestNilEqualsEmpty_Property(t *testing.T) {
	id := eddt.EntityID{1}
	now := time.Now()

	type tc struct {
		name          string
		aLabels       map[string]string
		bLabels       map[string]string
		aGroups       []string
		bGroups       []string
		wantNilLabels bool
		wantNilGroups bool
	}
	tests := []tc{
		// §E-17: nil ≙ empty — no delta produced for nil→nil, nil→empty, empty→nil.
		{"nil_to_nil",   nil, nil, nil, nil, true, true},
		{"nil_to_empty", nil, map[string]string{}, nil, []string{}, true, true},
		{"empty_to_nil", map[string]string{}, nil, []string{}, nil, true, true},
		// Non-trivial: nil→populated must produce a delta.
		{"nil_to_populated", nil, map[string]string{"k": "v"}, nil, []string{"x"}, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := composite.CompositeSnapshot{
				Header:  eddt.Header{EntityID: id, ChainID: "c", Sequence: 1, EffectiveAt: now},
				Key:     "k",
				Labels:  tt.aLabels,
				Groups:  tt.aGroups,
			}
			b := composite.CompositeSnapshot{
				Header:  eddt.Header{EntityID: id, ChainID: "c", Sequence: 2, EffectiveAt: now},
				Key:     "k",
				Labels:  tt.bLabels,
				Groups:  tt.bGroups,
			}
			d, err := composite.Diff(a, b)
			if err != nil {
				t.Fatalf("Diff: %v", err)
			}
			gotNilLabels := d.UpdatedLabels == nil && d.RemovedLabels == nil
			gotNilGroups := d.AddedGroups == nil && d.RemovedGroups == nil
			if gotNilLabels != tt.wantNilLabels {
				t.Errorf("Labels delta nil=%v, want %v (Updated=%v, Removed=%v)",
					gotNilLabels, tt.wantNilLabels, d.UpdatedLabels, d.RemovedLabels)
			}
			if gotNilGroups != tt.wantNilGroups {
				t.Errorf("Groups delta nil=%v, want %v (Added=%v, Removed=%v)",
					gotNilGroups, tt.wantNilGroups, d.AddedGroups, d.RemovedGroups)
			}
		})
	}
}
`
