package deltagen

// integration_test.go implements C-05: per-field emission and tag-validation
// integration tests.
//
// C-05 covers two gaps left by C-01..C-04:
//
//  1. Cross-package compilation: C-01 only runs go build in same-package mode.
//     TestEmitTemplate_*_CrossPackage tests are AST-only (no subprocess compile).
//     C-05 adds the first end-to-end cross-package go build for all corpus cases.
//
//  2. Tag-validation errors at the pipeline level: T-02/T-03 are tested as
//     parse-unit tests (tag_test.go, parse_test.go).  C-05 verifies that each
//     validation error reaches the caller through Run().
//
// Key-field discovery semantics (informing all T-03 tests):
//
//   - Tag-based path (Config.KeyFields[struct] == ""): walkFields scans direct
//     fields for eddt:"entity.key"; zero matches → error; two or more → error.
//   - Override path (Config.KeyFields[struct] != ""): selects field by name;
//     entity.key tags on other fields are silently ignored (warning only).
//   - walkFields direct-fields-only invariant: st.NumFields() iterates direct
//     fields; promoted fields from embedded struct types are NOT visited.  An
//     entity.key tag inside an embedded struct type (not on the embedding field
//     itself) is invisible to the generator.
//
// Test matrix (C-05):
//
//	TestIntegration_PerFieldEmission/EM-01..04/AtomicShapes
//	TestIntegration_PerFieldEmission/EM-01..04/CompositeShapes
//	TestIntegration_PerFieldEmission/EM-05/StructKey
//	TestIntegration_CrossPkgEmissionCompiles/BaselineSnapshot
//	TestIntegration_CrossPkgEmissionCompiles/CompositeSnapshot
//	TestIntegration_CrossPkgEmissionCompiles/SessionSnapshot
//	TestIntegration_TagValidationErrors/T02/NestedOnScalar
//	TestIntegration_TagValidationErrors/T02/NestedOnPointer
//	TestIntegration_TagValidationErrors/T03/ClearableDeferred
//	TestIntegration_TagValidationErrors/T03/UnknownTag
//	TestIntegration_TagValidationErrors/T03/NoEntityKeyTag
//	TestIntegration_TagValidationErrors/T03/MultipleEntityKeyTags
//	TestIntegration_TagValidationErrors/T03/EmbeddedStructKeyTag
//	TestIntegration_TagValidationErrors/T03/KeyOverrideFieldNotFound
//	TestIntegration_TagValidationErrors/T03/SliceEntityKey
//	TestIntegration_TagValidationErrors/T03/MapEntityKey
//	TestIntegration_KeyFieldOverride/T03/OverrideWithNoTag

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ── C-05.1: per-field emission organised by EM scope ─────────────────────────
//
// TestIntegration_PerFieldEmission provides the EM-item view of the corpus
// compile checks.  C-01 (TestCorpus_All) organises by corpus case; this test
// organises by emission rule, annotating which EM items each subtest covers.
//
// Each subtest generates the corpus fixture in same-package mode and runs
// compileCheckCorpus (defined in corpus_test.go) to confirm go build succeeds.
func TestIntegration_PerFieldEmission(t *testing.T) {
	cases := []struct {
		label string // EM scope label
		dir   string // testdata/corpus subdirectory
		name  string // Snapshot struct name
		pkg   string // isolated module name for compileCheckCorpus
	}{
		// EM-01..04: all 5 atomic shapes + presence tags (omit/retired/commutative)
		// + E-15 untagged slice + E-16 untagged map + Apply/Diff/Coalesce (E-19..E-21).
		{"EM-01..04/AtomicShapes", "baseline", "BaselineSnapshot", "baseline"},
		// EM-01..04: delta.nested on struct value (N-01) + map (N-03) + slice (N-04)
		// + atomic coexistence; Apply/Diff/Coalesce return (T, error) (E-19..E-21).
		{"EM-01..04/CompositeShapes", "composite", "CompositeSnapshot", "composite"},
		// EM-05: struct-valued entity.key → multi-field EntityID hash in lexicographic
		// field-name order (E-10); method wrapper on named key type.
		{"EM-05/StructKey", "struct_key", "SessionSnapshot", "struct_key"},
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			outPath := filepath.Join(t.TempDir(), "delta.go")
			if err := New(Config{
				InputPkgs:     []string{"./testdata/corpus/" + tc.dir},
				TargetStructs: []string{tc.name},
				OutPath:       outPath,
			}).Run(); err != nil {
				t.Fatalf("Run(): %v", err)
			}
			src, err := os.ReadFile(outPath)
			if err != nil {
				t.Fatalf("reading output: %v", err)
			}
			compileCheckCorpus(t, tc.dir, tc.pkg, src)
		})
	}
}

// ── C-05.2: cross-package emission compiles ───────────────────────────────────
//
// TestIntegration_CrossPkgEmissionCompiles is the first end-to-end cross-package
// compilation test for all three corpus cases.  It sets up a two-package temp
// module (corpus fixture in snap/, generated delta.go at module root), runs the
// generator with OutPkgNameOverride, then runs go build ./... (E-12).
func TestIntegration_CrossPkgEmissionCompiles(t *testing.T) {
	for _, tc := range corpus {
		t.Run(tc.name, func(t *testing.T) {
			crossPkgCompileCheck(t, tc.dir, tc.name, tc.pkg+"_xpkg")
		})
	}
}

// crossPkgCompileCheck sets up an isolated two-package temp module, runs the
// generator in cross-package mode (OutPkgNameOverride = modName), and runs
// go build -mod=mod ./... to confirm the generated output compiles (E-12).
//
// Layout of the temp module:
//
//	tmpDir/go.mod          — module modName; replace go.resystems.io/eddt → moduleRoot
//	tmpDir/go.sum          — copied from moduleRoot
//	tmpDir/snap/snapshot.go — corpus fixture (package declaration preserved)
//	tmpDir/delta.go        — generator output (package modName, imports modName/snap)
//
// go.mod is written BEFORE calling the generator so that go/packages resolves
// tmpDir/snap/ to the import path modName/snap (load.go sets Config.Dir = snapDir
// and calls packages.Load(cfg, "."); the go tool walks up to find go.mod).
//
// GOFLAGS=-mod=mod is set for the duration of this helper so that go/packages
// accepts the minimal go.mod (direct dep + replace only) without requiring all
// transitive deps to be listed (Go 1.17+ graph-pruning requirement).
func crossPkgCompileCheck(t *testing.T, dir, snapshotName, modName string) {
	t.Helper()
	t.Setenv("GOFLAGS", "-mod=mod")

	tmpDir := t.TempDir()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	moduleRoot := filepath.Clean(filepath.Join(wd, "..", ".."))

	// Write go.mod before loading so go/packages resolves snap/ as modName/snap.
	modContent := "module " + modName + "\n\ngo 1.25.0\n\nrequire go.resystems.io/eddt v0.0.0\n\nreplace go.resystems.io/eddt => " + moduleRoot + "\n"
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte(modContent), 0644); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}
	goSum, err := os.ReadFile(filepath.Join(moduleRoot, "go.sum"))
	if err != nil {
		t.Fatalf("read go.sum: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "go.sum"), goSum, 0644); err != nil {
		t.Fatalf("write go.sum: %v", err)
	}

	// Copy corpus fixture → snap/snapshot.go (package declaration preserved).
	snapDir := filepath.Join(tmpDir, "snap")
	if err := os.MkdirAll(snapDir, 0755); err != nil {
		t.Fatalf("mkdir snap: %v", err)
	}
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
			if err := os.WriteFile(filepath.Join(snapDir, "snapshot.go"), fixtureSrc, 0644); err != nil {
				t.Fatalf("write snap/snapshot.go: %v", err)
			}
			wroteFixture = true
			break
		}
	}
	if !wroteFixture {
		t.Fatalf("no .go file found in %s", fixtureDir)
	}

	// Run the generator in cross-package mode.
	deltaPath := filepath.Join(tmpDir, "delta.go")
	if err := New(Config{
		InputPkgs:          []string{snapDir},
		TargetStructs:      []string{snapshotName},
		OutPath:            deltaPath,
		OutPkgNameOverride: modName,
	}).Run(); err != nil {
		t.Fatalf("Run() cross-package: %v", err)
	}
	assertGofmtClean(t, deltaPath)
	runBuildCmd(t, tmpDir, "go", "build", "-mod=mod", "./...")
}

// ── C-05.3: tag-validation errors surface through Run() ──────────────────────
//
// TestIntegration_TagValidationErrors verifies that each invalid input
// configuration causes New(cfg).Run() to return a non-nil error.  No go build
// step is performed — the generator rejects the input before emitting output.
//
// Inline sources are written to a temp dir (not added to testdata/) because these
// are intentionally invalid inputs.
func TestIntegration_TagValidationErrors(t *testing.T) {
	for _, tc := range tagErrorCases {
		t.Run(tc.label, func(t *testing.T) {
			tagErrorCheck(t, tc.src, tc.structName, tc.keyFields, tc.wantErr)
		})
	}
}

// tagErrorCase describes one invalid-input row in TestIntegration_TagValidationErrors.
type tagErrorCase struct {
	label      string            // subtest label, e.g. "T02/NestedOnScalar"
	src        string            // complete Go source written to a temp snap.go
	structName string            // target struct name passed to the generator
	keyFields  map[string]string // Config.KeyFields override; nil → tag-based discovery
	wantErr    string            // expected substring in error; "" → any error suffices
}

var tagErrorCases = []tagErrorCase{
	// T-02 / E-14: delta.nested rejected on scalar field.
	{
		label:      "T02/NestedOnScalar",
		src:        "package snap\n\nimport eddt \"go.resystems.io/eddt/runtime\"\n\ntype NestedScalar struct {\n\teddt.Header\n\tKey  string `eddt:\"entity.key\"`\n\tName string `eddt:\"delta.nested\"`\n}\n",
		structName: "NestedScalar",
		wantErr:    "composite",
	},
	// T-02 / E-14: delta.nested rejected on pointer field.
	{
		label:      "T02/NestedOnPointer",
		src:        "package snap\n\nimport eddt \"go.resystems.io/eddt/runtime\"\n\ntype NestedPointer struct {\n\teddt.Header\n\tKey      string `eddt:\"entity.key\"`\n\tPriority *int32 `eddt:\"delta.nested\"`\n}\n",
		structName: "NestedPointer",
		wantErr:    "composite",
	},
	// CL-04 / E-23: standalone delta.clearable is rejected (Clearable ⟹ Nested).
	{
		label:      "T04/StandaloneClearableScalar",
		src:        "package snap\n\nimport eddt \"go.resystems.io/eddt/runtime\"\n\ntype StandaloneClearableScalar struct {\n\teddt.Header\n\tKey  string `eddt:\"entity.key\"`\n\tName string `eddt:\"delta.clearable\"`\n}\n",
		structName: "StandaloneClearableScalar",
		wantErr:    "delta.nested",
	},
	{
		label:      "T04/StandaloneClearablePointer",
		src:        "package snap\n\nimport eddt \"go.resystems.io/eddt/runtime\"\n\ntype StandaloneClearablePointer struct {\n\teddt.Header\n\tKey      string `eddt:\"entity.key\"`\n\tPriority *int32 `eddt:\"delta.clearable\"`\n}\n",
		structName: "StandaloneClearablePointer",
		wantErr:    "delta.nested",
	},
	{
		label:      "T04/ClearablePlusOmit",
		src:        "package snap\n\nimport eddt \"go.resystems.io/eddt/runtime\"\n\ntype ClearablePlusOmit struct {\n\teddt.Header\n\tKey  string `eddt:\"entity.key\"`\n\tName string `eddt:\"delta.omit,delta.clearable\"`\n}\n",
		structName: "ClearablePlusOmit",
		wantErr:    "delta.nested",
	},
	{
		// delta.nested,delta.clearable on a scalar: validateTagShape rejects
		// first ("composite"), before the combination predicate is reached.
		label:      "T04/ClearableOnScalarWithNested",
		src:        "package snap\n\nimport eddt \"go.resystems.io/eddt/runtime\"\n\ntype ClearableOnScalarWithNested struct {\n\teddt.Header\n\tKey  string `eddt:\"entity.key\"`\n\tName string `eddt:\"delta.nested,delta.clearable\"`\n}\n",
		structName: "ClearableOnScalarWithNested",
		wantErr:    "composite",
	},

	// T-03: unknown tag value is rejected by the tag parser.
	// Exact error message is an implementation detail; any non-nil error is sufficient.
	{
		label:      "T03/UnknownTag",
		src:        "package snap\n\nimport eddt \"go.resystems.io/eddt/runtime\"\n\ntype UnknownTagSnap struct {\n\teddt.Header\n\tKey  string `eddt:\"entity.key\"`\n\tName string `eddt:\"delta.bogus\"`\n}\n",
		structName: "UnknownTagSnap",
		wantErr:    "",
	},
	// T-03 / G-04: tag-based path with no entity.key tag on any direct field.
	// Qualifies: error only when Config.KeyFields[struct] == "" (tag-based path).
	{
		label:      "T03/NoEntityKeyTag",
		src:        "package snap\n\nimport eddt \"go.resystems.io/eddt/runtime\"\n\ntype NoKeyTagSnap struct {\n\teddt.Header\n\tName string\n}\n",
		structName: "NoKeyTagSnap",
		wantErr:    "no field tagged",
	},
	// T-03 / G-04: two DIRECT fields on the outer struct are both tagged entity.key.
	{
		label:      "T03/MultipleEntityKeyTags",
		src:        "package snap\n\nimport eddt \"go.resystems.io/eddt/runtime\"\n\ntype MultiKeySnap struct {\n\teddt.Header\n\tKey1 string `eddt:\"entity.key\"`\n\tKey2 string `eddt:\"entity.key\"`\n}\n",
		structName: "MultiKeySnap",
		wantErr:    "multiple fields tagged",
	},
	// T-03: entity.key tag placed on a field INSIDE an embedded struct type.
	// walkFields visits only direct fields (st.NumFields()); the promoted InnerKey.ID
	// field is invisible → "no field tagged entity.key" (direct-fields-only invariant).
	{
		label:      "T03/EmbeddedStructKeyTag",
		src:        "package snap\n\nimport eddt \"go.resystems.io/eddt/runtime\"\n\ntype InnerKey struct {\n\tID string `eddt:\"entity.key\"`\n}\n\ntype EmbeddedKeySnap struct {\n\teddt.Header\n\tInnerKey\n\tName string\n}\n",
		structName: "EmbeddedKeySnap",
		wantErr:    "no field tagged",
	},
	// T-03 / G-06: Config.KeyFields override names a field not present in the struct.
	{
		label:      "T03/KeyOverrideFieldNotFound",
		src:        "package snap\n\nimport eddt \"go.resystems.io/eddt/runtime\"\n\ntype OverrideSnap struct {\n\teddt.Header\n\tKey  string `eddt:\"entity.key\"`\n\tName string\n}\n",
		structName: "OverrideSnap",
		keyFields:  map[string]string{"OverrideSnap": "NonExistentField"},
		wantErr:    "override names field",
	},
	// T-03 / G-04 / E-10: slice-typed entity-key field; slices are not comparable.
	{
		label:      "T03/SliceEntityKey",
		src:        "package snap\n\nimport eddt \"go.resystems.io/eddt/runtime\"\n\ntype SliceKeySnap struct {\n\teddt.Header\n\tKey  []string `eddt:\"entity.key\"`\n\tName string\n}\n",
		structName: "SliceKeySnap",
		wantErr:    "not comparable",
	},
	// T-03 / G-04 / E-10: map-typed entity-key field; maps are not comparable.
	{
		label:      "T03/MapEntityKey",
		src:        "package snap\n\nimport eddt \"go.resystems.io/eddt/runtime\"\n\ntype MapKeySnap struct {\n\teddt.Header\n\tKey  map[string]string `eddt:\"entity.key\"`\n\tName string\n}\n",
		structName: "MapKeySnap",
		wantErr:    "not comparable",
	},
}

// tagErrorCheck writes inline Go source to a temp module, runs New(cfg).Run(),
// asserts a non-nil error is returned, and (when wantErr is non-empty) asserts
// the error message contains wantErr.
//
// No go build step is performed — the test verifies only that the generator
// rejects the input before emitting output.
//
// GOFLAGS=-mod=mod is set so that go/packages accepts the minimal go.mod.
func tagErrorCheck(t *testing.T, src, structName string, keyFields map[string]string, wantErr string) {
	t.Helper()
	t.Setenv("GOFLAGS", "-mod=mod")

	tmpDir := t.TempDir()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	moduleRoot := filepath.Clean(filepath.Join(wd, "..", ".."))

	modContent := "module snap\n\ngo 1.25.0\n\nrequire go.resystems.io/eddt v0.0.0\n\nreplace go.resystems.io/eddt => " + moduleRoot + "\n"
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte(modContent), 0644); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}
	goSum, err := os.ReadFile(filepath.Join(moduleRoot, "go.sum"))
	if err != nil {
		t.Fatalf("read go.sum: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "go.sum"), goSum, 0644); err != nil {
		t.Fatalf("write go.sum: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "snap.go"), []byte(src), 0644); err != nil {
		t.Fatalf("write snap.go: %v", err)
	}

	runErr := New(Config{
		InputPkgs:     []string{tmpDir},
		TargetStructs: []string{structName},
		KeyFields:     keyFields,
		OutPath:       filepath.Join(tmpDir, "delta.go"),
	}).Run()
	if runErr == nil {
		t.Fatalf("Run() expected error, got nil")
	}
	if wantErr != "" && !strings.Contains(runErr.Error(), wantErr) {
		t.Fatalf("Run() error %q does not contain %q", runErr.Error(), wantErr)
	}
}

// ── C-05.4: key-field override positive path ──────────────────────────────────
//
// TestIntegration_KeyFieldOverride verifies that Config.KeyFields selects the
// entity-key field by name even when no eddt:"entity.key" tag is present,
// and that the resulting generated code compiles (T-03 / G-06 override path).
func TestIntegration_KeyFieldOverride(t *testing.T) {
	t.Run("T03/OverrideWithNoTag", func(t *testing.T) {
		// Snapshot with Key string but no entity.key tag; override supplies the key.
		const src = "package snap\n\nimport eddt \"go.resystems.io/eddt/runtime\"\n\ntype OverrideSnap struct {\n\teddt.Header\n\tKey  string\n\tName string\n}\n"

		t.Setenv("GOFLAGS", "-mod=mod")
		tmpDir := t.TempDir()
		wd, err := os.Getwd()
		if err != nil {
			t.Fatalf("getwd: %v", err)
		}
		moduleRoot := filepath.Clean(filepath.Join(wd, "..", ".."))

		modContent := "module overridesnap\n\ngo 1.25.0\n\nrequire go.resystems.io/eddt v0.0.0\n\nreplace go.resystems.io/eddt => " + moduleRoot + "\n"
		if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte(modContent), 0644); err != nil {
			t.Fatalf("write go.mod: %v", err)
		}
		goSum, err := os.ReadFile(filepath.Join(moduleRoot, "go.sum"))
		if err != nil {
			t.Fatalf("read go.sum: %v", err)
		}
		if err := os.WriteFile(filepath.Join(tmpDir, "go.sum"), goSum, 0644); err != nil {
			t.Fatalf("write go.sum: %v", err)
		}
		if err := os.WriteFile(filepath.Join(tmpDir, "snap.go"), []byte(src), 0644); err != nil {
			t.Fatalf("write snap.go: %v", err)
		}

		deltaPath := filepath.Join(tmpDir, "delta.go")
		if err := New(Config{
			InputPkgs:     []string{tmpDir},
			TargetStructs: []string{"OverrideSnap"},
			KeyFields:     map[string]string{"OverrideSnap": "Key"},
			OutPath:       deltaPath,
		}).Run(); err != nil {
			t.Fatalf("Run(): %v", err)
		}
		assertGofmtClean(t, deltaPath)
		runBuildCmd(t, tmpDir, "go", "build", "-mod=mod", "./...")
	})
}
