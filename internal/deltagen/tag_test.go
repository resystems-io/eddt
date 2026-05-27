package deltagen

// tag_test.go exercises the eddt: tag parser (T-01) and the harmonised
// tag-validation gate (T-02).

import (
	"strings"
	"testing"
)

// TestParseTag covers all Group T parseTag cases.
// Covers: R-15 (partial), E-07
func TestParseTag(t *testing.T) {
	cases := []struct {
		name          string
		input         string
		wantKind      TagKind
		wantClearable bool              // assert Clearable == this
		wantNilOpts   bool              // assert Options == nil
		wantOpts      map[string]string // assert each entry present with exact value
		wantOptsLen   int               // >0: assert len(Options) == this
		wantRaw       string            // non-empty: assert Raw == this value
		wantErr       bool
		wantErrHas    []string // substrings that must appear in error
	}{
		{name: "T01_Empty", input: "", wantKind: TagKindNone, wantNilOpts: true},
		{name: "T02_EntityKey", input: "entity.key", wantKind: TagKindEntityKey, wantNilOpts: true},
		{name: "T03_Nested", input: "delta.nested", wantKind: TagKindNested, wantNilOpts: true},
		{name: "T04_Omit", input: "delta.omit", wantKind: TagKindOmit},
		{name: "T05_Retired", input: "delta.retired", wantKind: TagKindRetired},
		{name: "T06_Commutative", input: "delta.commutative", wantKind: TagKindCommutative},
		{
			name:     "T07_KnownOption",
			input:    "delta.retired,since=2026-01-15",
			wantKind: TagKindRetired,
			wantOpts: map[string]string{"since": "2026-01-15"},
		},
		{
			// Unknown option keys are preserved without acting on them (E-07).
			name:     "T08_UnknownOption",
			input:    "delta.nested,extra=foo",
			wantKind: TagKindNested,
			wantOpts: map[string]string{"extra": "foo"},
		},
		{
			name:        "T09_MultipleOptions",
			input:       "delta.retired,since=2026-01-15,reason=drop",
			wantKind:    TagKindRetired,
			wantOpts:    map[string]string{"since": "2026-01-15", "reason": "drop"},
			wantOptsLen: 2,
		},
		{
			name:     "T10_CommutativeOption",
			input:    "delta.commutative,mode=lww",
			wantKind: TagKindCommutative,
			wantOpts: map[string]string{"mode": "lww"},
		},
		{
			// entity.key with an unknown option: option is preserved (E-07).
			name:     "T11_EntityKeyOption",
			input:    "entity.key,scope=global",
			wantKind: TagKindEntityKey,
			wantOpts: map[string]string{"scope": "global"},
		},
		{
			// Empty option value ("k=") is accepted; key maps to "".
			name:     "T12_EmptyValue",
			input:    "delta.retired,k=",
			wantKind: TagKindRetired,
			wantOpts: map[string]string{"k": ""},
		},
		{
			// A bare unrecognised token is a tag lookup, not a malformed option.
			// Error now comes from tagKindFor (unrecognised tag) not the option parser.
			name:       "T13_BareUnrecognised",
			input:      "delta.nested,novalue",
			wantErr:    true,
			wantErrHas: []string{"novalue", "unrecognised"},
		},
		{
			// delta.clearable alone → secondary tag recognised, Kind stays None.
			// Semantic validation (Clearable ⟹ Nested) is enforced in CL-04.
			name:          "T14_ClearableAlone",
			input:         "delta.clearable",
			wantKind:      TagKindNone,
			wantClearable: true,
			wantNilOpts:   true,
		},
		{
			name:       "T15_Unknown",
			input:      "delta.bogus",
			wantErr:    true,
			wantErrHas: []string{"delta.bogus"},
		},
		{
			// Raw preserves the verbatim input string.
			name:     "T16_RawPreserved",
			input:    "delta.retired,since=2026-01-15",
			wantKind: TagKindRetired,
			wantOpts: map[string]string{"since": "2026-01-15"},
			wantRaw:  "delta.retired,since=2026-01-15",
		},
		// CL-03 cases
		{
			// Primary + secondary: standard combined clearable tag.
			name:          "T17_NestedClearable",
			input:         "delta.nested,delta.clearable",
			wantKind:      TagKindNested,
			wantClearable: true,
			wantNilOpts:   true,
		},
		{
			// Order must not matter: secondary before primary parses identically.
			name:          "T18_ClearableNestedReverse",
			input:         "delta.clearable,delta.nested",
			wantKind:      TagKindNested,
			wantClearable: true,
			wantNilOpts:   true,
		},
		{
			// Primary + secondary + option: all three parts coexist.
			name:          "T19_NestedClearableOption",
			input:         "delta.nested,delta.clearable,extra=foo",
			wantKind:      TagKindNested,
			wantClearable: true,
			wantOpts:      map[string]string{"extra": "foo"},
		},
		{
			// Regression: primary + option without secondary still works (Clearable false).
			name:          "T20_RetiredOptionNoClearable",
			input:         "delta.retired,since=2026-01-15",
			wantKind:      TagKindRetired,
			wantClearable: false,
			wantOpts:      map[string]string{"since": "2026-01-15"},
		},
		{
			// Two primaries in one tag value is an error.
			name:       "T21_TwoPrimaries",
			input:      "delta.nested,delta.omit",
			wantErr:    true,
			wantErrHas: []string{"multiple primary"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pt, err := parseTag(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				for _, want := range tc.wantErrHas {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("error should contain %q, got: %v", want, err)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if pt.Kind != tc.wantKind {
				t.Errorf("Kind: got %v, want %v", pt.Kind, tc.wantKind)
			}
			if pt.Clearable != tc.wantClearable {
				t.Errorf("Clearable: got %v, want %v", pt.Clearable, tc.wantClearable)
			}
			if tc.wantNilOpts && pt.Options != nil {
				t.Errorf("Options: got %v, want nil", pt.Options)
			}
			for k, want := range tc.wantOpts {
				got, ok := pt.Options[k]
				if !ok {
					t.Errorf("Options[%q]: key absent, want %q", k, want)
				} else if got != want {
					t.Errorf("Options[%q]: got %q, want %q", k, got, want)
				}
			}
			if tc.wantOptsLen > 0 && len(pt.Options) != tc.wantOptsLen {
				t.Errorf("len(Options): got %d, want %d", len(pt.Options), tc.wantOptsLen)
			}
			if tc.wantRaw != "" && pt.Raw != tc.wantRaw {
				t.Errorf("Raw: got %q, want %q", pt.Raw, tc.wantRaw)
			}
		})
	}
}

// TestValidateTagShape exercises the harmonised granularity-axis gate.
// Covers: R-17 (E-14)
func TestValidateTagShape(t *testing.T) {
	cases := []struct {
		name    string
		kind    TagKind
		shape   FieldShape
		wantErr bool
	}{
		{"T2.01_NestedOnStruct", TagKindNested, ShapeStructValue, false},
		{"T2.02_NestedOnSlice", TagKindNested, ShapeSlice, false},
		{"T2.03_NestedOnMap", TagKindNested, ShapeMap, false},
		{"T2.04_NestedOnScalar", TagKindNested, ShapeScalar, true},
		{"T2.05_NestedOnPointer", TagKindNested, ShapePointer, true},
		{"T2.06_OmitOnScalar", TagKindOmit, ShapeScalar, false},
		{"T2.07_OmitOnMap", TagKindOmit, ShapeMap, false},
		{"T2.08_RetiredOnSlice", TagKindRetired, ShapeSlice, false},
		{"T2.09_CommutativeOnPointer", TagKindCommutative, ShapePointer, false},
		{"T2.10_EntityKeyOnStruct", TagKindEntityKey, ShapeStructValue, false},
		{"T2.11_NoneOnScalar", TagKindNone, ShapeScalar, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateTagShape(ParsedTag{Kind: tc.kind}, tc.shape)
			if (err != nil) != tc.wantErr {
				t.Errorf("validateTagShape(%v, %v): err = %v, wantErr = %v",
					tc.kind, tc.shape, err, tc.wantErr)
			}
			if err != nil && !strings.Contains(err.Error(), "composite") {
				t.Errorf("error should mention 'composite', got: %v", err)
			}
		})
	}
}

// TestValidateTagCombination smoke-tests the harmonised combination stub.
// Covers: R-16 (E-14)
func TestValidateTagCombination(t *testing.T) {
	// In the baseline, every TagKind is a no-op: parseTag yields one
	// kind per field, and the single forbidden combination
	// (delta.omit + delta.clearable) is not expressible via single-tag
	// Go struct syntax. CL-04 (Phase 7) extends this function.
	for _, k := range []TagKind{
		TagKindNone, TagKindEntityKey, TagKindNested,
		TagKindOmit, TagKindRetired, TagKindCommutative,
	} {
		if err := validateTagCombination(ParsedTag{Kind: k}); err != nil {
			t.Errorf("kind %v: got error %v, want nil (baseline no-op)", k, err)
		}
	}
}

// TestTagKindIsSecondary locks the IsSecondary classifier: only
// TagKindClearable is secondary; every other kind is primary.
// Covers: CL-03 (R-03 secondary-tag invariant — Kind never holds clearable).
func TestTagKindIsSecondary(t *testing.T) {
	secondary := []TagKind{TagKindClearable}
	for _, k := range secondary {
		if !k.IsSecondary() {
			t.Errorf("kind %v: IsSecondary() = false, want true", k)
		}
	}
	primary := []TagKind{
		TagKindNone, TagKindEntityKey, TagKindNested,
		TagKindOmit, TagKindRetired, TagKindCommutative,
	}
	for _, k := range primary {
		if k.IsSecondary() {
			t.Errorf("kind %v: IsSecondary() = true, want false", k)
		}
	}
}
