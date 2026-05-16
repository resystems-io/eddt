package deltagen

// tag_test.go exercises the eddt: tag parser introduced in T-01.
//
// # Group T: parseTag unit tests
//
// Tests are structured around the parsing responsibilities of parseTag:
//
//   T.01 Empty tag — "" produces TagKindNone with no options.
//   T.02 EntityKey — "entity.key" recognised.
//   T.03 Nested — "delta.nested" recognised.
//   T.04 Omit — "delta.omit" recognised.
//   T.05 Retired — "delta.retired" recognised.
//   T.06 Commutative — "delta.commutative" recognised.
//   T.07 KnownOption — "delta.retired,since=2026-01-15" → option preserved.
//   T.08 UnknownOption — "delta.nested,extra=foo" → unknown key preserved (E-07).
//   T.09 MultipleOptions — two options parsed independently.
//   T.10 CommutativeOption — commutative with reserved option key.
//   T.11 EntityKeyOption — entity.key with option preserved.
//   T.12 EmptyValue — "delta.retired,k=" → empty value accepted.
//   T.13 BareOption — option without "=" → error.
//   T.14 Clearable — "delta.clearable" → error (deferred to CL-03).
//   T.15 Unknown — "delta.bogus" → error (unrecognised).

import (
	"strings"
	"testing"
)

// TestParseTag is the umbrella for all Group T cases.
// Covers: R-15 (partial), E-07
func TestParseTag(t *testing.T) {
	t.Run("T01_Empty", func(t *testing.T) {
		pt, err := parseTag("")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pt.Kind != TagKindNone {
			t.Errorf("Kind: got %v, want TagKindNone", pt.Kind)
		}
		if pt.Options != nil {
			t.Errorf("Options: got %v, want nil", pt.Options)
		}
	})

	t.Run("T02_EntityKey", func(t *testing.T) {
		pt, err := parseTag("entity.key")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pt.Kind != TagKindEntityKey {
			t.Errorf("Kind: got %v, want TagKindEntityKey", pt.Kind)
		}
		if pt.Options != nil {
			t.Errorf("Options: got %v, want nil", pt.Options)
		}
	})

	t.Run("T03_Nested", func(t *testing.T) {
		pt, err := parseTag("delta.nested")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pt.Kind != TagKindNested {
			t.Errorf("Kind: got %v, want TagKindNested", pt.Kind)
		}
		if pt.Options != nil {
			t.Errorf("Options: got %v, want nil", pt.Options)
		}
	})

	t.Run("T04_Omit", func(t *testing.T) {
		pt, err := parseTag("delta.omit")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pt.Kind != TagKindOmit {
			t.Errorf("Kind: got %v, want TagKindOmit", pt.Kind)
		}
	})

	t.Run("T05_Retired", func(t *testing.T) {
		pt, err := parseTag("delta.retired")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pt.Kind != TagKindRetired {
			t.Errorf("Kind: got %v, want TagKindRetired", pt.Kind)
		}
	})

	t.Run("T06_Commutative", func(t *testing.T) {
		pt, err := parseTag("delta.commutative")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pt.Kind != TagKindCommutative {
			t.Errorf("Kind: got %v, want TagKindCommutative", pt.Kind)
		}
	})

	t.Run("T07_KnownOption", func(t *testing.T) {
		pt, err := parseTag("delta.retired,since=2026-01-15")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pt.Kind != TagKindRetired {
			t.Errorf("Kind: got %v, want TagKindRetired", pt.Kind)
		}
		if got := pt.Options["since"]; got != "2026-01-15" {
			t.Errorf("Options[since]: got %q, want %q", got, "2026-01-15")
		}
	})

	t.Run("T08_UnknownOption", func(t *testing.T) {
		// Unknown option keys are preserved without acting on them (E-07).
		pt, err := parseTag("delta.nested,extra=foo")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pt.Kind != TagKindNested {
			t.Errorf("Kind: got %v, want TagKindNested", pt.Kind)
		}
		if got := pt.Options["extra"]; got != "foo" {
			t.Errorf("Options[extra]: got %q, want %q", got, "foo")
		}
	})

	t.Run("T09_MultipleOptions", func(t *testing.T) {
		pt, err := parseTag("delta.retired,since=2026-01-15,reason=drop")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pt.Kind != TagKindRetired {
			t.Errorf("Kind: got %v, want TagKindRetired", pt.Kind)
		}
		if got := pt.Options["since"]; got != "2026-01-15" {
			t.Errorf("Options[since]: got %q, want %q", got, "2026-01-15")
		}
		if got := pt.Options["reason"]; got != "drop" {
			t.Errorf("Options[reason]: got %q, want %q", got, "drop")
		}
		if len(pt.Options) != 2 {
			t.Errorf("len(Options): got %d, want 2", len(pt.Options))
		}
	})

	t.Run("T10_CommutativeOption", func(t *testing.T) {
		pt, err := parseTag("delta.commutative,mode=lww")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pt.Kind != TagKindCommutative {
			t.Errorf("Kind: got %v, want TagKindCommutative", pt.Kind)
		}
		if got := pt.Options["mode"]; got != "lww" {
			t.Errorf("Options[mode]: got %q, want %q", got, "lww")
		}
	})

	t.Run("T11_EntityKeyOption", func(t *testing.T) {
		// entity.key with an unknown option: option is preserved (E-07).
		pt, err := parseTag("entity.key,scope=global")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pt.Kind != TagKindEntityKey {
			t.Errorf("Kind: got %v, want TagKindEntityKey", pt.Kind)
		}
		if got := pt.Options["scope"]; got != "global" {
			t.Errorf("Options[scope]: got %q, want %q", got, "global")
		}
	})

	t.Run("T12_EmptyValue", func(t *testing.T) {
		// An empty option value ("k=") is accepted; the key maps to "".
		pt, err := parseTag("delta.retired,k=")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pt.Kind != TagKindRetired {
			t.Errorf("Kind: got %v, want TagKindRetired", pt.Kind)
		}
		if got, ok := pt.Options["k"]; !ok || got != "" {
			t.Errorf("Options[k]: got (%q, %v), want (%q, true)", got, ok, "")
		}
	})

	t.Run("T13_BareOption", func(t *testing.T) {
		// A bare option with no "=" separator is malformed.
		_, err := parseTag("delta.nested,novalue")
		if err == nil {
			t.Fatal("expected error for bare option, got nil")
		}
		for _, want := range []string{"novalue", "key=value"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error should contain %q, got: %v", want, err)
			}
		}
	})

	t.Run("T14_Clearable", func(t *testing.T) {
		// delta.clearable is deferred to Phase 7 (CL-03); the baseline
		// parser returns an error for it.
		_, err := parseTag("delta.clearable")
		if err == nil {
			t.Fatal("expected error for delta.clearable in baseline, got nil")
		}
		if !strings.Contains(err.Error(), "delta.clearable") {
			t.Errorf("error should mention %q, got: %v", "delta.clearable", err)
		}
	})

	t.Run("T15_Unknown", func(t *testing.T) {
		_, err := parseTag("delta.bogus")
		if err == nil {
			t.Fatal("expected error for unrecognised tag value, got nil")
		}
		if !strings.Contains(err.Error(), "delta.bogus") {
			t.Errorf("error should mention %q, got: %v", "delta.bogus", err)
		}
	})
}
