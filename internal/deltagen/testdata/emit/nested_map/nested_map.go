// Package nested_map is the N-03 emit fixture. NestedMapSnapshot exercises
// two map fields:
//
//   - Tags uses a scalar value type (map[string]string); generated Diff uses !=.
//   - Scores uses a struct value type (map[string]Entry); Entry is comparable
//     (all-scalar fields), so generated Diff also uses != — no reflect required.
//
// The fixture is consumed by TestEmitTemplate_Nested_Map_SamePkg and
// TestEmitTemplate_Nested_Map_CrossPkg in template_test.go.
package nested_map

import eddt "go.resystems.io/eddt/runtime"

// Entry is used as a map value type to exercise struct-value comparison in the
// generated Diff function. Because all Entry fields are scalar, Entry is comparable
// in Go: the generator uses != directly (MapValueUseReflectEq=false).
type Entry struct {
	// Score is the numeric score value stored per key.
	Score int32
	// Label is a human-readable label associated with the score.
	Label string
}

// NestedMapSnapshot is the root Snapshot processed by the generator.
//
// Delta encoding for map fields (N-03, E-16 upsert semantics):
//   - Tags  → UpdatedTags map[string]string + RemovedTags []string
//   - Scores → UpdatedScores map[string]Entry  + RemovedScores []string
//   - Count remains an atomic *int32 field in TDelta (unchanged field handling).
type NestedMapSnapshot struct {
	eddt.Header
	// Key is the entity key used for EntityID computation.
	Key string `eddt:"entity.key"`
	// Tags is a string→string map encoded with element-wise delta (N-03).
	Tags map[string]string `eddt:"delta.nested"`
	// Scores is a string→Entry map encoded with element-wise delta (N-03).
	// Entry is comparable, so the generated Diff uses != for value comparison.
	Scores map[string]Entry `eddt:"delta.nested"`
	// Count is a plain atomic field; changes produce SetCount *int32 in TDelta.
	Count int32
}
