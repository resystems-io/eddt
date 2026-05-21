// Package nested_nyi provides a Snapshot fixture with a delta.nested field,
// used by TestEmitTemplate_NestedNotYet to verify that EM-01 rejects
// delta.nested with a Phase-5 sentinel error rather than silently mis-emitting.
package nested_nyi

import eddt "go.resystems.io/eddt/runtime"

// Sub is the nested struct; delta.nested on it requires Phase-5 (N-01).
type Sub struct{ X, Y int32 }

// NestedNYISnapshot carries one delta.nested field to trigger the sentinel.
type NestedNYISnapshot struct {
	eddt.Header
	Key  string `eddt:"entity.key"`
	Sub  Sub    `eddt:"delta.nested"`
	Name string
}
