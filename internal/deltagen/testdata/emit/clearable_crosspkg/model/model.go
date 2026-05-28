// Package model is the HK-16 clearable cross-package emit fixture.
//
// When delta-gen is invoked with OutPkgNameOverride="deltas" the generated
// file belongs to package "deltas" and must qualify source-package types:
//
//   - Function signatures use model.ClearableCrossPkgSnapshot.
//   - The zero-composite for the clearable struct field is model.Tag{}.
//   - TagDelta, AttrsMapDelta, and GroupsSliceDelta live in the output package
//     so they need no qualifier.
//   - No method wrappers are emitted in cross-package mode (E-12).
//
// The fixture is consumed by TestEmitTemplate_Clearable_CrossPkg.
package model

import eddt "go.resystems.io/eddt/runtime"

// Tag is the inner struct type for the clearable struct field.
// All fields are scalar so Tag is comparable (no reflect.DeepEqual needed).
type Tag struct {
	Name  string
	Value string
}

// ClearableCrossPkgSnapshot exercises clearable struct + map + slice fields
// in cross-package mode, verifying qualifier handling for generated wrapper
// types (TagDelta, AttrsMapDelta, GroupsSliceDelta) that live in the output package.
type ClearableCrossPkgSnapshot struct {
	eddt.Header
	Key    string            `eddt:"entity.key"`
	Label  Tag               `eddt:"delta.nested,delta.clearable"`
	Attrs  map[string]string `eddt:"delta.nested,delta.clearable"`
	Groups []string          `eddt:"delta.nested,delta.clearable"`
	Score  int32
}
