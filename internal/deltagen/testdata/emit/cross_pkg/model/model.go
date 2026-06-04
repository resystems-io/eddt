// Package model provides a cross-package Snapshot fixture. delta-gen is
// invoked with --pkg-name=deltas, exercising cross-package type qualification:
// the generated file belongs to package "deltas" and must qualify
// model-package type references (e.g. *model.Address).
package model

import eddt "go.resystems.io/eddt/runtime"

// Address is a value type used to verify cross-package qualification on
// struct-value fields: the emitted Delta field must render as *model.Address.
type Address struct{ Street, City string }

// ModelKey is the entity-key struct.
type ModelKey struct{ ID string }

// CrossPkgSnapshot is the fixture Snapshot in the model package.
// When emitted into package "deltas", field types from model need the
// "model." qualifier prefix.
type CrossPkgSnapshot struct {
	eddt.Header
	Key      ModelKey `eddt:"entity.key"`
	Name     string
	Location Address
}
