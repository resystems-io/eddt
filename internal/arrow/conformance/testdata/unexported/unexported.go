// Package unexported holds a struct with an unexported field.
package unexported

type Record struct {
	Exported int64
	hidden   string
}
