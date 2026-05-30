// Package readergen generates reflection-free Apache Arrow readers for Go
// structs. It is the reciprocal tool to writergen. Given one or more input
// packages and a list of target struct names, it resolves all field types
// recursively via gencommon and emits a single Go source file containing:
//
//   - A New<Struct>ArrowReader(rec arrow.RecordBatch) constructor that
//     validates the schema and pre-resolves all column indices and array types.
//   - A LoadRow(i int, out *<Struct>) method for zero-allocation row
//     extraction, reusing existing slice capacity and zeroing pointer targets
//     rather than allocating anew.
//   - An Errors() / ResetErrors() accumulator for non-fatal unmarshal failures.
//
// External types (from packages not listed in InputPkgs) are deserialised via
// interface fallbacks in the following priority order:
//
//  1. encoding.TextUnmarshaler  — read from an Arrow String column.
//  2. encoding.BinaryUnmarshaler — read from an Arrow Binary column.
//
// Unmarshal failures are non-fatal: the target field is left at its zero value
// and the error is buffered in the reader's accumulator. Fields whose type
// implements only fmt.Stringer (no unmarshal inverse) are skipped with a
// warning at generation time.
//
// When the same output package is targeted by multiple generator invocations
// (e.g. once for snapshots, once for deltas), the second invocation scans the
// existing companion files and elides any New<X>ArrowReader helpers that are
// already declared, printing a comment block listing what was omitted. See
// internal/arrow/gencommon/output_scan.go for the semantics and ordering
// constraints of this elision mechanism.
package readergen
