// Package writergen generates reflection-free Apache Arrow append writers for
// Go structs. Given one or more input packages and a list of target struct
// names, it resolves all field types recursively via gencommon and emits a
// single Go source file containing:
//
//   - A New<Struct>Schema() helper that builds the Arrow schema.
//   - A <Struct>ArrowWriter struct wrapping an array.RecordBuilder.
//   - Append(*<Struct>) and NewRecordBatch() methods for zero-copy columnar
//     serialisation.
//   - Append<Struct>Struct helpers used for nested struct fields.
//
// External types (from packages not listed in InputPkgs) are serialised via
// interface fallbacks in the following priority order:
//
//  1. encoding.TextMarshaler  — mapped to an Arrow String column.
//  2. fmt.Stringer            — mapped to an Arrow String column.
//  3. encoding.BinaryMarshaler — mapped to an Arrow Binary column.
//
// If a field's type implements none of the above, it is skipped with a warning.
//
// When the same output package is targeted by multiple generator invocations
// (e.g. once for snapshots, once for deltas), the second invocation scans the
// existing companion files and elides any New<X>Schema helpers that are already
// declared, printing a comment block listing what was omitted. See
// internal/arrow/gencommon/output_scan.go for the semantics and ordering
// constraints of this elision mechanism.
package writergen
