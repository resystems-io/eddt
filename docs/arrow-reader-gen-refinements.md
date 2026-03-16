# Arrow Reader Generator — Refinement Plan

This document captures the requirements and implementation plan for building `arrow-reader-gen`, a subsystem reciprocal to `arrow-writer-gen`. It provides a structured roadmap for generating strongly-typed Go readers from Apache Arrow memory records.

---

## 1. Context Analysis Summary

The existing `arrow-writer-gen` tool successfully converts Go structs into Arrow arrays by generating type-safe wrapper code over `array.RecordBuilder`. This process consists of two major components:
1. **Type Parsing and Resolution**: It uses `golang.org/x/tools/go/packages` to load Go ASTs, resolve types across packages, and model them internally via `StructInfo` and `FieldInfo`.
2. **Template Generation**: It uses `text/template` to emit Go code that populates Arrow builders (e.g., `*array.Int32Builder`, `*array.ListBuilder`).

To build `arrow-reader-gen`, we need to do the inverse: read from Arrow memory arrays (e.g., `*array.Int32`, `*array.List`) back into Go structs. Crucially, **the robust type parsing and resolution engine built for the writer can be heavily reused**. The primary work lies in extracting this shared logic and developing a new templating backend that emits array-reading loops and array-to-Go type conversions.

---

## 2. Requirements

### 2.1 Generator Inputs and Outputs
| ID | Requirement | Description |
|---|---|---|
| R1 | CLI Inputs | The generator must accept `--pkg` (input packages), `--struct` (target structs), `--out` (output file), and `--pkg-alias` flags, matching the writer's interface. |
| R2 | Shared Parser | The generator must use a shared `internal/arrow/gencommon` parsing engine rather than duplicating `packages.Load` and `FieldInfo` resolution. |
| R3 | Template Output | The tool must output formatted Go source code (`.go`) containing the generated reader structs and methods. |

### 2.2 Generated Code Inputs and Outputs
| ID | Requirement | Description |
|---|---|---|
| R4 | Column Lookup by Name | The generated `NewXxxArrowReader(rec arrow.Record)` must read the Arrow record's schema and lookup column indices dynamically by name, supporting permutations in column ordering. |
| R5 | Validate-at-Init, Infallible LoadRow | The generated reader follows a **validate-at-init** philosophy. `NewXxxArrowReader(rec)` must perform all schema validation — column downcasting, nested struct child-field counts, FixedSizeList length matching — and return an error if any check fails. Once initialization succeeds, `LoadRow(i int, out *Xxx)` is infallible (no error return) and trusts the validated schema. Row index bounds are the caller's responsibility via `rec.NumRows()`. |
| R6 | Zero-Allocation Reuse | Generated readers must provide a zero-allocation `LoadRow(i int, out *Xxx)` method. When populating slice fields in the pre-allocated struct pointer `out`, the reader MUST reuse existing slice capacity by overwriting elements if the slice is large enough, rather than reallocating or blindly appending. For map fields, the reader must use `clear(m)` and repopulate into the existing map to reuse the backing hash table. For pointer-to-struct fields, the reader must reuse the existing pointed-to struct (zero its fields and repopulate) rather than allocating a new one each row. |
| R7 | Null Handling | The generated code must check `IsValid(i)` (or `!IsNull(i)`) before extracting values. Nil collections or pointers must be handled safely. |
| R8 | Array Traversal | Nested types (Lists, Maps) must be traversed using offset arrays (e.g., `ValueOffsets(i)`) to correctly unpack sub-elements. |
| R9 | External Types | The generated code must support `encoding.TextUnmarshaler` and `encoding.BinaryUnmarshaler` for types that were written via `MarshalText` and `MarshalBinary`. Types that the writer serialized via `fmt.Stringer` (`.String()`) but that do not implement `TextUnmarshaler` cannot be round-tripped; the generator must emit a warning at generation time and skip the field in the reader output. |
| R10 | Null Values in Non-Pointer Fields | The generated reader must handle Arrow nulls for Go value types (e.g., `int32`, `string`) safely by writing the zero value of the type into the pre-allocated struct, ensuring previous row data is not accidentally preserved. |
| R11 | Missing Columns | The generated reader must handle cases where the Arrow record is missing columns that exist in the Go struct (schema evolution). Missing columns should be skipped gracefully, leaving the Go field at its zero value or previous state. |
| R12 | Dictionary Encoding | The generated reader must seamlessly handle dictionary-encoded arrays (e.g., `*array.Dictionary`) for string/binary columns, which are common in Parquet-to-Arrow conversions, by resolving the dictionary index to the underlying value. |
| R13 | Embedded Struct Flattening | The generated reader must correctly reconstruct Go structs that contain embedded (anonymous) fields. The writer flattens embedded struct fields into top-level Arrow columns; the reader must map those flattened columns back into the correct embedded struct hierarchy within the target Go type. Ambiguity detection (duplicate field names across multiple embedded structs) must mirror the writer's behavior. |
| R14 | Named Composite Types | The generated reader must correctly assign to fields whose types are named composites (e.g., `type Tags []string`, `type Scores map[string]int`). The reader must emit explicit type conversions (e.g., `out.Tags = Tags(slice)`) rather than assigning bare composite literals, which would fail to compile for named types. |
| R15 | Byte and Rune Special Cases | The reader must handle the writer's special-case mappings: `[]byte` is stored as `arrow.BinaryTypes.Binary` (not `ListOf(Uint8)`) and must be read from `*array.Binary`; `byte` fields must cast from `uint8`; `rune` fields must cast from `int32`. |
| R16 | Accumulated Error Report | Although `LoadRow` is infallible (R5), runtime errors can still occur for unmarshal operations (`TextUnmarshaler`, `BinaryUnmarshaler`) where payload content is invalid despite a valid schema. Rather than returning per-row errors, `LoadRow` must accumulate these as `ReadError{Row int, Field string, Err error}` entries on the reader struct. The affected field is left at its zero value. The caller consults `Errors() []ReadError` after a batch and calls `ResetErrors()` (which reuses the slice) between batches. The accumulator is unbounded — the caller is responsible for checking mid-batch if early termination on error is desired. For readers with no unmarshal fields, the error slice is never touched (zero overhead). |

---

## 3. Implementation Plan

Based on the `system-refinement` process, this work is divided into incremental phases.

### Phase 1: Refactoring (Shared Engine)
> **Note:** This phase extracts the shared engine before the reader skeleton exists. The writer's parser is mature and the reader's needs are well-specified (F2), but a second pass on the `gencommon` boundary may be needed after Phase 2 once the reader template's actual consumption patterns are known.

- [x] **F1: Extract `gencommon`:** Move `StructInfo`, `FieldInfo`, `loadPackages()`, and AST resolution logic from `writer-gen/generator.go` to `internal/arrow/gencommon`.
- [x] **F2: Augment `FieldInfo`:** Add reader-specific fields to `FieldInfo`. The following are needed for the reader template:
  - `ArrowArrayType` — concrete array type for downcast (e.g., `*array.Int32`, `*array.List`)
  - `ValueMethod` — extraction method (e.g., `.Value(i)`, `.ValueStr(i)`)
  - `UnmarshalMethod` — reciprocal of `MarshalMethod` (e.g., `UnmarshalText`, `UnmarshalBinary`; empty for Stringer-only types)
  - `ConvertBackExpr` — inverse of `ConvertMethod`; a template snippet since inverses are constructors, not methods (e.g., `time.Unix(0, %s)`)
  - `ZeroExpr` — zero-value expression for the Go type (e.g., `0`, `""`, `false`), used for R10 null handling
- [x] **F3: Verify `writer-gen`:** Run the existing `writer-gen` test suite to ensure the extraction did not break writer code generation.
- [x] **F4: Cross-Package Template Concerns:** Verify that the shared engine exposes sufficient information for the reader template to emit correct cross-package imports, qualify types in `LoadRow` (e.g., `out.Address = otherpkg.Address{...}`), and detect reserved-name collisions (`arrow`, `array`, `memory`). These concerns are handled by the writer today and must carry over to the reader template without duplication.

### Phase 2: Skeleton and Primitives
- [x] **P1: Create reader-gen CLI:** Scaffold `cmd/arrow-reader-gen/main.go` and `internal/arrow/reader-gen/generator.go`.
- [x] **P2: Basic Reader Template:** Create `reader-gen/template.go` with the `NewXxxArrowReader` initialization logic. This must include reading the Arrow record schema, finding column indices by name (to support layout permutations), caching the downcasted columns, and emitting the `LoadRow(i int, out *Xxx)` body.
- [x] **P3: Primitive Loading:** Implement template logic for basic primitives (`int32`, `float64`, `string`, `bool`). Call `.Value(i)` and cast to the Go type.
- [x] **P4: Pointers to Primitives:** Implement null-checking (`IsValid(i)`) and pointer allocation for primitive fields.

### Phase 3: Nested Types
- [x] **N1: Slices and Lists:** Implement array traversal for `arrow.ListOf` using `ValueOffsets`. Generate loops to allocate and populate Go slices.
- [ ] **N2: Fixed-Size Arrays:** Implement iteration for fixed-size lists (where length is known statically).
- [ ] **N3: Maps:** Implement map traversal using `List` offsets, extracting from the underlying `Struct` array's `Keys()` and `Items()`.
- [ ] **N4: Nested Structs:** Implement recursive extraction of nested structs from `*array.Struct` using `.Field(childIndex)`.

### Phase 4: External, Well-Known, and Edge-Case Types
- [ ] **E1: Unmarshaler Support:** Implement template logic to detect `TextUnmarshaler` and `BinaryUnmarshaler`. Extract the string/binary, instantiate the type, and call the unmarshal method.
- [ ] **E2: Well-Known Types:** Implement reciprocal conversions for well-known types. Note that the inverse conversions are structurally different from the writer's — they are constructors or composite expressions, not simple method calls:
  - `time.Duration` ← `time.Duration(int64Value)`
  - `time.Time` ← `time.Unix(0, int64Value)`
  - `durationpb.Duration` ← `durationpb.New(time.Duration(int64Value))`
  - `timestamppb.Timestamp` ← `timestamppb.New(time.Unix(0, int64Value))`

  `FieldInfo` must carry a `ConvertBackExpr` template snippet (or equivalent) rather than a simple method name, since a single `ConvertMethod` string cannot represent these patterns.
- [ ] **E3: Nulls in Value Types:** Ensure the template writes the zero-value for primitives/structs when `!IsValid(i)` is true to avoid dirty reads from slice reuse.
- [ ] **E4: Dictionary Encoding:** Implement template branching to handle dictionary-encoded arrays (`*array.Dictionary`) alongside plain arrays for string/binary columns. During `NewXxxArrowReader` initialization, detect whether each applicable column is dictionary-encoded and store a flag on the reader struct. `LoadRow` uses the flag to branch between dictionary lookup (index → value dictionary) and direct `.Value(i)` access. This avoids init-time materialization and preserves compatibility with memory-mapped Arrow files where allocation would defeat zero-copy benefits.
- [ ] **E5: Missing Columns Strategy:** Ensure the `LoadRow` template wraps column reads in `if col != nil` or `isValid` checks so missing columns are ignored rather than causing a nil pointer panic.

### Phase 5: Verification
- [ ] **V1: Unit Tests:** Add unit tests testing the reader generator's parser mapping logic.
- [ ] **V2: Integration Round-Trip:** Create `internal/arrow/reader-gen/integration_test.go`. Ensure a test case serializes a complex struct using `writer-gen`, yields the `arrow.Record`, passes it to the `reader-gen` code, and asserts equality (`require.Equal(t, original, read)`). The existing writer-gen integration test helpers (`setupIntegrationTest`, `runInnerTest`, `tarball`) should be extracted to a shared test utility package so both generators can reuse them.
- [ ] **V3: Benchmarking:** Add a benchmark to ensure `LoadRow` performance meets expectations for zero-allocation reading.

### Phase 6: Documentation
- [ ] **D1: Update README/Docs:** Update `docs/arrow-reader-gen.md` detailing the generated API, usage examples, and supported types.
- [ ] **D2: Refinements Checklist:** Mark this document's changelog and task items as complete.

---

## 4. Change Log

Record completed items here with the date (check git blame for the git commit).

| Date       | Item | Notes                                                   |
|------------|------|---------------------------------------------------------|
| 2026-03-16 | F1   | Extracted `internal/arrow/gencommon` with `FieldInfo`, `StructInfo`, `ImportInfo`, `Parse()`, all resolution and builder functions, `DetectStructNameCollisions`, `FilterUnexportedFields`. Writer-gen reduced to thin `Generator` wrapper + template. Five tests migrated to gencommon. |
| 2026-03-16 | F3   | Full writer-gen test suite verified: unit tests, integration tests (DuckDB round-trip), and benchmarks all pass. |
| 2026-03-16 | F2   | Added five reader-specific fields to `FieldInfo`: `ArrowArrayType`, `ValueMethod`, `UnmarshalMethod`, `ConvertBackExpr`, `ZeroExpr`. Added three helper functions (`arrowArrayType`, `unmarshalForMarshal`, `zeroExprForCast`). Updated all 14 FieldInfo construction sites in resolve.go. Added `TestArrowArrayType`, `TestUnmarshalForMarshal`, `TestZeroExprForCast`, `TestReaderFieldsPopulated` in gencommon. Updated `TestGenerator_Parse` expected values. Full test suite verified (unit, integration, benchmarks). |
| 2026-03-16 | F4   | Extracted cross-package resolution pipeline from writer-gen `Run()` into gencommon: `ParsePkgAliases()` and `ResolveOutputContext()` (import map building, alias resolution, reserved name validation, qualifier assignment, unexported field filtering). Added `FieldInfo.StructQualifier` for struct-typed field qualification (propagated recursively through EltInfo/KeyInfo). Reserved names parameterized so reader can use `{"arrow","array"}` vs writer's `{"arrow","array","memory"}`. Writer's `Run()` reduced from ~130 to ~20 lines. Added `TestParsePkgAliases` (6 cases) and `TestResolveOutputContext` (11 cases) to gencommon. Full test suite verified (unit, integration, benchmarks). |
| 2026-03-16 | P1   | Scaffolded `cmd/arrow-reader-gen/main.go` (cobra CLI with same flags as writer-gen: `--pkg`, `--structs`, `--out`, `--pkg-name`, `--pkg-alias`, `--verbose`) and `internal/arrow/reader-gen/generator.go` (thin `Generator` wrapper delegating to `gencommon.Parse()` and `gencommon.ResolveOutputContext()`, with `Run()` stub for P2 template work). Reader-gen reserves `{"arrow","array"}` (not `"memory"`). Added `TestGenerator_Parse` (full FieldInfo round-trip) and `TestGenerator_RunReservedNames` (3 cases: arrow/array reserved, memory not reserved). Full test suite verified (unit, integration, benchmarks). |
| 2026-03-16 | P2+P3 | Created `reader-gen/template.go` with reader template: generates `XxxArrowReader` struct (typed column fields), `NewXxxArrowReader(rec)` constructor (validate-at-init with column lookup by name via `schema.FieldIndices`, type assertion per column, missing columns stay nil), and `LoadRow(i, out)` method (nil-guarded per-field reads, null→zero-value, cast uses `GoType` not `CastType` for correct named-type round-trip). Completed `Run()` in `generator.go` (template execution, `format.Source`, `os.WriteFile`). Covers all P3 primitive types: `int{8..64}`, `uint{8..64}`, `float{32,64}`, `string`, `bool`, `byte`, `rune`, `[]byte` (Binary), named-over-primitive (e.g. `MyID int32`). Template guards skip pointers, containers, structs, marshal, and convert fields (deferred to P4/N1-N4/E1-E5). Added `integration_test.go` with write-then-read round-trip test (writer-gen + reader-gen in subprocess, `SimpleStruct` with 7 fields including named type, verifies non-zero row and zero-value row). Full test suite verified (unit, integration, gencommon, writer-gen + benchmarks). |
| 2026-03-16 | P4   | Relaxed `(not .IsPointer)` guards on `colField` and `initField` sub-templates so pointer-to-primitive fields get column struct fields and init-time downcast. Added pointer-specific `loadField` branch with three-way logic: null→nil (clears previous allocation), non-null with nil pointer→allocate via `&v`, non-null with existing pointer→dereference-assign `*out.Field` (R6 zero-allocation reuse). Uses `stripPtr` FuncMap helper for cast expression (`*MyID` → `MyID(v)`). Added `TestGenerator_ParsePointerPrimitives` (5 pointer fields: `*int32`, `*float64`, `*bool`, `*string`, `*MyID`). Added `pointer-to-primitive-round-trip` integration test (write two rows with non-nil/nil pointers, read back, verify null→nil clears previous allocation, verify R6 pointer address stability on reuse). Full test suite verified. |
| 2026-03-16 | N1   | Added list/slice support to reader-gen template. Six new sub-templates: `colFieldList` (recursive struct field generation with `Elts` suffix chaining), `initFieldList`/`initFieldListChild` (top-level schema lookup + recursive child array downcast), `loadFieldList`/`loadFieldListInner` (recursive extraction loop with depth-based variable naming `s0/e0/n0/j0`). Added `add` and `repeat` FuncMap helpers. Modified `colField`/`initField`/`loadField` guards to dispatch to list branches. Supports arbitrary nesting depth (`[]T`, `[][]T`, etc.). R6 capacity reuse at every level (with `n > 0` guard to preserve non-nil empty slice semantics). R7 null handling: null list → nil slice, null inner list → nil inner slice. Added `TestGenerator_ParseListFields` (3 fields: `[]string`, `[][]int32`, `[][]byte`). Added `list-round-trip` integration test (3 rows: non-empty, nil, empty; R6 backing array reuse; null clearing) and `nested-list-round-trip` integration test (3 rows: nested values, null outer, nil inner + non-nil inner). Full test suite verified (reader-gen, gencommon, writer-gen). |
