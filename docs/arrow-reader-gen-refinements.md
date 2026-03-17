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
- [x] **N2: Fixed-Size Arrays:** Implement iteration for fixed-size lists (where length is known statically).
- [x] **N3: Maps:** Implement map traversal using `List` offsets, extracting from the underlying `Struct` array's `Keys()` and `Items()`.
- [x] **N4: Nested Structs:** Implement recursive extraction of nested structs from `*array.Struct` using `.Field(childIndex)`.

### Phase 4: External, Well-Known, and Edge-Case Types
- [x] **E1: Unmarshaler Support:** Implement template logic to detect `TextUnmarshaler` and `BinaryUnmarshaler`. Extract the string/binary, instantiate the type, and call the unmarshal method.
- [x] **E2: Well-Known Types:** Implement reciprocal conversions for well-known types. Note that the inverse conversions are structurally different from the writer's — they are constructors or composite expressions, not simple method calls:
  - `time.Duration` ← `time.Duration(int64Value)`
  - `time.Time` ← `time.Unix(0, int64Value)`
  - `durationpb.Duration` ← `durationpb.New(time.Duration(int64Value))`
  - `timestamppb.Timestamp` ← `timestamppb.New(time.Unix(0, int64Value))`

  `FieldInfo` must carry a `ConvertBackExpr` template snippet (or equivalent) rather than a simple method name, since a single `ConvertMethod` string cannot represent these patterns.
- [x] **E3: Nulls in Value Types:** Ensure the template writes the zero-value for primitives/structs when `!IsValid(i)` is true to avoid dirty reads from slice reuse.
- [x] **E4: Dictionary Encoding:** Implement template branching to handle dictionary-encoded arrays (`*array.Dictionary`) alongside plain arrays for string/binary columns. During `NewXxxArrowReader` initialization, detect whether each applicable column is dictionary-encoded and store a flag on the reader struct. `LoadRow` uses the flag to branch between dictionary lookup (index → value dictionary) and direct `.Value(i)` access. This avoids init-time materialization and preserves compatibility with memory-mapped Arrow files where allocation would defeat zero-copy benefits.
- [x] **E5: Missing Columns Strategy:** Ensure the `LoadRow` template wraps column reads in `if col != nil` or `isValid` checks so missing columns are ignored rather than causing a nil pointer panic.

### Phase 5: Verification
- [x] **V1: Unit Tests:** Add unit tests testing the reader generator's parser mapping logic.
- [x] **V2: Integration Round-Trip:** Create `internal/arrow/reader-gen/integration_test.go`. Ensure a test case serializes a complex struct using `writer-gen`, yields the `arrow.Record`, passes it to the `reader-gen` code, and asserts equality (`require.Equal(t, original, read)`). The existing writer-gen integration test helpers (`setupIntegrationTest`, `runInnerTest`, `tarball`) should be extracted to a shared test utility package so both generators can reuse them.
- [ ] **V3: Benchmarking:** Add a benchmark to ensure `LoadRow` performance meets expectations for zero-allocation reading.
- [x] **V4: Edge-Case and Coverage Tests:** Integration tests for specific scenarios not covered by V2's primary round-trip tests.
  - [x] **V4a: BinaryUnmarshaler Round-Trip:** Add an integration test exercising `encoding.BinaryUnmarshaler` (not just `TextUnmarshaler`). Define a type implementing `MarshalBinary`/`UnmarshalBinary`, verify writer→reader round-trip including error-on-invalid-payload and zero-on-error semantics.
  - [x] **V4b: Init-Time Error Paths (R5 negative):** Add integration tests that manually build Arrow records with incompatible column types (e.g., `*array.Int32` where `*array.String` is expected) and verify `NewXxxArrowReader` returns a descriptive error. Also test the dictionary-values type mismatch path (e.g., dictionary wrapping `*array.Int32` for a string column).
  - [x] **V4c: Embedded Struct Flattening (R13):** Add an integration test with embedded (anonymous) struct fields. Verify the reader correctly maps flattened top-level Arrow columns back into the embedded struct hierarchy. Include a case with multiple levels of embedding.
  - [x] **V4d: Missing Columns (R11/E5):** Add an integration test that builds an Arrow record missing one or more columns present in the Go struct. Verify `NewXxxArrowReader` succeeds and `LoadRow` leaves the corresponding Go fields untouched (at their zero or previous value).

### Phase 6: Documentation
- [ ] **D1: Update README/Docs:** Update `docs/arrow-reader-gen.md` detailing the generated API, usage examples, and supported types.
- [ ] **D2: Refinements Checklist:** Mark this document's changelog and task items as complete.

---

## 4. Change Log

Record completed items here with the date (check git blame for the git commit).

| Date       | Item | Notes                                                   |
|------------|------|---------------------------------------------------------|
| 2026-03-16 | F1   | Extracted `internal/arrow/gencommon` from writer-gen. Writer-gen reduced to thin wrapper + template. |
| 2026-03-16 | F3   | Writer-gen test suite verified (unit, integration, benchmarks). |
| 2026-03-16 | F2   | Added reader-specific fields to `FieldInfo`: `ArrowArrayType`, `ValueMethod`, `UnmarshalMethod`, `ConvertBackExpr`, `ZeroExpr`. Updated all 14 construction sites. |
| 2026-03-16 | F4   | Extracted cross-package resolution into `ResolveOutputContext()`. Added `StructQualifier` propagation. Parameterized reserved names per generator. |
| 2026-03-16 | P1   | Scaffolded `cmd/arrow-reader-gen` CLI and `reader-gen/generator.go` with `Parse()`/`Run()` delegating to gencommon. |
| 2026-03-16 | P2+P3 | Reader template with `XxxArrowReader` struct, validate-at-init `New` constructor, and `LoadRow`. Covers all primitive types including `[]byte`, named-over-primitive. Cast uses `GoType` (not `CastType`) for named-type round-trip. |
| 2026-03-16 | P4   | Pointer-to-primitive support: null→nil, nil→allocate, non-nil→dereference-assign (R6 reuse). `stripPtr` FuncMap helper. |
| 2026-03-16 | N1   | List/slice support with arbitrary nesting depth. Recursive `colFieldList`/`initFieldList`/`loadFieldList` sub-templates. R6 capacity reuse, R7 null→nil. |
| 2026-03-16 | N2   | Fixed-size array support via `loadFieldFixedList`/`loadFieldFixedListInner`. Cross-nesting with lists (`[][3]T`, `[3][]T`). Null→zero-value (not nil). |
| 2026-03-17 | N3   | Map reading with recursive `colFieldMap`/`initFieldMap`/`loadFieldMap` sub-templates. `clear(m)` for R6 reuse. Cross-nesting: map→list delegation. Supports nested maps, list values, int keys, named map types. |
| 2026-03-17 | N4   | Nested struct reading via dual-constructor pattern (`NewXxx` for records, `newXxxFromStruct` for children). `initStructField` template for struct-level init. Pointer-to-struct R6 reuse. Map struct values use temporary variable for addressability. |
| 2026-03-17 | E2   | Well-known type round-tripping via `ConvertBackExpr`. Added `ConvertBackIsPtr` and `ConvertBackImports` to `FieldInfo`. Fixed protobuf `ZeroExpr` (was `"nil"` for value types). Relaxed `ConvertMethod` guards. `ConvertBackExpr` branching in `loadField` (value + pointer) and all three container inner templates. `CollectConvertBackImports` for import merging. |
| 2026-03-17 | E1+R16 | Unmarshal support for TextMarshaler/BinaryMarshaler types with error accumulation. Fixed `buildMarshalFieldInfo` to produce short GoType (`netip.Addr` not `net/netip.Addr`). `ReadError` type + `Errors()`/`ResetErrors()` per reader, gated by `HasUnmarshalFields` (zero overhead otherwise). Refactored `loadField` to dict invocation to thread `HasUM` flag. Relaxed `MarshalMethod` guards to admit unmarshal-able fields. Unmarshal branches for value/pointer/container elements with zero-on-error. Nested struct error drain with field-path prefixing. Stringer-only fields emit warning and are skipped. |
| 2026-03-17 | E3   | Added explicit `IsNull` guards to container inner templates (`loadFieldListInner`, `loadFieldFixedListInner`, `loadFieldMapInner`) for plain-primitive and ConvertBackExpr element branches. Null elements now write `ZeroExpr` instead of relying on Arrow's implicit zero. Fixes `time.Time` null elements producing `time.Unix(0,0)` instead of `time.Time{}`. |
| 2026-03-17 | E4   | Dictionary encoding support via type-switch init + per-field `*array.Dictionary` reference. `isDictCandidate` FuncMap helper and `dictResolve` sub-template. Covers top-level and struct-child `*array.String`/`*array.Binary` columns. Dict tests for string (value + pointer) and binary round-trips. |
| 2026-03-17 | E5   | Already implemented — all `LoadRow` branches guarded by `if r.col<Name> != nil`, init uses `len(indices) > 0` / `FieldIdx` checks. No code changes needed, just checked off. |
| 2026-03-17 | V1   | Already covered — `gencommon/resolve_test.go` (4 core mapping function unit tests + `TestReaderFieldsPopulated` for 14 field types), `gencommon/gencommon_test.go` (9 `ResolveOutputContext` sub-tests, alias parsing, package loading), `reader-gen/generator_test.go` (7 Parse-level FieldInfo assertion tests). |
| 2026-03-17 | V2   | Already covered — 30 integration sub-tests in `reader-gen/integration_test.go` spanning all major type categories, well-known types, unmarshal types, dictionary encoding, R6 reuse, R7/R10 null handling, R9 stringer skip, R16 error accumulation, E3 null container elements. Both writer-based and manual record building. |
| 2026-03-17 | V4a  | BinaryUnmarshaler round-trip test. Custom `BinVal` type in separate package (required so parser uses marshal path, not nested-struct path). Writer→reader round-trip for value + pointer fields, nil pointer, zero value. Error accumulation with invalid binary payload (wrong length), zero-on-error, `ResetErrors()`. |
| 2026-03-17 | V4b  | Init-time error path tests. Four sub-cases: wrong type for non-dict column (`*array.String` where `*array.Int32` expected), wrong type for dict-candidate column (`*array.Int32` where `*array.String`/`*array.Dictionary` expected), dictionary values type mismatch (`*array.Binary` dict where `*array.String` values expected), and correct types succeed. |
| 2026-03-17 | V4c  | Embedded struct flattening round-trip. `Device` embeds `Base` (ID, CreatedAt); writer flattens to 3 top-level columns; reader maps them back via promoted field access. Verifies both `out.ID` (promoted) and `out.Base.ID` (explicit) produce the same value. |
| 2026-03-17 | V4d  | Missing columns test. Builds a partial Arrow record missing the `Score` column. Verifies init succeeds, present fields populate correctly, and missing field is left **untouched** (pre-set `99.9` survives `LoadRow`) — distinguishing R11 skip from R10 null→zero. |
