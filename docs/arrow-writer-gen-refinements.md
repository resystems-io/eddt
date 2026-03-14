# Arrow Writer Generator — Refinement Plan

This document captures the results of a type-support audit conducted on `arrow-writer-gen`
and provides a prioritised checklist of recommended changes. Items are intended to be
addressed one at a time in future sessions; tick the checkbox and note the date/commit
when each item is completed.

---

## 1. Audit Summary

### 1.1 Supported Types (as of 2026-03-13)

**Primitives:**

| Go Type | Arrow Type | Notes |
|---------|-----------|-------|
| `int8` | `Int8` | |
| `int16` | `Int16` | |
| `int32` | `Int32` | |
| `int64`, `int` | `Int64` | `int` widened to 64-bit |
| `uint8`, `byte` | `Uint8` | `byte` is an alias |
| `uint16` | `Uint16` | |
| `uint32` | `Uint32` | |
| `uint64`, `uint` | `Uint64` | `uint` widened to 64-bit |
| `float32` | `Float32` | |
| `float64` | `Float64` | |
| `bool` | `Boolean` | |
| `string` | `String` | |
| `[]byte` | `Binary` | Special-cased; not `ListOf(Uint8)` |

**Composites:**

| Go Pattern | Arrow Type | Notes |
|-----------|-----------|-------|
| Local/cross-pkg struct | `Struct` | Via `AppendXxxStruct` helper |
| `*T` (pointer to primitive) | Same as `T` | Nullable; nil → `AppendNull` |
| `*struct` | `Struct` | Nullable |
| `[]T` (primitive slice) | `ListOf(T)` | nil slice → `AppendNull` |
| `[]struct` / `[]*struct` | `ListOf(Struct)` | nil element → `AppendNull` for pointer |
| `[]*T` (pointer element) | `ListOf(T)` | nil → `AppendNull` |
| `map[K]V` (primitives) | `MapOf(K, V)` | nil map → `AppendNull` |
| `map[K]struct` | `MapOf(K, Struct)` | |
| Named primitive (`type X int`) | Underlying Arrow type | Cast to underlying |
| `time.Duration` | `Int64` | Stored as nanoseconds via `int64(d)` |
| `time.Time` | `Timestamp_ns` (UTC) | Stored via `arrow.Timestamp(t.UnixNano())` |
| `durationpb.Duration` | `Int64` | Via `int64(d.AsDuration())` — nanoseconds |
| `timestamppb.Timestamp` | `Timestamp_ns` (UTC) | Via `arrow.Timestamp(t.AsTime().UnixNano())` |
| External type (`MarshalText`) | `String` | e.g. `netip.Addr` |
| External type (`String()`) | `String` | e.g. `url.URL` |
| External type (`MarshalBinary`) | `Binary` | |
| `[]*ExternalType` | `ListOf(String/Binary)` | Via marshal fallback on elements |

**Intentionally unsupported (correctly skipped with warning):**

| Go Type | Reason |
|---------|--------|
| `interface{}` / `any` | No static type; cannot determine Arrow schema |
| `func()` | Not data |
| `chan T` | Not data |
| `uintptr` | Platform-dependent pointer; not meaningful as columnar data |

### 1.2 Bugs — Generate Broken (Non-Compiling) Code

These are cases where the generator produces an output file that will not compile
or will panic at runtime. They are the highest priority because they silently break
the user's build.

| ID | Go Type | Symptom |
|----|---------|---------|
| B1 | `[N]T` (fixed-size array, e.g. `[4]int32`) | Treated as a slice; generates `if row.F == nil` which won't compile (arrays are value types, never nil). The generated schema is also semantically wrong (variable-length list vs fixed-length). |
| B2 | `_ int32` (blank/underscore field name) | Not filtered out during parse; generates `row._` in the Append body, which is a compile error — Go does not allow referencing blank-identifier fields. |
| B3 | `[][]T` (nested slice) | Schema is correct (`ListOf(ListOf(T))`), but the generated Append code calls `valBldr.Append(v)` where `v` is a `[]T` — the inner `ListBuilder` does not accept a slice argument. The inner list elements are never iterated. |
| B4 | `map[K][]V` (map with slice value) | Same root cause as B3 — the map's value builder is a `ListBuilder` but the generated code tries to pass the entire slice as a single Append argument instead of iterating it. |
| B5 | `map[K]map[K2]V` (nested map) | Same pattern — the inner `MapBuilder` receives a `map` value directly instead of being iterated key-by-key. |
| B6 | Unexported fields in cross-package mode | When `--pkg-name` differs from the input package, the generated `Append` function receives `row *mypkg.Foo` but emits `row.unexportedField` — accessing unexported fields from another package is a compile error. The parse loop has no `ast.IsExported` guard. Only triggered with `--pkg-name` override; same-package generation (the default) is unaffected. |

### 1.3 Missing Primitive — Easy Win

| ID | Go Type | Issue |
|----|---------|-------|
| M1 | `rune` | `rune` is a built-in alias for `int32`, identical to how `byte` aliases `uint8`. Currently rejected as "unsupported primitive type: rune". Fix is a one-line addition to the `mapToArrowType` switch. |

### 1.4 Structural Gaps

| ID | Go Pattern | Issue |
|----|-----------|-------|
| S1 | Embedded structs (`type T struct { Base; Name string }`) | ~~Embedded fields have `len(field.Names) == 0` and are silently skipped.~~ **Fixed (2026-03-14).** Promoted fields are now flattened into the parent schema. Pointer-embedded structs (`*Base`) are skipped with warning (future work). |
| S2 | Arbitrary nesting depth (`[][][]T`, `[][]map[K]V`, etc.) | `FieldInfo` uses flat `Val*` fields to describe one level of element nesting, limiting nested slices to depth 2 and requiring ad-hoc fields for each new nesting pattern (B4, B5). A recursive `FieldInfo` (with `EltInfo *FieldInfo` and `KeyInfo *FieldInfo`) would remove the depth limit and unify the duplicated element dispatch logic in the template. |
| S3 | Struct name collision across packages | The `processed` map and `queue` use bare struct names (e.g. `"Inner"`). If pkg1 and pkg2 both define `Inner`, the second is silently skipped. The generated code would reference only one `AppendInnerStruct`, which may correspond to the wrong package's struct. Fix: key `processed` by qualified name (`pkgPath + "." + structName`). |
| S4 | Named slice/map types (`type Tags []string`, `type MyBytes []byte`) | Named types whose underlying type is a slice or map fall through `resolveIdent` (which checks `*types.Struct` and `*types.Basic` but not `*types.Slice` or `*types.Map`) and hit `mapToArrowType`, which fails. The field is skipped with a warning. Fix: add `*types.Slice` and `*types.Map` checks to `resolveIdent` and recurse into the underlying element/key/value types. |

### 1.5 Debatable / Low Priority

| ID | Go Type | Notes |
|----|---------|-------|
| D1 | `complex64` / `complex128` | No native Arrow complex type. Could decompose into a two-field struct (`real float32, imag float32`) but practical demand is low. |
| D2 | `time.Duration` stored as string | Currently `time.Duration` (named `int64` from the `time` package) resolves via the `SelectorExpr` path and hits `String()` → stored as `"1h30m0s"`. Arrow has a native `DurationType` with `DurationBuilder` (int64 storage, configurable time unit: s/ms/us/ns). However, **Parquet has no native Duration logical type** — `pqarrow` returns `ErrNotImplemented` when converting `arrow.DURATION` to a Parquet schema. This means Arrow Duration columns cannot be written to Parquet, which is the primary serialization target. See D2 checklist entry for options. |

---

## 2. Refinement Checklist

Items are grouped by priority. Within each group, the suggested order reflects
dependency and effort.

### Priority 1 — Fix Broken Code Generation

These must be fixed first because they produce output that does not compile.

- [x] **B1: Support fixed-size arrays** *(2026-03-13)* — `[N]T` is now mapped to
  `arrow.FixedSizeListOfNonNullable(N, T)` with `*array.FixedSizeListBuilder`.
  No nil check is generated (arrays are value types). Element iteration uses
  the same dispatch logic as variable-length lists.
  - Files: `generator.go` (`mapToFieldInfo` ArrayType case, `FieldInfo` struct),
    `template.go` (new `IsFixedSizeList` branch), `generator_test.go`

- [x] **B2: Skip blank-identifier fields** *(2026-03-13)* — In `Parse()`, blank-
  identifier fields (`_ T`) are now filtered out with a `fieldName == "_"` guard.
  This prevents the generator from emitting `row._` which does not compile.
  - Files: `generator.go` (parse loop), `generator_test.go` (new table case),
    `integration_test.go` (new `blank-identifier-field` subtest)

- [x] **B3: Support nested slices (`[][]T`)** *(2026-03-13, superseded by S2 on 2026-03-14)* —
  Initially implemented with flat `Val*` fields for depth-2 support. Now handled
  by the recursive `FieldInfo`/`appendValue` architecture (S2) which supports
  arbitrary nesting depth.
  - Files: `generator.go`, `template.go`, `generator_test.go`,
    `integration_test.go` (nested-slices subtest)

- [x] **B4: Handle `map[K][]V`** *(2026-03-14)* — Resolved by S2. The recursive
  `FieldInfo` and `appendValue` template naturally handle map values of any type,
  including slices. No ad-hoc fields needed.
  - Files: resolved as part of S2 refactor

- [x] **B5: Handle `map[K]map[K2]V`** *(2026-03-14)* — Resolved by S2. Same as B4;
  the recursive template dispatches inner maps through the same `IsMap` branch.
  - Files: resolved as part of S2 refactor

- [x] **B6: Skip unexported fields in cross-package generation** *(2026-03-14)* —
  When `--pkg-name` differs from the input package, the generated code accesses
  fields via `row *mypkg.Foo`, but unexported fields (`row.name`) are inaccessible
  from another package and produce a compile error. Added `filterUnexportedFields`
  helper in `template.go` that uses `token.IsExported` to filter fields on structs
  with a non-empty `Qualifier` (cross-package signal). Called in `Run()` between
  Qualifier-setting and template execution. Emits a warning for each skipped field.
  Same-package generation is unaffected. Promoted unexported fields from embedded
  structs are also filtered since they appear as top-level fields on the StructInfo.
  - Files: `template.go` (`filterUnexportedFields` + call in `Run()`),
    `generator_test.go` (4 new test cases), `integration_test.go` (new
    `cross-package-unexported-fields` subtest with compile verification)

### Priority 2 — Easy Wins

- [x] **M1: Add `rune` alias** *(2026-03-13)* — Add `"rune"` to the `case "int32":` branch in
  `mapToArrowType`, mirroring the existing `"byte"` → `"uint8"` pattern.
  - Files: `generator.go` (`mapToArrowType`), `generator_test.go` (`TestMapToArrowType`
    — add `{"rune", "rune", "arrow.PrimitiveTypes.Int32", "*array.Int32Builder", false}`)

### Priority 3 — Structural Enhancements

- [x] **S1: Flatten embedded struct fields** *(2026-03-14)* — Embedded struct
  fields (`type T struct { Base; ... }`) are now flattened: promoted fields
  appear as top-level Arrow columns rather than a nested struct. A two-pass
  approach in the parse loop handles shadowing (explicit field wins) and
  cross-embedding ambiguity (field promoted by multiple embeddings is skipped).
  Pointer-embedded structs (`*Base`) are skipped with a warning (future work).
  Embedded non-struct types are skipped. Depth-1 flattening only (nested
  embeddings within the embedded struct are not recursed).
  - Files: `generator.go` (`resolveEmbeddedFields` helper, parse loop restructured),
    `generator_test.go` (6 new test cases), `integration_test.go` (new `embedded-struct`
    subtest with Parquet/DuckDB round-trip)

- [x] **S2: Recursive `FieldInfo` for arbitrary nesting depth** *(2026-03-14)* —
  Replaced 11 flat `Val*`/`Key*` fields with two recursive pointers:
  `EltInfo *FieldInfo` (element info for lists, fixed-size-lists, and map values)
  and `KeyInfo *FieldInfo` (key info for maps). The template's three near-identical
  element dispatch blocks were collapsed into a single recursive `appendValue`
  sub-template. Intermediate builder variables are stored as `array.Builder`
  (interface) to allow type assertions at each recursive dispatch point.
  This naturally resolves B4 and B5 without additional special-case code.
  - Files: `generator.go` (`FieldInfo` restructure, `mapToFieldInfo` simplified),
    `template.go` (recursive `appendValue` sub-template),
    `generator_test.go` (updated assertions, new test cases for B4/B5/list-of-maps),
    `integration_test.go` (new subtests: triple-nested-slices, map-with-slice-value,
    nested-maps)

- [ ] **S3: Qualify struct names in `processed` map to avoid cross-package collisions** —
  The `processed` map and `queue` use bare struct names (e.g. `"Inner"`). If two
  input packages both define a struct named `Inner`, the second is silently skipped
  and the generated `AppendInnerStruct` may reference the wrong type. Fix: key
  `processed` by `pkgPath + "." + structName` instead of bare name.
  - Files: `generator.go` (`Parse()` — update `processed` map key and `queue`
    deduplication), `generator_test.go` (new test case with same-named structs
    from different packages)

- [ ] **S4: Support named slice/map types (`type Tags []string`)** —
  Named types whose underlying type is `*types.Slice` or `*types.Map` fall through
  `resolveIdent` (which only checks `*types.Struct` and `*types.Basic`) and hit
  `mapToArrowType`, which fails. The field is skipped with a warning. Fix: add
  `*types.Slice` and `*types.Map` branches to `resolveIdent` that unwrap the
  underlying type and recurse into element/key/value types.
  - Files: `generator.go` (`resolveIdent` — add slice/map underlying type handling),
    `generator_test.go` (new test cases for named slices and maps),
    `integration_test.go` (optional — round-trip verification)

### Priority 4 — Debatable / Future

- [ ] **D1: `complex64` / `complex128` support** — If there is demand, decompose
  into a two-field Arrow struct `{real: Float32/Float64, imag: Float32/Float64}`.
  Low priority unless a concrete use case arises.
  - Files: `generator.go`, `template.go`, `generator_test.go`

- [x] **D2: `time.Duration` as Int64 nanoseconds** *(2026-03-14)* —
  `time.Duration` is now intercepted in the `SelectorExpr` path (and the
  `*StarExpr` → `SelectorExpr` path for pointers) via a `resolveWellKnownType`
  helper, before the marshal method fallback. Mapped to `arrow.PrimitiveTypes.Int64`
  with `int64(d)` cast — lossless, sortable, Parquet-compatible. Previously stored
  as a string via `String()` (`"1h30m0s"`).

  **Options investigated:**
  - (a) Arrow `DurationType` — not viable (Parquet has no native Duration logical
    type; `pqarrow` returns `ErrNotImplemented`).
  - (b) **Int64 nanoseconds — selected.** Lossless, Parquet-compatible.
  - (c) `String` (previous behaviour) — human-readable but not sortable/queryable.
  - (d) `ARROW:schema` footer metadata — investigated, not viable (error occurs
    at schema conversion before any data can be written).
  - Files: `generator.go` (`resolveWellKnownType` helper, `SelectorExpr` and
    `*StarExpr` cases), `template.go` (no change — primitive path works),
    `generator_test.go` (2 new cases), `integration_test.go` (new subtest)

- [x] **D3: `time.Time` as Arrow Timestamp (nanosecond, UTC)** *(2026-03-14)* —
  `time.Time` is now intercepted in the `SelectorExpr` path via the same
  `resolveWellKnownType` helper. Mapped to `arrow.FixedWidthTypes.Timestamp_ns`
  with `arrow.Timestamp(t.UnixNano())` conversion. Previously stored as a string
  via `MarshalText()` (RFC 3339). A new `ConvertMethod` field on `FieldInfo`
  enables method-call-based value conversion in the template (e.g., `.UnixNano()`)
  without conflating with the `MarshalMethod` path. Pointer `*time.Time` fields
  get nil → `AppendNull` handling. DuckDB reads these as `TIMESTAMP WITH TIME ZONE`
  at microsecond precision; nanosecond precision is preserved in Arrow/Parquet.
  - Files: `generator.go` (`FieldInfo.ConvertMethod`, `resolveWellKnownType`),
    `template.go` (new `ConvertMethod` branch in `appendValue`),
    `generator_test.go` (2 new cases), `integration_test.go` (new subtest)

- [x] **D4: `durationpb.Duration` as Int64 nanoseconds** *(2026-03-14)* —
  `durationpb.Duration` (from `google.golang.org/protobuf/types/known/durationpb`)
  is a protobuf well-known type wrapping a duration as `Seconds int64` + `Nanos int32`.
  It currently resolves via the `SelectorExpr` path → `detectMarshalMethod` →
  `String()` (proto debug format), producing opaque strings like
  `"seconds:7200"` that are neither human-readable nor machine-parseable.

  **Approach:** Extend `resolveWellKnownType` with an entry for package path
  `"google.golang.org/protobuf/types/known/durationpb"`, type name `"Duration"`.
  Map to `arrow.PrimitiveTypes.Int64` with `ConvertMethod: "AsDuration"` and
  `CastType: "int64"`. The generated code becomes `int64(row.Field.AsDuration())`
  — this calls `durationpb.Duration.AsDuration() time.Duration`, and since
  `time.Duration` is `int64` (nanoseconds), the cast is lossless. Mirrors D2
  exactly, just with an extra conversion step through the protobuf accessor.

  Both value (`durationpb.Duration`) and pointer (`*durationpb.Duration`) fields
  are supported — pointer fields get nil → `AppendNull` handling. The template's
  `ConvertMethod` path auto-dereferences via Go method call semantics.

  **Note:** `AsDuration()` performs saturation arithmetic for out-of-range values
  (durations exceeding `±math.MaxInt64` nanoseconds clamp to `math.MinInt64` or
  `math.MaxInt64`). This matches `time.Duration`'s own range limitations.

  - Files: `generator.go` (`resolveWellKnownType` — add entry),
    `generator_test.go` (2 new cases: value + pointer),
    `integration_test.go` (new subtest with Parquet/DuckDB round-trip)

- [x] **D5: `timestamppb.Timestamp` as Arrow Timestamp (nanosecond, UTC)** *(2026-03-14)* —
  `timestamppb.Timestamp` (from `google.golang.org/protobuf/types/known/timestamppb`)
  is a protobuf well-known type wrapping a point in time as `Seconds int64` +
  `Nanos int32`. It currently resolves via the `SelectorExpr` path →
  `detectMarshalMethod` → `String()` (proto debug format), producing opaque
  strings like `"seconds:1710417600"`.

  **Approach:** Extend `resolveWellKnownType` with an entry for package path
  `"google.golang.org/protobuf/types/known/timestamppb"`, type name `"Timestamp"`.
  Map to `arrow.FixedWidthTypes.Timestamp_ns` with `ConvertMethod: "AsTime().UnixNano"`
  and `CastType: "arrow.Timestamp"`. The generated code becomes
  `arrow.Timestamp(row.Field.AsTime().UnixNano())` — this chains
  `timestamppb.Timestamp.AsTime() time.Time` with `time.Time.UnixNano() int64`.
  The `ConvertMethod` template already supports chained calls since it interpolates
  the string directly: `{{$var}}.{{$info.ConvertMethod}}()`. Mirrors D3 exactly,
  just with an extra conversion step through the protobuf accessor.

  Both value (`timestamppb.Timestamp`) and pointer (`*timestamppb.Timestamp`)
  fields are supported. The same DuckDB microsecond-precision caveat from D3
  applies — nanosecond precision is preserved in Arrow/Parquet but DuckDB reads
  at microsecond granularity.

  **Note:** `AsTime()` returns `time.Unix(seconds, nanos).UTC()`, so the resulting
  `time.Time` is always UTC — consistent with the `Timestamp_ns` timezone annotation.

  - Files: `generator.go` (`resolveWellKnownType` — add entry),
    `generator_test.go` (2 new cases: value + pointer),
    `integration_test.go` (new subtest with Parquet/DuckDB round-trip)

---

## 3. Testing Strategy

Each fix above should include:

1. **Unit test in `generator_test.go`** — verifies `Parse()` and/or `Run()` produce
   correct `StructInfo`/`FieldInfo` (or the expected skip/warning).
2. **Compilation check** — for bug fixes (B1–B5), verify the generated `.go` file
   compiles by writing it to a temp dir and running `go build`.
3. **Integration test in `integration_test.go`** (for S1 and any B-series items that
   implement support rather than skipping) — full Parquet round-trip with DuckDB
   verification.

---

## 4. Change Log

Record completed items here with date (check git blame for the git commit).

| Date       | Item |  Notes                                                  |
|------------|------|---------------------------------------------------------|
| 2026-03-13 | M1   | Added `rune` to `case "int32"` in `mapToArrowType`      |
| 2026-03-13 | B1   | Fixed-size arrays mapped to `FixedSizeListOfNonNullable` |
| 2026-03-13 | B2   | Blank-identifier fields (`_`) filtered out during parse  |
| 2026-03-13 | B3   | Nested slices (`[][]T`) supported via recursive list append |
| 2026-03-14 | S2   | Recursive `FieldInfo` with `EltInfo`/`KeyInfo`; recursive `appendValue` template |
| 2026-03-14 | B4   | `map[K][]V` resolved by S2                              |
| 2026-03-14 | B5   | `map[K]map[K2]V` resolved by S2                         |
| 2026-03-14 | S1   | Embedded struct fields flattened into parent Arrow schema |
| 2026-03-14 | D2   | `time.Duration` → Int64 nanoseconds via `resolveWellKnownType` |
| 2026-03-14 | D3   | `time.Time` → `Timestamp_ns` (UTC) via `resolveWellKnownType` + `ConvertMethod` |
| 2026-03-14 | D4   | `durationpb.Duration` → Int64 nanoseconds via `AsDuration()` |
| 2026-03-14 | D5   | `timestamppb.Timestamp` → `Timestamp_ns` (UTC) via `AsTime().UnixNano` |
| 2026-03-14 | B6   | Unexported fields filtered in cross-package generation via `filterUnexportedFields` |
