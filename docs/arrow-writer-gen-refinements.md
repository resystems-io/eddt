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
| External type (`MarshalText`) | `String` | e.g. `netip.Addr`, `time.Time` |
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

### 1.3 Missing Primitive — Easy Win

| ID | Go Type | Issue |
|----|---------|-------|
| M1 | `rune` | `rune` is a built-in alias for `int32`, identical to how `byte` aliases `uint8`. Currently rejected as "unsupported primitive type: rune". Fix is a one-line addition to the `mapToArrowType` switch. |

### 1.4 Structural Gaps

| ID | Go Pattern | Issue |
|----|-----------|-------|
| S1 | Embedded structs (`type T struct { Base; Name string }`) | Embedded fields have `len(field.Names) == 0` and are silently skipped. The embedded struct's fields (e.g. `Base.ID`) are lost from the Arrow schema. This is a common Go idiom; users will expect the promoted fields to appear in the output. |

### 1.5 Debatable / Low Priority

| ID | Go Type | Notes |
|----|---------|-------|
| D1 | `complex64` / `complex128` | No native Arrow complex type. Could decompose into a two-field struct (`real float32, imag float32`) but practical demand is low. |
| D2 | `time.Duration` stored as string | Currently `time.Duration` (named `int64` from the `time` package) resolves via the `SelectorExpr` path and hits `String()` → stored as `"1h30m0s"`. An alternative would be to store the underlying `int64` (nanoseconds), which is lossless and sortable. This is a domain-specific tradeoff. |

---

## 2. Refinement Checklist

Items are grouped by priority. Within each group, the suggested order reflects
dependency and effort.

### Priority 1 — Fix Broken Code Generation

These must be fixed first because they produce output that does not compile.

- [ ] **B2: Skip blank-identifier fields** — In `Parse()`, filter out fields where
  `field.Names[0].Name == "_"`. This is the smallest, safest fix and prevents a
  compile error in the generated output.
  - Files: `generator.go` (parse loop), `generator_test.go` (new test case)

- [ ] **B1: Handle fixed-size arrays** — In the `*ast.ArrayType` case of
  `mapToFieldInfo`, check `t.Len != nil`. When present, skip the field with a
  warning (e.g. "fixed-size arrays are not yet supported"). This prevents the
  broken nil-check code from being emitted. A future enhancement could map
  `[N]T` to `arrow.FixedSizeListOf(T, N)` if there is demand.
  - Files: `generator.go` (`mapToFieldInfo` ArrayType case), `generator_test.go`

- [ ] **B3: Handle nested slices (`[][]T`)** — Two options:
  - **(a) Skip with warning** — detect when the slice element is itself a list
    (`eltInfo.IsList == true`) and return an error. Safest short-term fix.
  - **(b) Implement recursive list append** — the template would need to emit a
    nested loop with a second `ListBuilder.Append(true)` + element iteration.
    This requires a template redesign (recursive `appendFields` or a dedicated
    `appendListValue` sub-template).
  - Recommended: option (a) first, option (b) as a follow-up if users need it.
  - Files: `generator.go`, `template.go` (if option b), `generator_test.go`

- [ ] **B4: Handle `map[K][]V`** — Same two options as B3. When `valInfo.IsList`
  is true in the MapType case, either skip with warning or implement nested
  append. Depends on B3(b) if implementing.
  - Files: `generator.go`, `template.go` (if implementing), `generator_test.go`

- [ ] **B5: Handle `map[K]map[K2]V`** — Same pattern. When `valInfo.IsMap` is true,
  either skip with warning or implement nested map append. Depends on B3(b)
  approach decision.
  - Files: `generator.go`, `template.go` (if implementing), `generator_test.go`

### Priority 2 — Easy Wins

- [ ] **M1: Add `rune` alias** — Add `"rune"` to the `case "int32":` branch in
  `mapToArrowType`, mirroring the existing `"byte"` → `"uint8"` pattern.
  - Files: `generator.go` (`mapToArrowType`), `generator_test.go` (`TestMapToArrowType`
    — add `{"rune", "rune", "arrow.PrimitiveTypes.Int32", "*array.Int32Builder", false}`)

### Priority 3 — Structural Enhancements

- [ ] **S1: Flatten embedded struct fields** — When `len(field.Names) == 0` in the
  parse loop, resolve the embedded type. If it is a struct (local or cross-package),
  recursively include its fields in the parent struct's `FieldInfo` list (flattening).
  This mirrors Go's own field promotion semantics.
  - Considerations:
    - Embedded pointer-to-struct (`*Base`) should also be handled; the promoted
      fields become nullable.
    - Name collisions between promoted fields and explicitly declared fields
      should follow Go's shadowing rules (explicit field wins).
    - Embedded non-struct types (e.g. `type T struct { string }`) can be skipped.
  - Files: `generator.go` (parse loop + new helper), `generator_test.go`,
    `integration_test.go` (new sub-test)

### Priority 4 — Debatable / Future

- [ ] **D1: `complex64` / `complex128` support** — If there is demand, decompose
  into a two-field Arrow struct `{real: Float32/Float64, imag: Float32/Float64}`.
  Low priority unless a concrete use case arises.
  - Files: `generator.go`, `template.go`, `generator_test.go`

- [ ] **D2: `time.Duration` as int64** — Consider adding special-case detection for
  `time.Duration` to store as `Int64` (nanoseconds) rather than the current
  `String()` serialization. This is lossless and preserves sort order, but changes
  the column semantics for anyone already relying on the string representation.
  Could be gated behind a flag or annotation if both behaviours are desired.
  - Files: `generator.go` (`SelectorExpr` case), `generator_test.go`

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

Record completed items here with date and commit hash.

| Date | Item | Commit | Notes |
|------|------|--------|-------|
| | | | |
