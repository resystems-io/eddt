# Arrow Writer Generator — Refinement Plan

This document captures the results of a type-support audit conducted on `arrow-writer-gen`
and provides a prioritised checklist of recommended changes. Items are intended to be
addressed one at a time in future sessions; tick the checkbox and note the date/commit
when each item is completed.

---

## 1. Audit Summary

### 1.1 Supported Types (as of 2026-03-14)

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
| Named slice (`type Tags []string`) | Same as underlying slice | Unwrapped via `fieldInfoFromType` |
| Named bytes (`type MyBytes []byte`) | `Binary` | `[]byte` special case preserved |
| Named map (`type Config map[K]V`) | Same as underlying map | Unwrapped via `fieldInfoFromType` |
| Named array (`type MAC [6]byte`) | Same as underlying array | Unwrapped via `fieldInfoFromType` |

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
| S2 | Arbitrary nesting depth (`[][][]T`, `[][]map[K]V`, etc.) | ~~`FieldInfo` uses flat `Val*` fields limiting nesting to depth 2.~~ **Fixed (2026-03-14).** Recursive `FieldInfo` with `EltInfo`/`KeyInfo` removes the depth limit. Also resolves B4 and B5. |
| S3 | Struct name collision across packages | The `processed` map and `queue` use bare struct names (e.g. `"Inner"`). If pkg1 and pkg2 both define `Inner`, the second is silently skipped. The generated code would reference only one `AppendInnerStruct`, which may correspond to the wrong package's struct. Fix: key `processed` by qualified name (`pkgPath + "." + structName`). |
| S4 | Named slice/map types (`type Tags []string`, `type MyBytes []byte`) | ~~Named types whose underlying type is a slice or map fall through `resolveIdent` and hit `mapToArrowType`, which fails.~~ **Fixed (2026-03-14).** Added `fieldInfoFromType` helper and `*types.Slice`/`*types.Map`/`*types.Array` branches to `resolveIdent`. Supports named bytes, named slices of structs, named maps, named fixed-size arrays, and arbitrary nesting via S2's recursive architecture. |
| S5 | `--pkg` accepts only local directories | `loadPackages()` forces `cfg.Dir`-based loading with `packages.Load(cfg, ".")`, rejecting Go import paths like `github.com/user/repo/pkg`. Other Go tools (mockery, go-sumtype, gqlgen) pass patterns directly to `packages.Load`, which natively handles both filesystem paths and import paths. See section 1.5 for full analysis. |

### 1.5 Package Input — Go Import Paths vs Local Directories

Currently `--pkg` only accepts local filesystem directory paths. The `loadPackages()`
method iterates over `InputPkgs`, sets `cfg.Dir = dir` for each, and calls
`packages.Load(cfg, ".")`. This means every input must be a directory on disk
containing `.go` files and a `go.mod`.

Most modern Go tools (mockery, go-sumtype, gqlgen) accept **Go import paths**
(e.g., `github.com/user/repo/pkg`) in addition to filesystem paths. This is more
ergonomic and consistent with how developers reference packages in Go code.

#### How `packages.Load` Pattern Resolution Works

`packages.Load` delegates to `go list` and natively distinguishes two pattern types:

| Pattern type | Examples | How identified |
|---|---|---|
| Filesystem paths | `"."`, `"./internal/foo"`, `"/absolute/path"` | Starts with `.`, `..`, or `/` |
| Import paths | `"fmt"`, `"github.com/user/repo/pkg"` | Everything else — resolved via module system |

The resolution chain for import paths: `packages.Load` → `go list` → module system
→ `go.mod` build list → module cache (`$GOMODCACHE`) → `GOPROXY` download.

**Critical constraint**: Since Go 1.16, the default is `-mod=readonly`. If the import
path is not already in the build list (`go.mod`), `go list` fails:
```
no required module provides package X; to add it: go get X
```

This constraint is standard Go tooling behaviour and is the expected workflow —
the user adds the dependency to their module first with `go get`, then references
it in tools.

#### Options Investigated

**Option A: Direct pass-through to `packages.Load` (recommended).**
Pass `--pkg` values directly as patterns to `packages.Load` instead of forcing
`cfg.Dir`-based directory loading. `packages.Load` natively handles both filesystem
paths and import paths. Requires the import path to be in the caller's `go.mod`
build list (standard Go convention). Minimal code change; consistent with mockery,
go-sumtype, gqlgen.

**Option B: Auto-download via `go get` before load.**
Detect import-path inputs, shell out to `go get <package>`, then load. Fully
automatic but modifies the user's `go.mod` as a side effect — surprising behaviour
for a code generator. Requires network access. Rejected.

**Option C: Temporary module scaffold.**
Create a temp module that `require`s the target, run `go mod download`, load from
there. No side effects on user's module but complex, slow, and loses the user's
`replace` directives. Rejected.

#### How Other Go Tools Handle This

| Tool | Import paths? | Approach |
|---|---|---|
| mockery | Yes | Passes import paths directly to `packages.Load` |
| go-sumtype | Yes | Passes `os.Args[1:]` directly to `packages.Load` |
| gqlgen | Yes | Passes import paths; light load first, full on demand |
| stringer | No | Directory-only, sets `cfg.Dir`, loads `"."` |
| mockgen (uber) | Yes | Shells out to `go list -json` directly |

#### Path Classification Heuristic

The same rule used by `go help packages`:

> An import path that is a rooted path or that begins with a `.` or `..` element
> is interpreted as a file system path.

```go
func isFilesystemPath(s string) bool {
    return strings.HasPrefix(s, ".") || strings.HasPrefix(s, "/") || filepath.IsAbs(s)
}
```

Filesystem paths → set `cfg.Dir` and load `"."` (current behaviour, preserved
for backward compatibility and separate-module support). Import paths → load the
pattern directly, with `cfg.Dir` set to the invoking module's root.

#### Error Handling and User Guidance

When `packages.Load` fails for an import path because the package is not in
`go.mod`, the tool must provide a clear, actionable error message:

```
Error: failed to load package "github.com/user/repo/pkg":
  no required module provides this package.

  To add it to your module's dependencies, run:
    go get github.com/user/repo/pkg

  Then re-run the generator.
```

This is the standard Go tooling workflow. The tool does not download packages
automatically — this is intentional to avoid modifying the user's `go.mod`
without explicit consent. Documentation (CLI `--help`, README) must clearly state:

1. Import paths require the package to be in the caller's `go.mod` dependency graph.
2. Run `go get <package>` if the package is not yet a dependency.
3. Filesystem paths (starting with `.`, `..`, or `/`) continue to work as before.

#### LoadMode Consideration

The current `packages.Config.Mode` (`NeedName | NeedFiles | NeedSyntax | NeedTypes
| NeedTypesInfo`) is sufficient for same-module loading. For robust cross-module
type resolution (interface implementation checks on types from external packages),
adding `NeedImports | NeedDeps` ensures transitive dependency type information is
fully available. This should be evaluated during implementation.

### 1.6 Debatable / Low Priority

| ID | Go Type | Notes |
|----|---------|-------|
| D1 | `complex64` / `complex128` | No native Arrow complex type. Could decompose into a two-field struct (`real float32, imag float32`) but practical demand is low. |
| D2 | `time.Duration` | ~~Resolved via `String()` → stored as `"1h30m0s"`.~~ **Fixed (2026-03-14).** Now stored as Int64 nanoseconds. Arrow `DurationType` was investigated but Parquet has no native Duration logical type (`pqarrow` returns `ErrNotImplemented`). Int64 nanoseconds is lossless, sortable, and Parquet-compatible. |
| D3 | `time.Time` | ~~Resolved via `MarshalText()` → stored as RFC 3339 string.~~ **Fixed (2026-03-14).** Now stored as `Timestamp_ns` (UTC) via `arrow.Timestamp(t.UnixNano())`. |
| D4 | `durationpb.Duration` | ~~Resolved via proto `String()` → opaque `"seconds:7200"`.~~ **Fixed (2026-03-14).** Now stored as Int64 nanoseconds via `AsDuration()` conversion to `time.Duration`. `AsDuration()` performs saturation arithmetic for out-of-range values, matching `time.Duration`'s own range. |
| D5 | `timestamppb.Timestamp` | ~~Resolved via proto `String()` → opaque `"seconds:1710417600"`.~~ **Fixed (2026-03-14).** Now stored as `Timestamp_ns` (UTC) via `AsTime().UnixNano()`. DuckDB reads at microsecond precision; nanosecond precision is preserved in Arrow/Parquet. |

---

## 2. Refinement Checklist

Items are grouped by priority. Within each group, the suggested order reflects
dependency and effort. Completed items note the date; see the change log (section 4)
and git blame for commit details.

### Priority 1 — Fix Broken Code Generation

These must be fixed first because they produce output that does not compile.

- [x] **B1: Support fixed-size arrays** *(2026-03-13)* — Map `[N]T` to
  `arrow.FixedSizeListOfNonNullable(N, T)`. No nil check (arrays are value types).
  - Files: `generator.go`, `template.go`, `generator_test.go`

- [x] **B2: Skip blank-identifier fields** *(2026-03-13)* — Filter `_ T` fields
  in `Parse()` to prevent generating `row._`.
  - Files: `generator.go`, `generator_test.go`, `integration_test.go`

- [x] **B3: Support nested slices (`[][]T`)** *(2026-03-13, superseded by S2)* —
  Now handled by the recursive `FieldInfo`/`appendValue` architecture (S2).
  - Files: `generator.go`, `template.go`, `generator_test.go`, `integration_test.go`

- [x] **B4: Handle `map[K][]V`** *(2026-03-14)* — Resolved by S2.

- [x] **B5: Handle `map[K]map[K2]V`** *(2026-03-14)* — Resolved by S2.

- [x] **B6: Skip unexported fields in cross-package generation** *(2026-03-14)* —
  Filter unexported fields via `token.IsExported` on cross-package structs
  (non-empty `Qualifier`). Emits a warning per skipped field. Same-package
  generation is unaffected.
  - Files: `template.go`, `generator_test.go`, `integration_test.go`

### Priority 2 — Easy Wins

- [x] **M1: Add `rune` alias** *(2026-03-13)* — Add `"rune"` to the `case "int32"`
  branch in `mapToArrowType`.
  - Files: `generator.go`, `generator_test.go`

### Priority 3 — Structural Enhancements

- [x] **S1: Flatten embedded struct fields** *(2026-03-14)* — Promoted fields
  appear as top-level Arrow columns. Handles shadowing and cross-embedding
  ambiguity. Pointer-embedded structs skipped with warning (future work).
  - Files: `generator.go`, `generator_test.go`, `integration_test.go`

- [x] **S2: Recursive `FieldInfo` for arbitrary nesting depth** *(2026-03-14)* —
  Replaced flat `Val*`/`Key*` fields with recursive `EltInfo`/`KeyInfo` pointers
  and a single recursive `appendValue` sub-template. Also resolves B4 and B5.
  - Files: `generator.go`, `template.go`, `generator_test.go`, `integration_test.go`

- [ ] **S3: Qualify struct names in `processed` map** — Key by
  `pkgPath + "." + structName` instead of bare name to avoid cross-package
  collisions. See section 1.4.
  - Files: `generator.go`, `generator_test.go`

- [x] **S4: Support named slice/map types** *(2026-03-14)* — Added
  `fieldInfoFromType` helper that resolves `types.Type` → `FieldInfo` recursively
  (parallels the AST-based `mapToFieldInfo`). Added `*types.Slice`, `*types.Map`,
  and `*types.Array` branches to `resolveIdent` via the underlying type. Named
  type's name is preserved as `GoType`. Handles named bytes (`type MyBytes []byte`
  → Binary), named slices of structs, named maps, named fixed-size arrays, and
  arbitrary nesting depth via S2's recursive `EltInfo`/`KeyInfo` architecture.
  - Files: `generator.go`, `generator_test.go`, `integration_test.go`

- [x] **S5: Accept Go import paths in `--pkg`** *(2026-03-14)* — Classify inputs
  using `go help packages` convention: filesystem paths (`.`, `..`, `/` prefix) use
  `cfg.Dir`-based loading (unchanged); everything else is treated as a Go import
  path and passed directly to `packages.Load`. Import paths batched into a single
  call for efficiency. Actionable `go get` guidance in error messages for missing
  modules. No LoadMode change needed (`NeedTypes` already triggers full type-checking).
  - Files: `generator.go`, `cmd/arrow-writer-gen/main.go`, `generator_test.go`,
    `integration_test.go`, `cmd/arrow-writer-gen/main_test.go`

### Priority 4 — Debatable / Future

- [ ] **D1: `complex64` / `complex128` support** — Low priority unless a concrete
  use case arises. See section 1.6.
  - Files: `generator.go`, `template.go`, `generator_test.go`

- [x] **D2: `time.Duration` as Int64 nanoseconds** *(2026-03-14)* — Intercepted
  via `resolveWellKnownType` before marshal fallback. Mapped to Int64 with
  `int64(d)` cast. See section 1.6 for options investigated.
  - Files: `generator.go`, `generator_test.go`, `integration_test.go`

- [x] **D3: `time.Time` as Arrow Timestamp** *(2026-03-14)* — Mapped to
  `Timestamp_ns` (UTC) via `arrow.Timestamp(t.UnixNano())`. Added `ConvertMethod`
  field to `FieldInfo` for method-call-based value conversion in the template.
  - Files: `generator.go`, `template.go`, `generator_test.go`, `integration_test.go`

- [x] **D4: `durationpb.Duration` as Int64 nanoseconds** *(2026-03-14)* — Extended
  `resolveWellKnownType` for the protobuf package. Uses `AsDuration()` conversion
  to `time.Duration`, then `int64()` cast. Supports both value and pointer fields.
  - Files: `generator.go`, `generator_test.go`, `integration_test.go`

- [x] **D5: `timestamppb.Timestamp` as Arrow Timestamp** *(2026-03-14)* — Extended
  `resolveWellKnownType` for the protobuf package. Uses chained
  `AsTime().UnixNano()` conversion. Supports both value and pointer fields.
  - Files: `generator.go`, `generator_test.go`, `integration_test.go`

---

## 3. Testing Strategy

Each fix above should include:

1. **Unit test in `generator_test.go`** — verifies `Parse()` and/or `Run()` produce
   correct `StructInfo`/`FieldInfo` (or the expected skip/warning).
2. **Compilation check** — for bug fixes (B1–B6), verify the generated `.go` file
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
| 2026-03-14 | S5   | `--pkg` accepts Go import paths in addition to filesystem paths |
| 2026-03-14 | S4   | Named slice/map/array types supported via `fieldInfoFromType` + `resolveIdent` branches |
