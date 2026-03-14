# Multi-Package Support for Arrow Writer Generator

## Overview

`arrow-writer-gen` can accept multiple input package directories, enabling it to
introspect Go structs that span package boundaries and generate native Apache
Arrow struct builders for all of them in a single output file.

When a field references a struct from another **explicitly provided** package,
the generator resolves it natively — producing `StructBuilder` / `AppendXxxStruct`
helpers — just as it does for structs in the same package.

For any external type that is **not** in the provided packages (e.g. `time.Time`,
`netip.Addr`, `sync.Mutex`), the existing marshal-based fallback is retained:

| Priority | Interface                  | Arrow Column Type |
|----------|----------------------------|-------------------|
| 1        | `encoding.TextMarshaler`   | String            |
| 2        | `fmt.Stringer`             | String            |
| 3        | `encoding.BinaryMarshaler` | Binary            |

If none of these interfaces are implemented, the field is silently skipped with
a warning during generation.

## CLI Flags

### `--pkg` / `-p` (repeatable, default `["."]`)

One or more input package directories. Each directory is loaded independently
(they may belong to separate Go modules).

```
--pkg ./internal/model
--pkg ./internal/model --pkg ./internal/types
--pkg ./internal/model,./internal/types
```

### `--pkg-alias` / `-a` (repeatable, optional)

Aliases for imported packages in `importpath=alias` format, where `importpath`
must be the **full Go import path** of the package (as resolved by
`packages.Load` from the input directory — i.e. the module path declared in
`go.mod`). If an alias is provided, the generated import statement uses the
alias and all type references are qualified accordingly.

```
--pkg-alias myapp/internal/types=modeltypes
--pkg-alias go.onelayer.dev/v/ericsson/ebm/mme=ebmmme
```

### `--structs` / `-s` (required)

Comma-separated list of top-level struct names to generate writers for. The
generator will automatically discover and generate helpers for any nested
structs reachable from the listed structs, across all provided packages.

### `--pkg-name` / `-n` (optional)

Override the output `package` declaration. When this differs from the input
package name(s), the generator will emit import statements and qualify struct
type references.

### `--out` / `-o` (default `arrow-writer-gen.go`)

Output file path for the generated Go source.

## Example CLI Invocations

### Single package (backward-compatible)

```bash
arrow-writer-gen \
  --pkg ./internal/model \
  --structs User,Order \
  --out internal/model/arrow_writer.go
```

### Two packages — Outer references Inner from a sibling package

```bash
arrow-writer-gen \
  --pkg ./internal/entities \
  --pkg ./internal/types \
  --structs Device \
  --out internal/entities/arrow_writer.go
```

If `Device` (in `entities`) has a field `Position types.Location`, the generator
loads both packages, resolves `Location` natively as an Arrow struct, and emits:

- `NewDeviceSchema()` with a nested struct field for `Position`
- `AppendLocationStruct()` helper
- An `import "myapp/internal/types"` in the generated file

### Alias to avoid name collisions

If two input packages share the same base name or a package name collides with
the generated output package:

```bash
arrow-writer-gen \
  --pkg ./internal/entities \
  --pkg ./vendor/thirdparty/entities \
  --pkg-alias thirdparty.example.com/entities=tpentities \
  --structs Device \
  --pkg-name writers \
  --out internal/writers/arrow_writer.go
```

The generated file will contain:

```go
package writers

import (
    "github.com/apache/arrow/go/v18/arrow"
    "github.com/apache/arrow/go/v18/arrow/array"
    "github.com/apache/arrow/go/v18/arrow/memory"
    "myapp/internal/entities"
    tpentities "thirdparty.example.com/entities"
)
```

### Mixed: native cross-package structs + external marshal fallback

```bash
arrow-writer-gen \
  --pkg ./internal/model \
  --pkg ./internal/geo \
  --structs Device \
  --out internal/model/arrow_writer.go
```

If `Device` contains:

```go
type Device struct {
    ID       int32
    Position geo.Location    // from ./internal/geo — resolved natively
    Addr     *netip.Addr     // from net/netip — NOT in provided packages
}
```

- `geo.Location` is resolved as a native Arrow struct (StructBuilder).
- `*netip.Addr` falls back to `MarshalText()` → Arrow String column.

## Resolution Decision Tree

```
Is the field's type a struct in one of the --pkg directories?
  YES → Native Arrow struct (StructBuilder, AppendXxxStruct helper)
  NO  → Does it implement TextMarshaler?
          YES → Arrow String via MarshalText()
          NO  → Does it implement Stringer?
                  YES → Arrow String via String()
                  NO  → Does it implement BinaryMarshaler?
                          YES → Arrow Binary via MarshalBinary()
                          NO  → Field skipped (warning emitted)
```
