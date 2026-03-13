# Arrow Writer Generator — Recursive Nesting Architecture

This document describes the recursive `FieldInfo` and template architecture
introduced in S2 (2026-03-14) to support arbitrary nesting depth in generated
Arrow append writers.

---

## Problem

The original `FieldInfo` struct used flat fields (`ValArrowBuilder`,
`ValCastType`, `KeyArrowBuilder`, `KeyCastType`, `ValIsList`,
`ValValArrowBuilder`, `ValValCastType`, etc.) to describe one level of element
nesting. This meant:

- Nested slices were limited to depth 2 (`[][]T`).
- `map[K][]V` and `map[K]map[K2]V` each needed their own ad-hoc fields.
- The template duplicated near-identical element dispatch logic across the
  `IsList`, `IsFixedSizeList`, and `IsMap` branches.

## Solution

### Recursive `FieldInfo`

The 11 flat `Val*`/`Key*` fields were replaced with two recursive pointer
fields:

```go
type FieldInfo struct {
    // ... existing fields (Name, ArrowType, ArrowBuilder, etc.) ...
    EltInfo *FieldInfo // element info for lists, fixed-size-lists, and map values
    KeyInfo *FieldInfo // key info for maps
}
```

A `FieldInfo` for `map[string][]int32` now looks like:

```
FieldInfo{IsMap, KeyInfo: &{string}, EltInfo: &{IsList, EltInfo: &{int32}}}
```

There is no depth limit — the parser recurses naturally through the Go AST.

### Recursive `appendValue` Template

The template uses a single recursive sub-template `appendValue` that dispatches
on the `FieldInfo` type flags:

```
appendFields
  └── for each field i:
        appendValue(Info=field, Var="row.Field", BldrExpr="w.b.Field(i)", Depth=0)

appendValue dispatches:
  IsStruct        → AppendXxxStruct(bldr.(*StructBuilder), var)
  IsList          → bldr.(*ListBuilder).Append(true); recurse on EltInfo
  IsFixedSizeList → bldr.(*FixedSizeListBuilder).Append(true); recurse on EltInfo
  IsMap           → bldr.(*MapBuilder).Append(true); recurse on KeyInfo + EltInfo
  MarshalMethod   → bldr.(Builder).Append(var.MarshalXxx())
  primitive       → bldr.(Builder).Append(cast(var))
```

Each recursive call increments a `Depth` counter that generates unique variable
names: `v0`, `v1`, `v2`, etc. with corresponding builder variables `v0Bldr`,
`v1Bldr`, `v2Bldr`. For maps, key/value builders use `v0KeyBldr`/`v0ValBldr`
and range variables `v0K`/`v0V`.

### Builder Variable Typing

Intermediate builder variables are stored as `array.Builder` (the Arrow
interface type) rather than asserting to a concrete type on assignment. This
is critical for recursion: when a leaf `appendValue` call receives a builder
expression, it must be able to perform a type assertion (`bldr.(*Int32Builder)`)
on it. If the variable were already a concrete type (e.g. `*ListBuilder`),
Go would reject the assertion since concrete types are not interfaces.

The pattern is:

```go
// Builder assigned as interface (array.Builder):
v0Bldr := w.b.Field(1).(*array.ListBuilder).ValueBuilder()

// Each dispatch branch asserts to the concrete type it needs:
v0Bldr.(*array.ListBuilder).Append(true)      // container recursion
v0Bldr.(*array.Int32Builder).Append(int32(v0)) // leaf append
```

The top-level builder expressions (`w.b.Field(i)`) naturally return
`array.Builder`, so the first level of dispatch always works. For deeper
levels, `ValueBuilder()`, `KeyBuilder()`, and `ItemBuilder()` all return
`array.Builder`, maintaining the invariant.

## Generated Code Example

For `[][][]int32` at depth 3:

```go
if row.Data == nil {
    w.b.Field(1).(*array.ListBuilder).AppendNull()
} else {
    w.b.Field(1).(*array.ListBuilder).Append(true)
    v0Bldr := w.b.Field(1).(*array.ListBuilder).ValueBuilder()
    for _, v0 := range row.Data {
        if v0 == nil {
            v0Bldr.(*array.ListBuilder).AppendNull()
        } else {
            v0Bldr.(*array.ListBuilder).Append(true)
            v1Bldr := v0Bldr.(*array.ListBuilder).ValueBuilder()
            for _, v1 := range v0 {
                if v1 == nil {
                    v1Bldr.(*array.ListBuilder).AppendNull()
                } else {
                    v1Bldr.(*array.ListBuilder).Append(true)
                    v2Bldr := v1Bldr.(*array.ListBuilder).ValueBuilder()
                    for _, v2 := range v1 {
                        v2Bldr.(*array.Int32Builder).Append(int32(v2))
                    }
                }
            }
        }
    }
}
```

## Test Coverage

- **Unit tests** (`generator_test.go`): `TestGenerator_Parse` verifies recursive
  `EltInfo`/`KeyInfo` structure. `TestGenerator_RunOutput` includes cases for
  `[][]T`, `[][][]T`, `map[K][]V`, `map[K]map[K2]V`, and `[]map[K]V`.
- **Integration tests** (`integration_test.go`): Subtests `nested-slices`,
  `triple-nested-slices`, `map-with-slice-value`, and `nested-maps` verify
  that generated code compiles, produces valid Arrow records, and handles nil
  values at every nesting level.
