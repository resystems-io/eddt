# Arrow Writer Generator

## Architecture Mandate

Taking guidance from the [Arrow-Go](https://github.com/apache/arrow-go) and in
particular from `schema.NewSchemaFromStruct`, the Arrow writer generator will
implement similar logic, but will generate an append writer for each struct
type.

The generated code will default to being placed in a file named
`arrow-writer-gen.go`. The tool takes in a explicit list of struct types to
generate append writers for, and will place the generated code in the designated
file.

## Implementation Phases

The implementation will be broken down into the following phases: simple
structs, lists and maps, and nested structs.

- For each phase, the generator will be extended to support the features of that
phase, together with unit tests verifying the generator's behaviour.
- While the conversion to Arrow will be one-way, and we do not convert back from
  Arrow to go yet, the unit tests must be able to verify the conversion.
- Note, while the generator will be extended to cover the requirements of each
phase, the CLI calling convention will remain the same, and the generated code
will be placed in the same file.
- The CLI will be placed under `cmd/arrow-writer-gen`.
- The code generator logical will be placed under `internal/arrow/writer-gen`.

### Phase 1: Simple Structs

The first phase of the implementation will focus on generating append writers
for simple structs. This will include the following features:

- Support for basic types (int, string, etc.)
- Support for nested structs
- Support for slices of basic types
- Support for maps of basic types

### Phase 2: Lists and Maps

The second phase of the implementation will focus on generating append writers
for lists and maps. This will include the following features:

- Support for lists of basic types
- Support for maps of basic types

### Phase 3: Nested Structs

The third phase of the implementation will focus on generating append writers
for nested structs. This will include the following features:

- Support for nested structs
- Support for slices of nested structs
- Support for maps of nested structs

### Phase 4: Benchmarks

The final phase of the implementation will focus on generating benchmarks for
the generated code. This will include the following features:

- Support for benchmarking the marshalling of the generated code
