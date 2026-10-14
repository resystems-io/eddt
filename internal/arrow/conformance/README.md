# conformance

A round-trip suite for `arrow-writer-gen` and `arrow-reader-gen`. It answers
with test results, rather than documentation claims, the question of what
the generated code does for every field shape a record can carry.

Run everything with `go test ./internal/arrow/conformance/...`. The short
mode (`-short`) skips `generator_test.go`, which runs the generators and the
compiler on the fixture packages under `testdata`.

## Layout

Each fixture file holds the structs for one theme and the two `go:generate`
directives that produce its writer and reader. The generated code is checked
in next to it, and the matching `_test.go` file encodes every fixture through
the generated writer, carries the batch through an Arrow IPC stream and a
Parquet file, decodes it through the generated reader, and compares field by
field.

| File | What is in it | What we check |
|---|---|---|
| `baseline.go` | A record shaped like a production one, and one with every primitive type | That the harness itself is sound, and that every number, string and bool comes back exact at the edges of its range |
| `nesting.go` | Structs inside structs, eight levels by pointer, five by value, and ten levels with lists, maps and arrays in between | That no level goes missing and a null at any level stays a null |
| `containers.go` | Slices, maps, fixed-size arrays, named types over them (`type Tags []string`), and pointers to slices and maps | That elements, keys and values come back exact, that nil and empty stay apart, and that maps of maps and lists of lists work |
| `pointers.go` | One record with every nullable field shape | That a nil comes back nil and a pointer to a zero value comes back as exactly that, also into a struct reused across rows |
| `embedding.go` | Structs embedded by value, by pointer, two levels deep, with a shadowed field, from another package, and with an ambiguous field | Which promoted fields become columns and which are lost, with a warning |
| `generics.go` | `Pair[K, V]`, `List[T]` and `Optional[T]` instantiated in every wrapper | That each instantiation gets its own schema and round-trips |
| `timerecord.go` | `time.Duration`, `time.Time`, `durationpb.Duration`, `timestamppb.Timestamp` | Microsecond precision, the zero time, UTC on the way back, the edges of the column and the whole protobuf range |
| `fallback.go` | Types from packages the generator does not look inside, so it falls back to their text or binary encoding (`MarshalText`, `String`, `MarshalBinary`), plus malformed payloads | Which rung of the fallback is used, what survives the read, what the writer reports when a marshal fails, and what `Errors()` reports on bad input |
| `skipped.go` | A record full of the field kinds the generator drops: functions, channels, interfaces, `any`, anonymous structs | That the ordinary fields around them still work and the schema holds exactly those |
| `external/` | The types that `fallback.go` uses, kept in their own package on purpose | Nothing by itself |
| `schema_test.go` | Records built by hand with columns missing, extra, reordered, mistyped or dictionary-encoded | How the reader treats a record the writer never produced |
| `generator_test.go` and `testdata/` | Small packages with the shapes the generators refuse or skip: empty structs, zero-length arrays, recursive structs, unusual map keys, aliases, generics in every wrapper, write-only types | The error or warning each one gets, and that what is generated compiles and runs |

## What the suite establishes

Every shape in the fixture files round-trips exactly through both carriers,
with nil kept apart from empty for every container kind. Timestamps live in
a microsecond column that holds the zero `time.Time`, every year a calendar
can name and the whole protobuf range, and an instant past its edges is
written as null. A field whose column is missing reads back as its zero
value. A failed unmarshal zeroes the element it was for, names it with its
index or key, and leaves a slice taken from `Errors()` intact across a reset.
A failed marshal writes null and is reported by the writer. A type whose
only method is `String` is write-only, so the column holds the name and the
reader skips the field. Embedding is flattened one level, with a warning for
the second. A struct the generators cannot map at all, because it has no
fields, refers to itself, or keeps only unreadable fields, stops generation
with an error that names it.
