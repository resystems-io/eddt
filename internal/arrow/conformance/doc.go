// Package conformance is a round-trip suite for arrow-writer-gen and
// arrow-reader-gen. Those generators turn a Go struct into code that
// appends the struct to an Apache Arrow record batch and reads it back, and
// everything that stores records through them depends on that code being
// right for every field shape it uses.
//
// The package holds fixture structs covering the shapes that could
// plausibly go wrong (deep nesting, maps of maps, nil against zero values,
// embedding, generics, the time and protobuf mappings, the external-type
// fallback chain), the code the generators emit for them, and tests that
// encode each fixture, carry it through an Arrow IPC stream and a Parquet
// file, decode it, and compare field by field. Shapes the generators refuse,
// or skip with a warning, are exercised by generator_test.go, which runs
// the generators at test time on the small packages under testdata and
// checks their warnings and their output. Nothing here is imported by
// production code.
//
// Each fixture file carries its own pair of go:generate directives, one for
// the writer and one for the reader, so a generator warning is attributable
// to the fixtures in that file and a shape that breaks generation takes
// down only its own group. Struct names are unique across the files on
// purpose: the generators scan companion files in the output directory and
// elide any helper already declared there, which would couple the groups
// silently.
//
// Regenerate the checked-in writers and readers with:
//
//	go generate ./internal/arrow/conformance/
//
// The short mode (-short) skips generator_test.go, which runs the
// generators and the compiler on the fixture packages under testdata.
package conformance
