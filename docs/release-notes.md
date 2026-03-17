# EDDT Release Notes

## 2026-03-18

### Added

- `arrow-writer-gen`: A code generator that statically introspects Go types and
  outputs highly optimized, reflection-free, zero-copy Apache Arrow append
  writers.
- `arrow-reader-gen`: The reciprocal tool to `arrow-writer-gen` that generates
  highly optimized, zero-allocation reader loops for extracting tabular Arrow
  data straight into native Go domain structs.

## 2026-01-16

### Added

- Initial release
