---
name: arrow-generator
description: Enforces strict memory and performance rules when generating Apache
  Arrow builder code in Go.
triggers:
  - "generate arrow code"
  - "build arrow record"
  - "build arrow marshalling code generator"
  - "parquet backdoor"
---

# Go Arrow Engineering Rules

## Architectural Constraints

1. **NO RUNTIME REFLECTION:** Do not use Go's `reflect` package to map struct
   fields to Arrow buffers. All builder appends must be manually typed and
   explicit (e.g., `builder.Field(0).(*array.StringBuilder).Append(val)`).
2. **Pre-Cast Arrays:** When reading Arrow records, always cast the column
   arrays outside of the row iteration loop to prevent interface-check overhead.

## Memory Management

1. **Always Release:** Every `Record` and `Builder` must have a `defer
   Release()` immediately following its creation.
2. **Checked Allocators:** All generated test files must utilize
   `memory.CheckedAllocator` to verify that the generated code does not leak
   buffers.


## Execution Workflow
1. Once the generator code is written, you must use the Terminal Subagent to run
   `go test -v`.
2. If the checked allocator reports a memory leak, you must locate the missing
   `Release()` call, fix it, and re-run the tests before submitting the final
   Artifact.
