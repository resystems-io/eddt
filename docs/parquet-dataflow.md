# Design Requirement: Parquet Dataflow via Arrow

## Architecture Mandate

The Event-Driven Digital Twin (EDDT) architecture mandates a
* **Go Struct -> Arrow Record -> Parquet (via `pqarrow`)**
data serialization path.

Direct construction of Parquet files from Go structs using the low-level Apache
Parquet library is strictly prohibited.

---

## Background and Motivation

The decision to avoid manual construction of Parquet directly from Go structs in
favour of an Arrow intermediary is based on several architectural necessities
and engineering trade-offs.

### 1. Unified Egress Routing ("Write Once, Route Anywhere")

The EDDT architecture requires two distinct egress paths for ingested telemetry:
1. **The Network (Data in Motion):** Streaming over NATS via Arrow IPC.
  - There would be two additional subpaths:
    - Deltaflows streamed as partial updates.
    - Snapshots streamed as full updates.
2. **The Storage (Data at Rest):** Saving to the Bronze data lake as Parquet.

Writing directly to Parquet would force the maintenance of two entirely separate
manual mapping loops in the Go codebase: a `builder.Append()` loop for the NATS
stream, and a `columnWriter.WriteBatch()` loop for disk storage.

By building the in-memory Arrow `Record` first, the manual marshalling cost is
paid exactly once. That identical `Record` pointer can then be routed to both
the NATS IPC Writer and the `pqarrow.FileWriter` simultaneously.

Note, the Arrow schema is created by following the Parquet "backdoor" pattern
via the `pqarrow` package. This involves using `schema.NewSchemaFromStruct` to
create a Parquet schema from a Go struct, and then using `parrow.FromParquet` to
create the Arrow schema. This ensures that the Arrow schema is compatible with
the Parquet schema.

### 2. Abstraction of the Low-Level Parquet API

While the official Arrow Builder API requires manual value appending, it remains
logically straightforward. In contrast, the low-level Parquet Column Writer API
in Go is highly complex.

Because Parquet uses a flat column shredding model (the Dremel algorithm),
writing nested data structures (e.g., `List<BatteryReadings>`) directly requires
manual calculation of:
- **Definition Levels** (indicating nullability).
- **Repetition Levels** (indicating list boundaries).

Passing these as parallel integer arrays for every single value is error-prone;
a single mathematical error in repetition levels can corrupt the entire Parquet
file. The `pqarrow` package abstracts this complexity by automatically
calculating these levels directly from Arrow offsets.

### 3. Automatic Union Translation

To handle control signals like `ASSERT/RETRACT`, the system represents them as
Arrow Unions in memory. However, Parquet strictly requires Structs on disk.

Manually writing to Parquet would require complex, custom logic to pivot union
branches into physical struct tags. By utilizing the Arrow intermediary,
`pqarrow` handles the union-to-struct physical translation entirely under the
hood.

### Exceptions

The only scenario where bypassing Arrow to write directly to Parquet is
justified is when building a pure, isolated batch-logging application that is
severely RAM-constrained, possesses a completely flat schema (no lists or nested
structs), and has zero network streaming requirements. This exception does not
apply to the high-throughput, distributed nature of this digital twin
architecture.
