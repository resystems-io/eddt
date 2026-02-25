# Arrow-based Delta Flow Implementation Design

This document details the architecture and data flow for the Event-Driven Digital Twin (EDDT) using Apache Arrow and Parquet for high-performance delta processing.

## 1. Ingress and Type Definition

Data enters the system from external sources (e.g., IoT devices, edge gateways) often in row-based formats like JSON or CBOR. The ingress layer's responsibility is to map this incoming data into a strict, unified internal representation.

### Unified Go Structs

The core pattern involves defining standard Go structs that act as the single source of truth for a specific telemetry payload or event type. These structs use specific tags to drive both the unmarshalling of the ingress format and the generation of downstream schemas.

```go
// Example: Unified Telemetry Struct
type ScooterTelemetry struct {
	ScooterID   string  `json:"scooter_id" parquet:"name=scooter_id, logical=String"`
	Speed       float64 `json:"speed"      parquet:"name=speed"`
	Battery     int32   `json:"battery"    parquet:"name=battery, logical=Int32"`
	PitchSignal string  `json:"pitch_op"   parquet:"name=pitch_op, logical=String"`
}
```

## 2. Schema Derivation

To facilitate data-agnostic downstream processing, we require both an Arrow schema (for in-memory transport and processing) and a Parquet schema (for durable storage). We derive both automatically from the unified Go struct using the "Parquet backdoor" pattern.

### Derivation Flow

1.  **Struct to Parquet Schema**: We use the Go `parquet` library's reflection capabilities to generate a Parquet schema node directly from the annotated Go struct.
2.  **Parquet to Arrow Schema**: We then convert that Parquet schema node into a strict Arrow schema. This ensures perfect alignment between our in-memory transport format and our durable storage format without maintaining separate IDL definitions.

```go
// 1. Reflect struct into Parquet node
pqNode, err := schema.NewSchemaFromStruct(&ScooterTelemetry{})

// 2. Convert Parquet node to Arrow Schema
arrowSchema, err := pqarrow.FromParquet(pqNode, nil, nil)
```

## 3. Record Batch Population

While the schema generation is highly automated via reflection, the process of migrating row-based ingress data into columnar Arrow memory buffers must bypass reflection for maximum CPU throughput.

The ingress component maintains an Arrow `RecordBuilder` initialised with the derived `arrowSchema`. It must provide a custom, explicit implementation to extract fields from the populated Go structs and append them to the specific column builders within the `RecordBuilder`.

```go
// Explicit, high-performance columnar appending
builder.Field(0).(*array.StringBuilder).Append(update.ScooterID)
builder.Field(1).(*array.Float64Builder).Append(update.Speed)
// ...

// Finalise batch
recordBatch := builder.NewRecord()
```

## 4. NATS Publication Strategy

The EDDT ecosystem utilises NATS as its central nervous system. The data flow utilises two distinct publication streams: control (schema) and data (records).

### SchemaMessage Publication

Before a publisher can begin streaming variable-length record batches, it must broadcast the schema defining those batches. The publisher wraps the serialised Arrow schema into a `SchemaMessage` and publishes it over NATS.

This is necessary because Arrow IPC streams require the reader to possess the exact schema prior to decoding the associated record batches.

### Arrow RecordBatch Publication

Once the schema is established, the ingress provider takes the finalised Arrow `RecordBatch` (containing multiple rows of telemetry), serialises it via Arrow IPC (Inter-Process Communication), and publishes the resulting byte payload to the appropriate NATS data subject.

Every `RecordBatch` message **must** include a NATS header named `EDDT-Schema-ID` which holds the content-address (hash) of the schema used to encode the batch. This header allows the receiver to perform rapid schema lookups without needing to inspect the payload or maintain stateful knowledge of the stream's previous messages.

## 5. EDDT Receiver Responsibilities

The EDDT Receiver acts as the primary consumer of the delta streams. It subscribes to the NATS subjects and is responsible for both durable archiving and live-view maintenance.

### Schema Resolution and Storage

When the receiver consumes an Arrow IPC stream, it requires the corresponding schema.
1.  The receiver extracts the schema content-address from the `EDDT-Schema-ID` NATS header.
2.  The receiver looks up the required schema from a centralised NATS Key-Value (K-V) store using this content address.
3.  If the schema is newly published via a `SchemaMessage`, the receiver is responsible for persisting it to the NATS K-V store under its derived content address, making it available for recovery and subsequent consumers.

### Durable Parquet Archiving

The receiver routes the consumed Arrow delta streams to long-term storage.
1.  It retrieves the Arrow schema from the K-V store.
2.  It converts the Arrow schema *back* into a Parquet schema (or utilises the original Parquet schema if stored alongside).
3.  It streams the incoming Arrow RecordBatches directly into Parquet partition files on disk (or object storage), fulfilling the data lake / cold storage requirement.

### In-Memory Live Views

Simultaneously, the receiver routes the consumed Arrow delta streams into an in-memory execution engine. Given that the stream models "deltas" (additions, modifications, and specifically retractions/deletions), the receiver applies these deltas sequentially to maintain live, rolled-up views of the entity elements (e.g., the current real-time state of all scooters in a fleet).

These fully materialised live views are then available for rapid querying or further downstream event generation (routing/synthesis).
