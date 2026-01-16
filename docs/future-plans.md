# EDDT Future Plans

Below are the plans for future development of the EDDT system.

These are not promises, but rather an indication of the direction in which we
expect to progress. No fixed timelines are provided.

However, the short descriptions should provide some indication of the
architectural intent, or alternatively, provide some inspiration for your next
contribution to the EDDT project 😉.

## Relationships

Relationship management is key to the EDDT design as it generalises the notion
of maintaining directional connections between elements. These relationships are
then used by the routing stage to republish payloads to new destination
subjects.

The performance of relationship management is critical, so the initial design
used Flatbuffers and the benchmarks are favourable. However, relationships are
also useful to the analytics stage.

We aim to export and support:
- Arrow relationship set representations while maintaining performance.
- expose telemetry focusing on relationship management performance.
- expose telemetry focusing on relationship management connectivity and
  volatility statistics.

## Analytics

Part of the EDDT design calls for the ability to perform analytics on the digital
twin. This is is dependent on serialising event notifications into suitable
database formats.

We aim to support:
- direct writes into relational tables (e.g. PostgreSQL)
- direct writes into columnar tables (e.g. Parquet)
- direct writes of Arrow relationship sets into Parquet files.
- facilitated views of the digital twin (e.g. SQL views with `duckdb` and
  Parquet globbing)

## Storage

For pre-processing prior to writing to analytics formats, it is useful to
persist event notifications in a format that is easy to process.

We aim to support:
- writes into event storage like Avro

## Transformations

Post processing of event notifications is performed by the transformations
stage. This enables event driven collation and aggregation of event
notifications. Additionally, this stage can enable synthetic events to be
produced.

We aim to provide:
- tooling and rules for generalised collation.
- tooling and rules for generalised aggregation.
- tooling and rules for generalised synthetic event production.

## Modelling

The EDDT system is built on the notion of relationships between elements. This
in turn depends on the ability to manage and model the digital twin domain for
specific problem spaces.

We aim to provide:
- tooling and rules for generalised digital twin domain modelling.
- tooling to visualise domain models.
- tooling to analyse the domain models.
- tooling to verify the constraints and requirements of domain models.
- tooling to facilitate the validation of domain models.

## Consumption

While not strictly a stage of the EDDT system, it is useful to be able to
consume event notifications (produced by any of the EDDT stages) in analytics
pipelines, external integrations or directly in frontends.

We aim to provide:
- exemplars and tooling for consuming event notifications in analytics
  pipelines.
- exemplars and tooling for consuming event notifications in external
  integrations.
- exemplars and tooling for consuming event notifications in frontends.
