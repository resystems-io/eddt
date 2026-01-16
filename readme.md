# Event-Driven Digital Twin

This system provides mechanisms and patterns needed to build a fully
event-driven digital twin.

## System Requirements

### Architectural Requirements

The key high-level principles are:
- continuous processing of digital twin state as events
- eventual consistency through decoupled assertion and correction streams
  - separate corrections through constraint enforcement
  - many separate constraints e.g. `1→* ⋀ *→1 ⇒ 1→1`
- horizontal scaling of mutators
- only track relationships that affect dataflow routing
- separately support analytics through columnar and relational storage

### Decomposition

1. Input as assertions or retractions of directed edges encoded as token pairs.
2. Routing maintained by multiple in-memory maps performing republishing of
   payloads.
3. Consistency maintained by multiple in-memory stream-group checkers performing
   order away retractions.
4. Historical and relational views created through continuous storage of
   compound payloads (key, struct) pairs.
5. Realtime visibility created through routing.
6. Historical relational visibility created through table/view access.


## System Design

The `eddt` system is designed by modelling the functional decomposition required
in order to continuously ingest event notifications from an external observation
layer, and then usher that information through a processing pathway that
includes: relationship extraction, relationship mapping, payload routing,
payload transformation and synthesis, and finally onto data tabulation with
supported access views.

### Functional Decomposition

This model covers each of the eleven
functions:

- `F1`: assert active relationships
- `F2`: retract relationships
- `F3`: monitor constraint violations & generation retractions
- `F4`: monitor relationships & maintain in-memory forward and reverse lookup
- `F5`: monitor payload streams/subjects and perform rerouting and mirroring (with subject rewrite based on lookups)
- `F6`: monitor subjects and perform downsampling (dropping/squashing or aggregation)
- `F7`: monitor subjects and perform payload upserts into tables.
- `F8`: track TTL for active relationships and expire inactive.
- `F9`: monitor relationships and synthesise active mapping payloads.
- `F10`: construct suitable SQL views via JOIN and @> contains leveraging GIN indices.
- `F11`: change detection based on revision comparison for select streams/subjects and their payloads.

#### Flow Diagram

![Flow Diagram][functional-decomposition-diagram]

[functional-decomposition-diagram]:docs/assets/Event_Driven_Digital_Twin_-_Functional_Decomposition.png

### Simulation

In order facilitate development of digital twin processing pathways, using EDDT,
event simulation tooling enables the creation of simulators that can induce
representative load with realistic event characteristics. These simulators can
be used to refine a given digital twin, together with the compiled rules and
routing definitions, before attaching a given digital twin realisation to
real-world systems.

However, while the EDDT pipeline is general purpose, the simulations are domain
specific. As such, simulations are maintained separately from the EDDT system.

### Data life-cycle

For every domain element, that is for every subject we _must_ be able to define
the life-cycle of that element. This includes:
- how the element is created,
- how the element is updated,
- how the element is refreshed,
- how the element is disposed.

That is, what the triggers are for these events. For this we define:
- sources (one-of): instrument observation, enrichment, routing, transform.
- disposal (one or both of): TTL and/or Signal.
- lifespan (one-of): relative (to create, update), absolute (explicit refresh
    with TTL or expiry)

In this way, every domain element has a well-defined life-cycle within the
system.

As part of the domain life-cycle management the system then _must_ expose a
life-cycle definition on a well-known subject path that can be inferred from the
domain path. It is the responsibility of the source publisher to ensure that the
life-cycle definition is also published.

In addition to the life-cycle definition, the element data definition schema
(and representation format) must also be published to a well-known subject path.
This might include: protobuf, json-schemai, arrow or it might be less well typed
and designate the use of JSON, CBOR.

## Vision and Future

Ultimately, the intention of EDDT to is to provide a performant, reliable and
scalable structure for building event-driven data processing that targets
maintaining eventually consistent relationships across complex domain models,
while enabling (soft) realtime observation of the digital twin together with
facilitating analytics pipelines and data access.

Please see the:
- [future plans][future-plans] in a window into the features that we aim to add.
- [release notes][release-notes] for a summary of features that have been added.

[future-plans]:docs/future-plans.md "EDDT Future Plans"
[release-notes]:docs/release-notes.md "EDDT Release Notes"
