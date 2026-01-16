# Event-Driven Digital Twin Model

This model aims to demonstrate how we can build a fully event-driven digital
twi

- System Requirements
- System Design

## System Requirements

### Architectural Requirements

The key principles are:
- continuous processing of digital twin state as events
- eventual consistency through decoupled assertion and correction streams
  - separate corrections through constraint enforcement
  - many separate constraints e.g. `1→* ⋀ *→1 ⇒ 1→1`
- horizontal scaling of mutators
- only track relationships that affect dataflow routing

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

We start by building a model, `eddt`. This model will provide simulated load for
the key observable data, and then demonstrate the critical path through the data
flow from relationship mapping, through routing and onto tabular views.


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

See the [flow diagram][functional-decomposition-diagram].

### Refinement Process

Once the model has been implemented we want to support the following story:

We will run one or more simulator processes. The Ⓢ simulators will generate both
relationship events and entity/element events. The relationship events simply
encode a directional mapping from one identity to another. Whereas, the element
events will encode a range of fields (or structures) associated with a single
element.

Each event source will be mapped to telemetry counters in order to show the
event rates at the source.

Once the simulators are running we can start manage the domain. This consists of
Ⓐ managing the life-cycle and consistency of the relationships, and then using
the relationships to Ⓑ perform mirroring and routing. Following this we we
perform Ⓒ aggregations and transformations. Finally, we Ⓓ record selected events
in tabular formats for analytics.

Our critical path consists first of Ⓑ , followed by Ⓓ . After that we can return
to manage life-cycle Ⓐ and more details via Ⓒ .

As part of aggregations and downsampling in Ⓒ we may need to coalesce multiple
subjects into a single mapping. Note, this is not just additive, but potentially
also subtractive. Further, for some mappings it may be the case that the
tracking is lossy, and that we need to maintain TTLs as part of the management.
The final output from the internal coalescence step is an accurate (modulo
eventual consistency) map from an element to a set of data.

Once we have basic ingestion, with realtime domain subjects and tabular storage
we can drive further Ⓔ enrichment and view Ⓣ tabular data or track Ⓡ realtime
data.

Therefore, to refine our minimal critical path we need:
1. Ⓢ  + Ⓑ  + Ⓡ
2. Ⓢ  + Ⓑ  + Ⓓ  + Ⓣ
3. Ⓢ  + Ⓑ  + Ⓡ  + Ⓔ 
4. Ⓐ
5. Ⓒ

### Simulation

Before attaching the digital twin to real-world systems, it is useful to develop
against simulations. While the EDDT pipeline is general purpose, the simulations
are domain specific. As such, simulations are maintained separately from the
EDDT system.

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


[functional-decomposition-diagram]:https://docs.google.com/drawings/d/1IUiV0ovTnqy8t4agOTi-EapfF1i8ygY2L422jbkJ80w/edit "(original source)"
