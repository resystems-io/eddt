# Event-Driven Digital Twin

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
