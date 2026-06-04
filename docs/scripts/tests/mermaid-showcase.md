# Mermaid Showcase

This self-test document exercises the Mermaid-fence support added to
`markdown-pdf.py`. Each section below embeds one diagram of a
different Mermaid type. Building this document to PDF verifies that:

1. Every supported diagram type renders as an SVG (not as a
   syntax-highlighted code listing).
2. Diagrams sit in their original position in the document flow.
3. The cache mechanism avoids re-rendering unchanged diagrams on
   repeat builds.
4. Mixing Mermaid blocks with prose, code blocks, and math does
   not confuse the pre-processor.

Build with:

```
make test-mermaid
```

from this `designs/scripts/tests/` directory, or directly via:

```
./designs/scripts/markdown-pdf.py designs/scripts/tests/mermaid-showcase.md
```

A plain code block (not Mermaid) should still render as syntax-
highlighted source:

```python
def hello():
    print("not a diagram — just a control")
```

An inline math check (also not Mermaid): $\mathcal{A}(s, d) = s'$.

---

## 1. Flowchart

```mermaid
graph TD
  A[Start] --> B{Branch?}
  B -->|Yes| C[Do Thing]
  B -->|No| D[Do Other]
  C --> E[End]
  D --> E
```

## 2. Sequence Diagram

```mermaid
sequenceDiagram
  participant Client
  participant Server
  participant DB
  Client->>Server: Request
  Server->>DB: Query
  DB-->>Server: Rows
  Server-->>Client: Response
```

## 3. Class Diagram

```mermaid
classDiagram
  class Snapshot {
    +Header header
    +Attached bool
    +TAI tai
  }
  class Delta {
    +Header header
    +Set<Attached> attached
  }
  class Header {
    +ChainID id
    +Sequence seq
  }
  Snapshot --> Header
  Delta --> Header
```

## 4. State Diagram

```mermaid
stateDiagram-v2
  [*] --> Idle
  Idle --> Attached: LAttach
  Attached --> Idle: LDetach
  Attached --> Handover: HandoverRequest
  Handover --> Attached: HandoverComplete
```

## 5. Entity-Relationship Diagram

```mermaid
erDiagram
  UE ||--o{ BEARER : has
  UE {
    string imsi PK
    string imei
    bool attached
  }
  BEARER {
    int erab_id PK
    int qci
    string apn
  }
```

## 6. Gantt Chart

```mermaid
gantt
  title Refinement Plan Phases
  dateFormat  YYYY-MM-DD
  section Foundation
  Phase 1 Scaffolding     :a1, 2026-04-19, 1d
  Phase 2 Pipeline wiring :a2, after a1, 1d
  section Polish
  Phase 3 CLI + errors    :a3, after a2, 1d
  Phase 4 CSS + theme     :a4, after a3, 1d
  Phase 5 Caching         :a5, after a4, 1d
  Phase 6 Tests           :a6, after a5, 1d
  Phase 7 Docs            :a7, after a6, 1d
```

## 7. Git Graph

```mermaid
gitGraph
  commit id: "init"
  branch feature
  checkout feature
  commit id: "add mermaid"
  commit id: "tests"
  checkout main
  merge feature
  commit id: "docs"
```

## 8. Pie Chart

```mermaid
pie title Open-question statuses
  "Resolved" : 9
  "Choice" : 4
  "Investigate" : 5
```

## 9. Mindmap

```mermaid
mindmap
  root((EDDT))
    Tiers
      Bronze
      Silver
      Gold
    Natures
      Snapshot
      Deltaflow
    POVs
      Columnar
      Relational
      Subject
```

## 10. Timeline

```mermaid
timeline
  title EDDT investigation progression
  section Phase 1
    Gedanken : Snapshot / Delta types
             : Algebraic invariants
  section Phase 2
    Refinements : Taint semantics
                : CRDT framework
  section Phase 3
    Split : §9.13 Handling
          : §9.18 Recovery
```

## 11. Sankey Diagram

```mermaid
sankey-beta
  Bronze,Silver,100
  Silver,Gold,40
  Silver,Analytics,60
  Gold,Dashboards,25
  Gold,Reports,15
```

---

## Closing

All eleven diagrams above should render as images in the PDF. If
any appear as syntax-highlighted code, the pre-processor is not
intercepting the fence correctly; if any fail to render, `mmdc`
returned a non-zero exit code (check stderr during build).

The `--no-mermaid` flag can be used to force fall-through to plain
code rendering, for offline work or when `mmdc` is unavailable.
