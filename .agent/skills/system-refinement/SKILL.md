---
name: system-refinement
description: Structured process for planning, implementing, verifying and
  documenting refinements to existing Go subsystems. Covers code review,
  design planning, incremental implementation, test verification, and
  documentation updates.
triggers:
  - "refine"
  - "implement refinement"
  - "fix bug in generator"
  - "implement checklist item"
  - "address refinement"
---

# System Refinement Process

A disciplined workflow for evolving existing subsystems. Each refinement
follows the same phases regardless of scope: plan first, implement
incrementally, verify at every step, and document when warranted.

## Phase 1 — Review and Plan

Present the plan for user approval before writing any code.

1. **Read the refinement spec.** Locate the relevant item in the project's
   refinements document (e.g. `docs/*-refinements.md`). Understand the
   problem statement, expected behaviour, and any noted dependencies.
2. **Review affected code.** Read every file that the refinement will touch.
   Understand the existing structure, invariants, and how the current code
   handles adjacent cases.
3. **Draft the plan.** Produce a numbered, step-by-step plan covering:
   - What changes are needed in each file (parser, template, types, etc.).
   - Which existing tests must be updated and why.
   - What new unit test cases are required.
   - What new integration test cases are required.
   - Whether a documentation update under `docs/` is warranted.
   - The order of operations (dependency-aware sequencing).
4. **Present for review.** Show the plan to the user and wait for explicit
   approval before proceeding.

## Phase 2 — Implement

Apply changes incrementally, verifying compilation at each step.

1. **Make structural changes first.** Type definitions, struct fields, and
   interface changes go in before any logic that depends on them.
2. **Update logic.** Modify parsers, generators, templates, or other logic
   to use the new structures.
3. **Compile-check after each logical unit.** Run `go vet ./...` (or the
   equivalent for the subsystem) after each coherent set of changes to
   catch errors early rather than accumulating breakage.
4. **Update existing tests.** Fix any test assertions that reference changed
   structures or produce different output. Do not leave known-broken tests
   for later.
5. **Add new test cases.** Write unit tests for every new code path. Write
   integration tests that exercise the full pipeline (generate, compile,
   run, verify output).

## Phase 3 — Verify

Run the complete test suite to confirm nothing has regressed.

1. **Unit tests.** `go test ./path/to/pkg/... -v -count=1` — all must pass.
2. **Integration tests.** Same command — integration subtests compile the
   generated code in an isolated module, run it, and verify output. All
   must pass.
3. **Benchmarks.** `go test ./path/to/pkg/... -bench=. -benchtime=1s` — run
   to confirm no performance regression. Note any significant changes.
4. **If any test fails:** diagnose, fix, and re-run the full suite. Do not
   proceed to Phase 4 with failures.

## Phase 4 — Document

Update project documentation to reflect the completed work.

1. **Refinements checklist.** Check off the completed item(s) with the
   current date. Update the change log table at the bottom. If the
   implementation supersedes or resolves other items, update those too.
2. **Architecture docs.** For complex changes (new recursion patterns, new
   template structures, new type-mapping strategies), create or update a
   dedicated document under `docs/` describing the design, rationale, and
   generated code examples.
3. **Commit message.** Draft a concise commit message that:
   - Starts with the subsystem prefix (e.g. `arrow:`).
   - States the intent, not just the mechanics.
   - References the refinement item IDs (e.g. S2, B4, B5).
   - Gives percentage-based impact metrics rather than raw line counts.
   - Mentions what is now possible that was not before.

## Common Pitfalls

- **Do not skip the planning phase.** Even for seemingly small fixes, the
  plan surfaces dependencies and test gaps that save time downstream.
- **Do not batch unrelated changes.** Each refinement item gets its own
  plan-implement-verify-document cycle and its own commit.
- **Do not leave test assertions referencing removed structures.** Update
  tests in the same step as the structural change to keep the build green.
- **Do not add depth limits or special-case guards when a recursive
  solution exists.** Prefer general solutions over ad-hoc nesting caps.
- **Compile-check frequently.** A `go vet` after each logical unit of
  change catches type errors before they compound.
