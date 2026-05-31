// Package deltagen implements the delta-gen code generator. It reads an EDDT
// Snapshot struct annotated with eddt:"entity.key" and delta.* struct tags and
// emits the companion Delta type together with the package-level functions
// Apply, Diff, Coalesce, and EntityID. When the output package matches the
// source package, optional ergonomic method wrappers are also emitted (R-DG-012, R-DG-013, R-DG-019).
//
// The generator pipeline has four stages, each delivered by its own item in
// the implementation plan:
//
//   - Load    (G-02): resolve the input package(s) with golang.org/x/tools/go/packages.
//   - Resolve (G-05): determine the output package name and cross-package mode.
//   - Parse   (G-03 / G-07 / G-04): walk the loaded types to find the Snapshot
//     struct, its embedded runtime.Header, its entity.key field, and its
//     payload fields. Driven by a single `parseSnapshot(pkgs, name, ParseOpts{...})`
//     call per target struct; the entity-key field is surfaced via
//     ParsedSnapshot.KeyVar and excluded from Fields. Per-struct key-field
//     overrides are supplied via Generator.KeyFields (G-06).
//   - Tag    (Phase 3): parse and validate eddt: tag values on payload fields.
//   - Emit    (Phase 4): render the TDelta type (R-DG-015) and, in later items,
//     Apply, Diff, Coalesce, EntityID bodies via text/template (template.go).
package deltagen
