// Package deltagen implements the delta-gen code generator. It reads an EDDT
// Snapshot struct annotated with eddt:"entity.key" and delta.* struct tags and
// emits the companion Delta type together with the package-level functions
// Apply, Diff, Coalesce, and EntityID. When the output package matches the
// source package, optional ergonomic method wrappers are also emitted (R-DG-012, R-DG-013, R-DG-019).
//
// The generator pipeline has five stages:
//
//   - Load:    resolve the input package(s) with golang.org/x/tools/go/packages.
//   - Resolve: determine the output package name and cross-package mode.
//   - Parse:   walk the loaded types to find the Snapshot struct, its embedded
//     runtime.Header, its entity.key field, and its payload fields. Driven by
//     a single `parseSnapshot(pkgs, name, ParseOpts{...})` call per target struct;
//     the entity-key field is surfaced via ParsedSnapshot.KeyVar and excluded
//     from Fields. Per-struct key-field overrides are supplied via
//     Generator.KeyFields (R-DG-040).
//   - Tag:     parse and validate eddt: tag values on payload fields (R-DG-004–R-DG-009).
//   - Emit:    render the TDelta type (R-DG-015) and the Apply, Diff, Coalesce,
//     EntityID function bodies via text/template (template.go).
package deltagen
