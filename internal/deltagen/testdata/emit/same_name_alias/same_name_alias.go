package same_name_alias

// same_name_alias_test fixture — exercises the alias-promoted cross-package
// scenario where the source and output packages share a short name.
//
// When delta-gen is invoked with:
//   --pkg-name same_name_alias  (matches this package's own short name)
//   --pkg-alias <this-path>=sna (explicit alias for the source package)
//
// Prior to the fix, crossPackage was false (name match), so types appeared
// unqualified and the import was omitted. With the fix, the alias promotes
// crossPackage to true, so types are qualified "sna.XxxSnapshot" and the
// aliased import is included.

import eddt "go.resystems.io/eddt/runtime"

// SubStruct is a field type referenced in the Snapshot.
type SubStruct struct {
	Value int32
}

// SameNameSnapshot is the target struct. Its field type (SubStruct) comes from
// this package, so its qualified form in cross-package mode is "sna.SubStruct".
type SameNameSnapshot struct {
	eddt.Header
	Key    string    `eddt:"entity.key"`
	Score  int32
	Detail SubStruct
}
