package annotator

// Annotator is a package that provides functions to annotate Go structs with Parquet metadata.
// It is used to generate Parquet schemas from Go structs.
//
// This provides a tool that can be used by a code generator to re-annotate existing Go structs,
// adding the necessary Parquet tags to them. The annotator will operate in an idempotent manner,
// meaning that it will not modify the struct if the tags are already present and correct.
//
// The annotator will choose the parquet column type based on a combination of the Go type,
// with additional hints taken from the JSON tags, if present. All existing tags are kept as-is.
// The annotator will not modify any existing tags, even if they are incorrect.
