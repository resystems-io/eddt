package deltagen

// parse_fields.go walks a Snapshot struct's fields exactly once, separating
// the embedded runtime.Header from candidate payload fields and classifying
// each payload field's Go type shape. Called by parseSnapshot in parse.go.

import (
	"fmt"
	"go/types"
	"reflect"
	"strings"
)

// parseFields walks the fields of st exactly once, returning the embedded
// runtime.Header field separately from the candidate payload fields. It is
// the internal helper that step 3 of parseSnapshot delegates to.
//
// Responsibilities:
//
//   - Identify the embedded runtime.Header field by type identity (compared
//     against headerType). Multiple Header fields are an error.
//   - In cross-package mode (opts.CrossPackage), silently drop unexported
//     fields — they are inaccessible from the generated code.
//   - Classify each non-Header field's Go type via classifyShape and reject
//     unsupported shapes (function, channel, interface).
//   - Parse each candidate's eddt: tag string via parseTag; validate the
//     parsed tag against the field shape (validateTagShape) and combination
//     rules (validateTagCombination); store the structured Tag on the
//     candidate (Tag.Raw preserves the verbatim source for diagnostics).
//
// The candidate slice may include a field tagged eddt:"entity.key";
// parseKeyField will subsequently remove it.
//
// structName is supplied only for error-message context.
func parseFields(
	st *types.Struct,
	structName string,
	headerType types.Type,
	opts ParseOpts,
) (header *types.Var, fields []ParsedField, err error) {
	for i := 0; i < st.NumFields(); i++ {
		field := st.Field(i)

		if opts.CrossPackage && !field.Exported() {
			continue
		}

		if types.Identical(field.Type(), headerType) {
			if header != nil {
				return nil, nil, fmt.Errorf(
					"struct %q has multiple embedded runtime.Header fields; exactly one is required",
					structName)
			}
			header = field
			continue
		}

		shape, err := classifyShape(field.Type())
		if err != nil {
			return nil, nil, fmt.Errorf("field %s.%s: %w", structName, field.Name(), err)
		}

		rawTag := reflect.StructTag(st.Tag(i)).Get("eddt")

		tag, err := parseTag(rawTag)
		if err != nil {
			return nil, nil, fmt.Errorf("field %s.%s: parsing eddt:%q: %w", structName, field.Name(), rawTag, err)
		}

		if err := validateTagShape(tag, shape); err != nil {
			return nil, nil, fmt.Errorf("field %s.%s: %w", structName, field.Name(), err)
		}

		if tag.Kind == TagKindNested && shape == ShapeStructValue {
			if containsHeaderEmbed(field.Type(), headerType) {
				return nil, nil, fmt.Errorf(
					"field %s.%s: delta.nested struct type %s embeds runtime.Header; "+
						"nested types must be sub-structures, not chain anchors (§3.3.2)",
					structName, field.Name(), field.Type())
			}
			// R-DG-009: cycle guard — seed path with the snapshot struct name so the
			// error message reads "SnapshotName → A → B → A".
			if err := validateNestedAcyclic(field.Type(), []string{structName}, headerType); err != nil {
				return nil, nil, fmt.Errorf("field %s.%s: %w", structName, field.Name(), err)
			}
		}

		if err := validateTagCombination(tag); err != nil {
			return nil, nil, fmt.Errorf("field %s.%s: %w", structName, field.Name(), err)
		}

		fields = append(fields, ParsedField{
			Name:   field.Name(),
			Tag:    tag,
			Shape:  shape,
			GoType: field.Type(),
			Var:    field,
		})
	}

	return header, fields, nil
}

// containsHeaderEmbed reports whether t directly embeds a field of type headerType.
// Used by parseFields to reject delta.nested struct types that embed runtime.Header
// (they would be chain anchors, not sub-structures — §3.3.2).
func containsHeaderEmbed(t types.Type, headerType types.Type) bool {
	st, ok := t.Underlying().(*types.Struct)
	if !ok {
		return false
	}
	for i := 0; i < st.NumFields(); i++ {
		if types.Identical(st.Field(i).Type(), headerType) {
			return true
		}
	}
	return false
}

// validateNestedAcyclic returns a non-nil error if the delta.nested type graph
// rooted at t contains a cycle reachable via struct-value delta.nested fields.
// path is the sequence of type names already on the current ancestry chain (the
// snapshot struct name seeds it, so the error reads "Snapshot → A → B → A").
//
// For struct-value shapes Go's type checker prevents cycles in source code; this
// function provides the infrastructure that R-DG-016/R-DG-016, R-DG-028 will extend for map/slice
// paths, and gives a clear diagnostic for programmatically-constructed cycles.
func validateNestedAcyclic(t types.Type, path []string, headerType types.Type) error {
	named, ok := t.(*types.Named)
	if !ok {
		return nil // anonymous struct already rejected elsewhere
	}
	typeName := named.Obj().Name()
	for _, ancestor := range path {
		if ancestor == typeName {
			cycle := strings.Join(append(path, typeName), " → ")
			return fmt.Errorf("delta.nested type chain forms a cycle: %s (§3.3.2)", cycle)
		}
	}
	st, ok := named.Underlying().(*types.Struct)
	if !ok {
		return nil
	}
	newPath := append(path, typeName)
	for i := 0; i < st.NumFields(); i++ {
		field := st.Field(i)
		rawTag := reflect.StructTag(st.Tag(i)).Get("eddt")
		tag, err := parseTag(rawTag)
		if err != nil {
			continue // malformed tag — parse stage will surface this separately
		}
		if tag.Kind != TagKindNested {
			continue
		}
		shape, err := classifyShape(field.Type())
		if err != nil || shape != ShapeStructValue {
			continue // non-struct shapes handled by R-DG-016/R-DG-016, R-DG-028
		}
		if err := validateNestedAcyclic(field.Type(), newPath, headerType); err != nil {
			return err
		}
	}
	return nil
}

// classifyShape returns the FieldShape for a payload field type t.
//
// Classification is driven by the type's underlying type (t.Underlying()), so
// that named types (e.g. type Status int32) are correctly classified by their
// structural nature rather than their name. Map types are accepted and
// classified as ShapeMap; untagged maps are admitted with the atomic default
// per the harmonised three-axis model (refinements §1.6.3, Errata R-DG-006, R-DG-016).
func classifyShape(t types.Type) (FieldShape, error) {
	switch t.Underlying().(type) {
	case *types.Basic:
		return ShapeScalar, nil
	case *types.Struct:
		return ShapeStructValue, nil
	case *types.Pointer:
		return ShapePointer, nil
	case *types.Slice:
		return ShapeSlice, nil
	case *types.Map:
		return ShapeMap, nil
	case *types.Signature:
		return 0, fmt.Errorf("function-valued fields are not supported by delta-gen (§3.2)")
	case *types.Chan:
		return 0, fmt.Errorf("channel fields are not supported by delta-gen (§3.2)")
	case *types.Interface:
		return 0, fmt.Errorf("interface-typed fields are not supported by delta-gen (§3.2)")
	default:
		return 0, fmt.Errorf("unsupported field type %T (not in delta-gen §3.2 payload shape catalogue)", t.Underlying())
	}
}
