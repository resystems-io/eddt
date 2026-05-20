package deltagen

// parse_fields.go walks a Snapshot struct's fields exactly once, separating
// the embedded runtime.Header from candidate payload fields and classifying
// each payload field's Go type shape. Called by parseSnapshot in parse.go.

import (
	"fmt"
	"go/types"
	"reflect"
)

// walkFields walks the fields of st exactly once, returning the embedded
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
//   - Parse each candidate's eddt: tag string via parseTag (T-01); validate
//     the parsed tag against the field shape (T-02 / validateTagShape) and
//     against the combination rules (T-02 / validateTagCombination); store
//     both the raw string (for parseKeyField, pending T-03) and the
//     structured Tag on the candidate.
//
// The candidate slice may include a field tagged eddt:"entity.key";
// parseKeyField will subsequently remove it.
//
// structName is supplied only for error-message context.
func walkFields(
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
			return nil, nil, fmt.Errorf("field %s.%s: %w", structName, field.Name(), err)
		}

		if err := validateTagShape(tag, shape); err != nil {
			return nil, nil, fmt.Errorf("field %s.%s: %w", structName, field.Name(), err)
		}

		if err := validateTagCombination(tag); err != nil {
			return nil, nil, fmt.Errorf("field %s.%s: %w", structName, field.Name(), err)
		}

		fields = append(fields, ParsedField{
			Name:   field.Name(),
			RawTag: rawTag,
			Tag:    tag,
			Shape:  shape,
			GoType: field.Type(),
			Var:    field,
		})
	}

	return header, fields, nil
}

// classifyShape returns the FieldShape for a payload field type t.
//
// Classification is driven by the type's underlying type (t.Underlying()), so
// that named types (e.g. type Status int32) are correctly classified by their
// structural nature rather than their name. Map types are accepted and
// classified as ShapeMap; untagged maps are admitted with the atomic default
// per the harmonised three-axis model (refinements §1.6.3, Errata E-16).
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
