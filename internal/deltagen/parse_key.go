package deltagen

// parse_key.go identifies and validates the entity-key field from the walkFields
// candidate list, supporting both tag-based discovery and CLI key-field override.
// Called by parseSnapshot in parse.go.

import (
	"fmt"
	"go/types"
)

// parseKeyField identifies the entity-key field among the walkFields candidates
// and partitions the slice into (keyVar, payload fields).
//
// Two identification paths are supported:
//
//   - Override path — opts.KeyFieldOverride is non-empty: the named field is
//     selected directly. Errors if no candidate has that name. Any
//     eddt:"entity.key" tags on the same struct are silently ignored; the CLI
//     layer emits a warning when it detects this combination.
//
//   - Tag path — opts.KeyFieldOverride is empty: candidates are scanned for
//     Tag.Kind == TagKindEntityKey. Exactly one match is required; zero and
//     multiple matches each produce a descriptive error.
//
// The selected field's type is validated: scalars and comparable struct values
// are accepted; pointers (identity != value equality), slices, and maps are
// rejected.
//
// Returned payload fields are the candidates with the key removed. structName
// is used only to scope error messages.
func parseKeyField(candidates []ParsedField, structName string, opts ParseOpts) (keyVar *types.Var, keyShape FieldShape, payloadFields []ParsedField, err error) {
	keyIdx := -1
	if opts.KeyFieldOverride != "" {
		for i := range candidates {
			if candidates[i].Name == opts.KeyFieldOverride {
				keyIdx = i
				break
			}
		}
		if keyIdx == -1 {
			return nil, 0, nil, fmt.Errorf(
				"struct %q: --key-field override names field %q which is not present in the struct",
				structName, opts.KeyFieldOverride)
		}
	} else {
		for i := range candidates {
			if candidates[i].Tag.Kind != TagKindEntityKey {
				continue
			}
			if keyIdx != -1 {
				return nil, 0, nil, fmt.Errorf(
					"struct %q has multiple fields tagged eddt:%q (at least %q and %q); exactly one entity-key field is required",
					structName, tagEntityKey, candidates[keyIdx].Name, candidates[i].Name)
			}
			keyIdx = i
		}
		if keyIdx == -1 {
			return nil, 0, nil, fmt.Errorf(
				"struct %q has no field tagged eddt:%q; a conforming Snapshot must mark exactly one entity-key field "+
					"(or supply --key-field on the command line)",
				structName, tagEntityKey)
		}
	}

	keyField := &candidates[keyIdx]

	switch keyField.Shape {
	case ShapeScalar:
		// Basic and named basic types are always comparable.

	case ShapeStructValue:
		if !types.Comparable(keyField.GoType) {
			if st, ok := keyField.GoType.Underlying().(*types.Struct); ok {
				for i := 0; i < st.NumFields(); i++ {
					f := st.Field(i)
					if !types.Comparable(f.Type()) {
						return nil, 0, nil, fmt.Errorf(
							"struct %q: entity-key field %q has non-comparable sub-field %q of type %s; "+
								"all fields of a key struct must be comparable",
							structName, keyField.Name, f.Name(), f.Type())
					}
				}
			}
			return nil, 0, nil, fmt.Errorf(
				"struct %q: entity-key field %q has non-comparable type %s",
				structName, keyField.Name, keyField.GoType)
		}

	case ShapePointer:
		return nil, 0, nil, fmt.Errorf(
			"struct %q: entity-key field %q has pointer type %s; key fields must be value types "+
				"(pointer equality is identity, not value equality)",
			structName, keyField.Name, keyField.GoType)

	case ShapeSlice:
		return nil, 0, nil, fmt.Errorf(
			"struct %q: entity-key field %q has slice type %s; slices are not comparable and cannot be entity keys",
			structName, keyField.Name, keyField.GoType)

	case ShapeMap:
		return nil, 0, nil, fmt.Errorf(
			"struct %q: entity-key field %q has map type %s; maps are not comparable and cannot be entity keys",
			structName, keyField.Name, keyField.GoType)

	default:
		return nil, 0, nil, fmt.Errorf(
			"internal: unhandled key field shape %v for field %q in struct %q",
			keyField.Shape, keyField.Name, structName)
	}

	keyVar = keyField.Var
	payloadFields = make([]ParsedField, 0, len(candidates)-1)
	payloadFields = append(payloadFields, candidates[:keyIdx]...)
	payloadFields = append(payloadFields, candidates[keyIdx+1:]...)
	return keyVar, keyField.Shape, payloadFields, nil
}
