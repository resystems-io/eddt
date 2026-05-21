package deltagen

// key.go implements the hash-line renderer used by the EntityID emission stage
// (EM-05). It maps a key type (primitive or struct) to a slice of
// runtime.Write* call strings, one per field, in source declaration order.

import (
	"fmt"
	"go/types"
)

// buildKeyHashLines returns an ordered slice of runtime.Write* call strings for
// hashing a key of the given type and shape:
//   - ShapeScalar: one Write* line for the primitive key value itself (expr "k").
//   - ShapeStructValue: one Write* line per exported field in source order
//     (expr "k.<FieldName>").
//
// Used by buildSnapshotView to populate snapshotView.KeyHashLines (EM-05).
func buildKeyHashLines(keyType types.Type, keyShape FieldShape) ([]string, error) {
	switch keyShape {
	case ShapeScalar:
		line, err := keyHashLine(keyType, "k")
		if err != nil {
			return nil, err
		}
		return []string{line}, nil

	case ShapeStructValue:
		st, ok := keyType.Underlying().(*types.Struct)
		if !ok {
			return nil, fmt.Errorf(
				"EM-05: key type %s has ShapeStructValue but non-struct underlying",
				keyType)
		}
		var lines []string
		for i := 0; i < st.NumFields(); i++ {
			f := st.Field(i)
			if !f.Exported() {
				continue
			}
			line, err := keyHashLine(f.Type(), "k."+f.Name())
			if err != nil {
				return nil, fmt.Errorf("EM-05: key field %q: %w", f.Name(), err)
			}
			lines = append(lines, line)
		}
		return lines, nil

	default:
		return nil, fmt.Errorf("EM-05: unsupported key shape %v for hash-line generation", keyShape)
	}
}

// keyHashLine renders a single runtime.Write* call for hashing recvExpr, whose
// declared type is t. An explicit conversion to the target basic type is emitted
// when t is a named alias (e.g. type IMSI string → string(k)); raw basic types
// are passed directly. Signed integer types convert to the corresponding-width
// unsigned type (two's-complement, no information loss).
func keyHashLine(t types.Type, recvExpr string) (string, error) {
	b, ok := t.Underlying().(*types.Basic)
	if !ok {
		return "", fmt.Errorf(
			"EM-05: key field has unsupported underlying type %q; "+
				"only basic comparable types are hashable in this phase",
			t)
	}

	// cast wraps recvExpr with an explicit conversion to targetName when t is a
	// named alias. Raw basic types (t is already *types.Basic) need no wrapping.
	cast := func(targetName string) string {
		if _, isBasic := t.(*types.Basic); isBasic {
			return recvExpr
		}
		return targetName + "(" + recvExpr + ")"
	}

	switch b.Kind() {
	case types.String:
		return fmt.Sprintf("runtime.WriteString(h, %s)", cast("string")), nil
	case types.Bool:
		return fmt.Sprintf("runtime.WriteBool(h, %s)", cast("bool")), nil
	case types.Uint8: // covers byte (alias)
		return fmt.Sprintf("runtime.WriteUint8(h, %s)", cast("uint8")), nil
	case types.Uint16:
		return fmt.Sprintf("runtime.WriteUint16(h, %s)", cast("uint16")), nil
	case types.Uint32:
		return fmt.Sprintf("runtime.WriteUint32(h, %s)", cast("uint32")), nil
	case types.Uint64:
		return fmt.Sprintf("runtime.WriteUint64(h, %s)", cast("uint64")), nil
	case types.Uint, types.Uintptr:
		return fmt.Sprintf("runtime.WriteUint64(h, uint64(%s))", recvExpr), nil
	case types.Int8:
		return fmt.Sprintf("runtime.WriteUint8(h, uint8(%s))", recvExpr), nil
	case types.Int16:
		return fmt.Sprintf("runtime.WriteUint16(h, uint16(%s))", recvExpr), nil
	case types.Int32: // covers rune (alias)
		return fmt.Sprintf("runtime.WriteUint32(h, uint32(%s))", recvExpr), nil
	case types.Int64, types.Int:
		return fmt.Sprintf("runtime.WriteUint64(h, uint64(%s))", recvExpr), nil
	default:
		return "", fmt.Errorf(
			"EM-05: key field has unsupported underlying type %q; "+
				"only basic comparable types are hashable in this phase",
			t)
	}
}
