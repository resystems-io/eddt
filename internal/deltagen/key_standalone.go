package deltagen

// key_standalone.go provides hash-line rendering for standalone mode (--standalone).
// In standalone mode, EntityID() methods call local standaloneWrite* helpers
// (defined in the generated delta_types.go companion file) instead of the
// runtime.Write* functions used in normal mode.
//
// The encoding format is identical to the runtime helpers, so EntityIDs produced
// by blake2b standalone mode are byte-compatible with runtime-mode EntityIDs.

import (
	"fmt"
	"go/types"
	"sort"
)

// buildStandaloneKeyHashLines is the standalone-mode analogue of buildKeyHashLines.
// It returns hash-line strings that call standaloneWrite* helpers rather than
// runtime.Write* functions. The encoding strategy is identical, ensuring
// EntityID compatibility with runtime mode when blake2b is selected.
func buildStandaloneKeyHashLines(keyType types.Type, keyShape FieldShape) ([]string, error) {
	switch keyShape {
	case ShapeScalar:
		line, err := standaloneKeyHashLine(keyType, "k")
		if err != nil {
			return nil, err
		}
		return []string{line}, nil

	case ShapeStructValue:
		st, ok := keyType.Underlying().(*types.Struct)
		if !ok {
			return nil, fmt.Errorf(
				"R-DG-034: key type %s has ShapeStructValue but non-struct underlying",
				keyType)
		}

		type fieldEntry struct {
			name string
			typ  types.Type
		}
		var fields []fieldEntry
		for i := 0; i < st.NumFields(); i++ {
			f := st.Field(i)
			if !f.Exported() {
				continue
			}
			fields = append(fields, fieldEntry{f.Name(), f.Type()})
		}
		sort.Slice(fields, func(i, j int) bool { return fields[i].name < fields[j].name })

		var lines []string
		for _, fe := range fields {
			line, err := standaloneKeyHashLine(fe.typ, "k."+fe.name)
			if err != nil {
				return nil, fmt.Errorf("R-DG-034: key field %q: %w", fe.name, err)
			}
			lines = append(lines, line)
		}
		return lines, nil

	default:
		return nil, fmt.Errorf("R-DG-034: unsupported key shape %v for hash-line generation", keyShape)
	}
}

// standaloneKeyHashLine renders a single standaloneWrite* call for hashing
// recvExpr. The encoding rules mirror keyHashLine in key.go exactly, ensuring
// EntityID byte-compatibility between standalone and runtime modes.
func standaloneKeyHashLine(t types.Type, recvExpr string) (string, error) {
	b, ok := t.Underlying().(*types.Basic)
	if !ok {
		return "", fmt.Errorf(
			"R-DG-034: key field has unsupported underlying type %q; "+
				"only basic comparable types are hashable in this phase",
			t)
	}

	// cast wraps recvExpr with an explicit conversion when t is a named alias.
	cast := func(targetName string) string {
		if _, isBasic := t.(*types.Basic); isBasic {
			return recvExpr
		}
		return targetName + "(" + recvExpr + ")"
	}

	// The function prefix is "standalone" so "runtime.WriteString" becomes
	// "standaloneWriteString", maintaining naming symmetry with the companion file.
	const pfx = "standalone"
	switch b.Kind() {
	case types.String:
		return fmt.Sprintf("%sWriteString(h, %s)", pfx, cast("string")), nil
	case types.Bool:
		return fmt.Sprintf("%sWriteBool(h, %s)", pfx, cast("bool")), nil
	case types.Uint8:
		return fmt.Sprintf("%sWriteUint8(h, %s)", pfx, cast("uint8")), nil
	case types.Uint16:
		return fmt.Sprintf("%sWriteUint16(h, %s)", pfx, cast("uint16")), nil
	case types.Uint32:
		return fmt.Sprintf("%sWriteUint32(h, %s)", pfx, cast("uint32")), nil
	case types.Uint64:
		return fmt.Sprintf("%sWriteUint64(h, %s)", pfx, cast("uint64")), nil
	case types.Uint, types.Uintptr:
		return fmt.Sprintf("%sWriteUint64(h, uint64(%s))", pfx, recvExpr), nil
	case types.Int8:
		return fmt.Sprintf("%sWriteUint8(h, uint8(%s))", pfx, recvExpr), nil
	case types.Int16:
		return fmt.Sprintf("%sWriteUint16(h, uint16(%s))", pfx, recvExpr), nil
	case types.Int32:
		return fmt.Sprintf("%sWriteUint32(h, uint32(%s))", pfx, recvExpr), nil
	case types.Int64, types.Int:
		return fmt.Sprintf("%sWriteUint64(h, uint64(%s))", pfx, recvExpr), nil
	default:
		return "", fmt.Errorf(
			"R-DG-034: key field has unsupported underlying type %q; "+
				"only basic comparable types are hashable in this phase",
			t)
	}
}
