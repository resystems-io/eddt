// Package enumslice holds slices of external named integers with a String
// method, the shape of a repeated protobuf enum.
package enumslice

import (
	"google.golang.org/protobuf/types/descriptorpb"

	"go.resystems.io/eddt/internal/arrow/conformance/external"
)

type EnumSlice struct {
	Levels []external.Level
	Types  []descriptorpb.FieldDescriptorProto_Type
}
