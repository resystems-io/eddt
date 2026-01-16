package relate

import (
	flatbuffers "github.com/google/flatbuffers/go"

	"go.resystems.io/eddt/internal/relate/relationset"
)

var EMPTY_REPLATIONSET_BYTES []byte
var EMPTY_REPLATIONSET *relationset.RelationSet

func init() {

	// Create an empty relation set
	r := &relationset.RelationSetT{}

	builder := flatbuffers.NewBuilder(1024)
	rOffset := r.Pack(builder)
	builder.Finish(rOffset)
	rBytes := builder.FinishedBytes()

	EMPTY_REPLATIONSET_BYTES = rBytes
	EMPTY_REPLATIONSET = relationset.GetRootAsRelationSet(EMPTY_REPLATIONSET_BYTES, 0)
}
