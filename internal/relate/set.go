package relate

import (
	"fmt"
	"sort"

	flatbuffers "github.com/google/flatbuffers/go"
	"go.resystems.io/eddt/internal/relate/assertion"
	"go.resystems.io/eddt/internal/relate/relationset"
)

func kv_set_key(a *assertion.Assertion) string {
	return fmt.Sprintf("resys.sol.%s.r.kv.%s.%s.%s.set", a.Sid(), a.St(), a.Si(), a.Dt())
}

// kv_set_merge will merge an assertion into an existing relation set, returning a built relation set.
//
// Invariants:
//   - The merge depends on the fact that relation set table is always sorted by key (di).
//   - If the TTL of the assertion is zero, then the corresponding entry in the vector is removed in the created copy.
//   - If the destination (Di) already exists in the vector (which can be found via a binary search),
//     then the TTL and expiry (Exp) will be updated to new values in the created copy.
//   - If the asserted value does not exist in the vector then it must be added while retaining the sort order
//     in the resultant copy.
func kv_set_merge(a *assertion.Assertion, s *relationset.RelationSet) []byte {
	var builder *flatbuffers.Builder
	if false {
		// Interestingly it would seem reasonable to estimate the allocation size in order to
		// avoid buffer allocations. However, benchmarking shows this to actually be slower.
		estimate := 100 * s.RLength() /* n */ * 44
		builder = flatbuffers.NewBuilder(estimate)
	} else {
		// It is worth noting that the builder places a lot of performance pressure on the system.
		// We could consider using builder.Reset and reusing the builder.
		builder = flatbuffers.NewBuilder(1024)
	}

	return kv_set_merge_builder(a, s, builder)
}

func kv_set_merge_builder(a *assertion.Assertion, s *relationset.RelationSet, builder *flatbuffers.Builder) []byte {

	// Create a reusable accessor for reading the existing vector.
	// (Note, we do not use new(relationset.Relation) since this will be unnecessarily allocated on the heap.)
	var r relationset.Relation

	// Fetch the vector length and details.
	n := s.RLength()
	di := a.Di()

	// Use sort.Search to find the insertion point for the new assertion.
	// (REVIEW we could consider using s.RByKey(...) - though it doesn't provide the index.)
	i := sort.Search(n, func(i int) bool {
		s.R(&r, i)
		return string(r.Di()) >= string(di)
	})

	// Check if the item was found.
	found := false
	if i < n {
		if s.R(&r, i) && string(r.Di()) == string(di) {
			found = true
		}
	}

	// Create the new relations vector.
	newRelCount := n
	if a.Ttl() == 0 {
		if found {
			newRelCount--
		}
	} else {
		if !found {
			newRelCount++
		}
	}

	// Create the relation offsets.
	// We need to collect all the offsets before creating the vector.
	offsets := make([]flatbuffers.UOffsetT, newRelCount)
	offsetI := 0

	// Add relations before the insertion point.
	for j := range i {
		if s.R(&r, j) { // initialise the accessor
			di_offset := builder.CreateString(string(r.Di()))
			relationset.RelationStart(builder)
			relationset.RelationAddDi(builder, di_offset)
			relationset.RelationAddTtl(builder, r.Ttl())
			relationset.RelationAddExp(builder, r.Exp())
			offsets[offsetI] = relationset.RelationEnd(builder)
			offsetI++
		}
	}

	// Add the new/updated relation.
	// (Skip retracted elements designated by a zero TTL.)
	if a.Ttl() != 0 {
		di_new := builder.CreateString(string(a.Di()))
		relationset.RelationStart(builder)
		relationset.RelationAddDi(builder, di_new)
		relationset.RelationAddTtl(builder, a.Ttl())
		relationset.RelationAddExp(builder, a.Exp())
		offsets[offsetI] = relationset.RelationEnd(builder)
		offsetI++
	}

	// Add relations after the insertion/update point.
	start := i
	if found {
		start++
	}
	for j := start; j < n; j++ {
		if s.R(&r, j) { // initialise the accessor
			di_offset := builder.CreateString(string(r.Di()))
			relationset.RelationStart(builder)
			relationset.RelationAddDi(builder, di_offset)
			relationset.RelationAddTtl(builder, r.Ttl())
			relationset.RelationAddExp(builder, r.Exp())
			offsets[offsetI] = relationset.RelationEnd(builder)
			offsetI++
		}
	}

	// Create the relations vector.
	// (Note, flatbuffers requires us to have created all the relations in advance.)
	relationset.RelationSetStartRVector(builder, newRelCount)
	for j := len(offsets) - 1; j >= 0; j-- {
		builder.PrependUOffsetT(offsets[j])
	}
	relations := builder.EndVector(newRelCount)

	// Create the new relation set.
	sid := builder.CreateString(string(s.Sid()))
	st := builder.CreateString(string(s.St()))
	si := builder.CreateString(string(s.Si()))
	dt := builder.CreateString(string(s.Dt()))

	relationset.RelationSetStart(builder)
	relationset.RelationSetAddSid(builder, sid)
	relationset.RelationSetAddSt(builder, st)
	relationset.RelationSetAddSi(builder, si)
	relationset.RelationSetAddDt(builder, dt)
	relationset.RelationSetAddTs(builder, a.Exp()-a.Ttl())
	relationset.RelationSetAddR(builder, relations)
	relationSet := relationset.RelationSetEnd(builder)

	builder.Finish(relationSet)

	return builder.FinishedBytes()
}

// kv_set_create will create a new relation set from an assertion, returning a built relation set.
//
// The assertion is used to create a relation set with a single entry that contains the details from the assertion.
func kv_set_create(a *assertion.Assertion) []byte {
	builder := flatbuffers.NewBuilder(1024)

	// Create the strings used in the table
	sid := builder.CreateString(string(a.Sid()))
	di := builder.CreateString(string(a.Di()))
	st := builder.CreateString(string(a.St()))
	si := builder.CreateString(string(a.Si()))
	dt := builder.CreateString(string(a.Dt()))

	// Create the relation
	relationset.RelationStart(builder)
	relationset.RelationAddDi(builder, di)
	relationset.RelationAddTtl(builder, a.Ttl())
	relationset.RelationAddExp(builder, a.Exp())
	relation := relationset.RelationEnd(builder)

	// Create the relations vector
	relationset.RelationSetStartRVector(builder, 1)
	builder.PrependUOffsetT(relation)
	relations := builder.EndVector(1)

	// Create the relation set
	relationset.RelationSetStart(builder)
	relationset.RelationSetAddSid(builder, sid)
	relationset.RelationSetAddSt(builder, st)
	relationset.RelationSetAddSi(builder, si)
	relationset.RelationSetAddDt(builder, dt)
	relationset.RelationSetAddTs(builder, a.Exp()-a.Ttl())
	relationset.RelationSetAddR(builder, relations)
	relationSet := relationset.RelationSetEnd(builder)

	builder.Finish(relationSet)

	return builder.FinishedBytes()
}

// kv_set_empty creates a new empty relation set.
//
// The assertion is used to configure the relation set types.
func kv_set_empty(a *assertion.Assertion) []byte {
	builder := flatbuffers.NewBuilder(1024)

	// Create the strings used in the table
	sid := builder.CreateString(string(a.Sid()))
	st := builder.CreateString(string(a.St()))
	si := builder.CreateString(string(a.Si()))
	dt := builder.CreateString(string(a.Dt()))

	// Create the relations vector
	relationset.RelationSetStartRVector(builder, 0)
	relations := builder.EndVector(0)

	// Create the relation set
	relationset.RelationSetStart(builder)
	relationset.RelationSetAddSid(builder, sid)
	relationset.RelationSetAddSt(builder, st)
	relationset.RelationSetAddSi(builder, si)
	relationset.RelationSetAddDt(builder, dt)
	relationset.RelationSetAddTs(builder, a.Exp()-a.Ttl())
	relationset.RelationSetAddR(builder, relations)
	relationSet := relationset.RelationSetEnd(builder)

	builder.Finish(relationSet)

	return builder.FinishedBytes()
}
