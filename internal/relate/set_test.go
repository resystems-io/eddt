package relate

import (
	"fmt"
	"testing"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	"go.resystems.io/eddt/internal/common/assert"
	"go.resystems.io/eddt/internal/relate/assertion"
	"go.resystems.io/eddt/internal/relate/relationset"
)

func Test_kv_set_create(t *testing.T) {
	// Create a test assertion
	a := &assertion.AssertionT{
		Ttl: 100,
		Exp: time.Now().Unix() + 100,
		Sid: "test-sid",
		Si:  "test-si",
		Di:  "test-di",
		St:  "test-st",
		Dt:  "test-dt",
	}
	builder := flatbuffers.NewBuilder(1024)
	aOffset := a.Pack(builder)
	builder.Finish(aOffset)
	aBytes := builder.FinishedBytes()
	aAssertion := assertion.GetRootAsAssertion(aBytes, 0)

	// Call the function to be tested
	resultSetBytes := kv_set_create(aAssertion)

	// Verify the result
	rs := relationset.GetRootAsRelationSet(resultSetBytes, 0)

	assert.EqualS(t, "test-st", string(rs.St()))
	assert.EqualS(t, "test-si", string(rs.Si()))
	assert.EqualS(t, "test-dt", string(rs.Dt()))
	assert.EqualI64(t, a.Exp-a.Ttl, rs.Ts())
	assert.EqualI(t, 1, rs.RLength()) // we should have only one entry in the vector

	relation := new(relationset.Relation)
	assert.True(t, rs.R(relation, 0))
	assert.EqualS(t, "test-di", string(relation.Di()))
	assert.EqualI64(t, a.Ttl, relation.Ttl())
	assert.EqualI64(t, a.Exp, relation.Exp())
}

func Test_kv_set_empty(t *testing.T) {
	// Create a test assertion
	a := &assertion.AssertionT{
		Ttl: 100,
		Exp: time.Now().Unix() + 100,
		Sid: "test-sid",
		Si:  "test-si",
		Di:  "test-di",
		St:  "test-st",
		Dt:  "test-dt",
	}
	builder := flatbuffers.NewBuilder(1024)
	aOffset := a.Pack(builder)
	builder.Finish(aOffset)
	aBytes := builder.FinishedBytes()
	aAssertion := assertion.GetRootAsAssertion(aBytes, 0)

	// Call the function to be tested
	resultSetBytes := kv_set_empty(aAssertion)

	// Verify the result
	rs := relationset.GetRootAsRelationSet(resultSetBytes, 0)

	assert.EqualS(t, "test-st", string(rs.St()))
	assert.EqualS(t, "test-si", string(rs.Si()))
	assert.EqualS(t, "test-dt", string(rs.Dt()))
	assert.EqualI64(t, a.Exp-a.Ttl, rs.Ts())
	assert.EqualI(t, 0, rs.RLength()) // we should have no entries in the vector
}

func Test_kv_set_merge(t *testing.T) {
	// 1. Test case: start from an empty set and add
	t.Run("merge into empty set", func(t *testing.T) {
		a1 := newAssertionT("sid", "st", "si", "dt", "di1", 100)
		a1Bytes := packAssertion(a1)
		a1Assertion := assertion.GetRootAsAssertion(a1Bytes, 0)

		emptySetBytes := kv_set_empty(a1Assertion)
		emptySet := relationset.GetRootAsRelationSet(emptySetBytes, 0)

		mergedSetBytes := kv_set_merge(a1Assertion, emptySet)
		mergedSet := relationset.GetRootAsRelationSet(mergedSetBytes, 0)

		assert.EqualI(t, 1, mergedSet.RLength())
		var r relationset.Relation
		mergedSet.R(&r, 0)
		assert.EqualS(t, "di1", string(r.Di()))
		assert.EqualI64(t, 100, r.Ttl())
	})

	// 2. Test case: start from a singleton and add
	t.Run("merge into singleton set - insert", func(t *testing.T) {
		a1 := newAssertionT("sid", "st", "si", "dt", "di1", 100)
		a1Bytes := packAssertion(a1)
		a1Assertion := assertion.GetRootAsAssertion(a1Bytes, 0)

		initialSetBytes := kv_set_create(a1Assertion)
		initialSet := relationset.GetRootAsRelationSet(initialSetBytes, 0)

		a2 := newAssertionT("sid", "st", "si", "dt", "di2", 200)
		a2Bytes := packAssertion(a2)
		a2Assertion := assertion.GetRootAsAssertion(a2Bytes, 0)

		mergedSetBytes := kv_set_merge(a2Assertion, initialSet)
		mergedSet := relationset.GetRootAsRelationSet(mergedSetBytes, 0)

		assert.EqualI(t, 2, mergedSet.RLength())

		var r relationset.Relation

		mergedSet.R(&r, 0)
		assert.EqualS(t, "di1", string(r.Di()))

		mergedSet.R(&r, 1)
		assert.EqualS(t, "di2", string(r.Di()))
	})

	// 3. Test case: start from a singleton and update
	t.Run("merge into singleton set - update", func(t *testing.T) {
		a1 := newAssertionT("sid", "st", "si", "dt", "di1", 100)
		a1Bytes := packAssertion(a1)
		a1Assertion := assertion.GetRootAsAssertion(a1Bytes, 0)

		initialSetBytes := kv_set_create(a1Assertion)
		initialSet := relationset.GetRootAsRelationSet(initialSetBytes, 0)

		a2 := newAssertionT("sid", "st", "si", "dt", "di1", 200)
		a2Bytes := packAssertion(a2)
		a2Assertion := assertion.GetRootAsAssertion(a2Bytes, 0)

		mergedSetBytes := kv_set_merge(a2Assertion, initialSet)
		mergedSet := relationset.GetRootAsRelationSet(mergedSetBytes, 0)

		assert.EqualI(t, 1, mergedSet.RLength())
		var r relationset.Relation
		mergedSet.R(&r, 0)
		assert.EqualS(t, "di1", string(r.Di()))
		assert.EqualI64(t, 200, r.Ttl())
	})

	// 4. Test case: asserting 0 TTL to non-existent elements
	t.Run("merge with zero TTL for non-existent element", func(t *testing.T) {
		a1 := newAssertionT("sid", "st", "si", "dt", "di1", 100)
		a1Bytes := packAssertion(a1)
		a1Assertion := assertion.GetRootAsAssertion(a1Bytes, 0)

		initialSetBytes := kv_set_create(a1Assertion)
		initialSet := relationset.GetRootAsRelationSet(initialSetBytes, 0)

		a2 := newAssertionT("sid", "st", "si", "dt", "di2", 0)
		a2Bytes := packAssertion(a2)
		a2Assertion := assertion.GetRootAsAssertion(a2Bytes, 0)

		mergedSetBytes := kv_set_merge(a2Assertion, initialSet)
		mergedSet := relationset.GetRootAsRelationSet(mergedSetBytes, 0)

		assert.EqualI(t, 1, mergedSet.RLength())
		var r relationset.Relation
		mergedSet.R(&r, 0)
		assert.EqualS(t, "di1", string(r.Di()))
	})

	// 5. Test case: insertion
	t.Run("merge insertion", func(t *testing.T) {
		a1 := newAssertionT("sid", "st", "si", "dt", "di1", 100)
		a1Bytes := packAssertion(a1)
		a1Assertion := assertion.GetRootAsAssertion(a1Bytes, 0)
		initialSetBytes := kv_set_create(a1Assertion)
		initialSet := relationset.GetRootAsRelationSet(initialSetBytes, 0)

		a3 := newAssertionT("sid", "st", "si", "dt", "di3", 300)
		a3Bytes := packAssertion(a3)
		a3Assertion := assertion.GetRootAsAssertion(a3Bytes, 0)
		mergedSetBytes1 := kv_set_merge(a3Assertion, initialSet)
		mergedSet1 := relationset.GetRootAsRelationSet(mergedSetBytes1, 0)

		a2 := newAssertionT("sid", "st", "si", "dt", "di2", 200)
		a2Bytes := packAssertion(a2)
		a2Assertion := assertion.GetRootAsAssertion(a2Bytes, 0)
		mergedSetBytes2 := kv_set_merge(a2Assertion, mergedSet1)
		mergedSet2 := relationset.GetRootAsRelationSet(mergedSetBytes2, 0)

		assert.EqualI(t, 3, mergedSet2.RLength())

		var r relationset.Relation

		mergedSet2.R(&r, 0)
		assert.EqualS(t, "di1", string(r.Di()))

		mergedSet2.R(&r, 1)
		assert.EqualS(t, "di2", string(r.Di()))

		mergedSet2.R(&r, 2)
		assert.EqualS(t, "di3", string(r.Di()))
	})

	// 6. Test case: update
	t.Run("merge update", func(t *testing.T) {
		a1 := newAssertionT("sid", "st", "si", "dt", "di1", 100)
		a1Bytes := packAssertion(a1)
		a1Assertion := assertion.GetRootAsAssertion(a1Bytes, 0)
		initialSetBytes := kv_set_create(a1Assertion)
		initialSet := relationset.GetRootAsRelationSet(initialSetBytes, 0)

		a2 := newAssertionT("sid", "st", "si", "dt", "di2", 200)
		a2Bytes := packAssertion(a2)
		a2Assertion := assertion.GetRootAsAssertion(a2Bytes, 0)
		mergedSetBytes1 := kv_set_merge(a2Assertion, initialSet)
		mergedSet1 := relationset.GetRootAsRelationSet(mergedSetBytes1, 0)

		a1_update := newAssertionT("sid", "st", "si", "dt", "di1", 150)
		a1UpdateBytes := packAssertion(a1_update)
		a1UpdateAssertion := assertion.GetRootAsAssertion(a1UpdateBytes, 0)
		mergedSetBytes2 := kv_set_merge(a1UpdateAssertion, mergedSet1)
		mergedSet2 := relationset.GetRootAsRelationSet(mergedSetBytes2, 0)

		assert.EqualI(t, 2, mergedSet2.RLength())

		var r relationset.Relation

		mergedSet2.R(&r, 0)
		assert.EqualS(t, "di1", string(r.Di()))
		assert.EqualI64(t, 150, r.Ttl())

		mergedSet2.R(&r, 1)
		assert.EqualS(t, "di2", string(r.Di()))
		assert.EqualI64(t, 200, r.Ttl())
	})

}

func Example_kv_set_merge() {
	sizes := []int{0, 1, 2, 3, 4, 5, 10, 100, 1000, 10000}

	for _, size := range sizes {
		// Create an initial set
		initialSet := createRelationSet(size)
		fmt.Printf("Relationset [size=%d] [bytes=%d]\n", size, len(initialSet.Table().Bytes))
	}

	// Output:
	// Relationset [size=0] [bytes=96]
	// Relationset [size=1] [bytes=144]
	// Relationset [size=2] [bytes=184]
	// Relationset [size=3] [bytes=232]
	// Relationset [size=4] [bytes=272]
	// Relationset [size=5] [bytes=312]
	// Relationset [size=10] [bytes=488]
	// Relationset [size=100] [bytes=4448]
	// Relationset [size=1000] [bytes=44048]
	// Relationset [size=10000] [bytes=440048]
}

func createRelationSet(size int) *relationset.RelationSet {
	builder := flatbuffers.NewBuilder(1024)
	var relationOffsets []flatbuffers.UOffsetT
	for i := range size {
		di := fmt.Sprintf("di%d", i)
		di_offset := builder.CreateString(di)
		relationset.RelationStart(builder)
		relationset.RelationAddDi(builder, di_offset)
		relationset.RelationAddTtl(builder, 100)
		relationset.RelationAddExp(builder, time.Now().Unix()+100)
		relationOffsets = append(relationOffsets, relationset.RelationEnd(builder))
	}

	relationset.RelationSetStartRVector(builder, len(relationOffsets))
	for i := len(relationOffsets) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(relationOffsets[i])
	}
	relations := builder.EndVector(len(relationOffsets))

	sid := builder.CreateString("sid")
	st := builder.CreateString("st")
	si := builder.CreateString("si")
	dt := builder.CreateString("dt")

	relationset.RelationSetStart(builder)
	relationset.RelationSetAddSid(builder, sid)
	relationset.RelationSetAddSt(builder, st)
	relationset.RelationSetAddSi(builder, si)
	relationset.RelationSetAddDt(builder, dt)
	relationset.RelationSetAddTs(builder, time.Now().Unix())
	relationset.RelationSetAddR(builder, relations)
	relationSetOffset := relationset.RelationSetEnd(builder)
	builder.Finish(relationSetOffset)

	finishedBytes := builder.FinishedBytes()

	return relationset.GetRootAsRelationSet(finishedBytes, 0)
}

func newAssertionT(sid, st, si, dt, di string, ttl int64) *assertion.AssertionT {
	return &assertion.AssertionT{
		Ttl: ttl,
		Exp: time.Now().Unix() + ttl,
		Sid: sid,
		Si:  si,
		Di:  di,
		St:  st,
		Dt:  dt,
	}
}

func packAssertion(a *assertion.AssertionT) []byte {
	builder := flatbuffers.NewBuilder(1024)
	aOffset := a.Pack(builder)
	builder.Finish(aOffset)
	return builder.FinishedBytes()
}
