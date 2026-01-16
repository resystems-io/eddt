package route

import (
	"fmt"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"

	flatbuffers "github.com/google/flatbuffers/go"
	"go.resystems.io/eddt/contract"
	"go.resystems.io/eddt/internal/common/assert"
	"go.resystems.io/eddt/internal/relate/relationset"
)

// mockRelationSetSource is a mock implementation of the RelationSetSource interface.
type mockRelationSetSource struct {
	sets map[string]*relationset.RelationSet
}

func newOtherThingRelationSetSource() RelationSetSource {

	var hello *relationset.RelationSet
	var today *relationset.RelationSet

	builder := flatbuffers.NewBuilder(1024)

	// hello set
	h := relationset.RelationSetT{
		Sid: "SOL_HELLO",
		St:  "src_hello_t",
		Si:  "THING",
		Dt:  "dst_hello_t",
		Ts:  0,
		R: []*relationset.RelationT{
			&relationset.RelationT{Di: "Alice", Ttl: 0, Exp: 0},
			&relationset.RelationT{Di: "Bob", Ttl: 0, Exp: 0},
			&relationset.RelationT{Di: "Charles", Ttl: 0, Exp: 0},
		},
	}
	hOffset := h.Pack(builder)
	builder.Finish(hOffset)
	builder_bytes := builder.FinishedBytes()
	hello_bytes := make([]byte, len(builder_bytes))
	copy(hello_bytes, builder_bytes)
	hello = relationset.GetRootAsRelationSet(hello_bytes, 0)

	// world set
	w := relationset.RelationSetT{
		Sid: "SOL_TODAY",
		St:  "src_world_t",
		Si:  "OTHER",
		Dt:  "dst_world_t",
		Ts:  0,
		R: []*relationset.RelationT{
			&relationset.RelationT{Di: "Mars", Ttl: 0, Exp: 0},
			&relationset.RelationT{Di: "Venus", Ttl: 0, Exp: 0},
		},
	}
	builder.Reset()
	wOffset := w.Pack(builder)
	builder.Finish(wOffset)
	builder_bytes = builder.FinishedBytes()
	world_bytes := make([]byte, len(builder_bytes))
	copy(world_bytes, builder_bytes)
	today = relationset.GetRootAsRelationSet(world_bytes, 0)

	otherthing := &mockRelationSetSource{
		sets: map[string]*relationset.RelationSet{
			"some.domain.rel.HELLO.set":  hello,
			"some.domain.rel.TODAY.list": today,
		},
	}

	if false {
		v, ok := otherthing.Get("some.domain.rel.HELLO.set")
		fmt.Printf("ok=%v v.RLength=%d\n", ok, v.RLength())
		v, ok = otherthing.Get("some.domain.rel.TODAY.list")
		fmt.Printf("ok=%v v.RLength=%d\n", ok, v.RLength())
	}

	return otherthing
}

// Get returns the RelationSet for the given key.
func (m *mockRelationSetSource) Get(key string) (*relationset.RelationSet, bool) {
	set, ok := m.sets[key]
	return set, ok
}

func hello_expected() []string {
	expected := []string{
		"some.domain.prefix.Alice.and.HELLO.with.more.Mars.numbered.WORLD.stuff",
		"some.domain.prefix.Alice.and.HELLO.with.more.Venus.numbered.WORLD.stuff",
		"some.domain.prefix.Bob.and.HELLO.with.more.Mars.numbered.WORLD.stuff",
		"some.domain.prefix.Bob.and.HELLO.with.more.Venus.numbered.WORLD.stuff",
		"some.domain.prefix.Charles.and.HELLO.with.more.Mars.numbered.WORLD.stuff",
		"some.domain.prefix.Charles.and.HELLO.with.more.Venus.numbered.WORLD.stuff",
	}
	return expected
}

func hello_route() contract.Route {
	route := contract.Route{
		ID:        "my-route",
		Disabled:  false,
		Match:     "some.domain.prefix.>",
		Transform: "some.domain.prefix.${thing}.and.${3}.with.more.${other}.numbered.${4}.stuff",
		References: map[string]string{
			"thing": "some.domain.rel.${3}.set",
			"other": "some.domain.rel.${5}.list",
		},
	}
	return route
}

func TestExecuteExpansion(t *testing.T) {
	t.Run("otherthing", func(t *testing.T) {
		otherthing := newOtherThingRelationSetSource()
		r, ok := otherthing.Get("some.domain.rel.HELLO.set")
		if !ok {
			t.Errorf("failed to fetch set")
		}
		assert.EqualI(t, 3, r.RLength())
	})

	t.Run("execute-route", func(t *testing.T) {

		expected := hello_expected()
		route := hello_route()
		cr, err := Compile(&route)
		if err != nil {
			t.Errorf("route compilation failed: %v", err)
		}

		otherthing := newOtherThingRelationSetSource()
		expansion, err := cr.execute("some.domain.prefix.HELLO.WORLD.TODAY", otherthing)

		if err != nil {
			t.Errorf("Failed to execute expansion: %v", err)
		}

		if len(expansion) != len(expected) {
			t.Errorf("expected [%d] expansions but there were [%d]", len(expected), len(expansion))
		}

		sort.Strings(expansion)
		diff := cmp.Diff(expected, expansion)
		if len(diff) != 0 {
			t.Errorf("expansion differs: %v", diff)
		}

		t.Logf("%v", expansion)
	})
	t.Run("cross-reference-names-with-hyphens", func(t *testing.T) {
		// TODO it seems like the expansion is failing when using cross-reference identifiers with hyphens
		t.Skip("Not yet implemented")
	})
	t.Run("empty-expansion", func(t *testing.T) {
		// TODO it seems like the expansion is not populating "missing" blank keys when there are no relations
		t.Skip("Not yet implemented")
	})
}

func Benchmark_route_expansion(b *testing.B) {

	route := hello_route()
	cr, err := Compile(&route)
	if err != nil {
		b.Errorf("route compilation failed: %v", err)
	}

	b.Run("execute-builder", func(b *testing.B) {
		otherthing := newOtherThingRelationSetSource()
		for b.Loop() {
			_, err := cr.execute("some.domain.prefix.HELLO.WORLD.TODAY", otherthing)
			if err != nil {
				break
			}
		}
	})
	b.Run("execute-concat", func(b *testing.B) {
		otherthing := newOtherThingRelationSetSource()
		for b.Loop() {
			_, err := cr.execute_concat("some.domain.prefix.HELLO.WORLD.TODAY", otherthing)
			if err != nil {
				break
			}
		}
	})
}
