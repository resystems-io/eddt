package conformance

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func fullDeepChain() *Deep1 {
	return &Deep1{Value: "l1", Next: &Deep2{Value: "l2", Next: &Deep3{Value: "l3", Next: &Deep4{Value: "l4", Next: &Deep5{Value: "l5", Next: &Deep6{Value: "l6", Next: &Deep7{Value: "l7", Next: &Deep8{Value: "l8"}}}}}}}}
}

// TestDeepPointerChainRoundTrip nests nullable structs eight levels deep and
// cuts the chain at different depths, so a level that went missing or a null
// that was mistaken for a zero struct shows up at the exact level.
func TestDeepPointerChainRoundTrip(t *testing.T) {
	cutAtFour := fullDeepChain()
	cutAtFour.Next.Next.Next.Next = nil
	in := []*Deep1{
		fullDeepChain(),
		cutAtFour,
		{Value: "only the top"},
		{Value: "empty structs all the way", Next: &Deep2{Next: &Deep3{Next: &Deep4{Next: &Deep5{Next: &Deep6{Next: &Deep7{Next: &Deep8{}}}}}}}},
	}
	roundTripBoth(t, NewDeep1ArrowWriter(newPool(t)), func(rec arrow.RecordBatch) (loader[Deep1], error) {
		return NewDeep1ArrowReader(rec)
	}, in, derefs(in), pqarrow.DefaultWriterProps())
}

func TestValueChainRoundTrip(t *testing.T) {
	in := []*ValueDeep1{
		{Level: 1, Inner: ValueDeep2{Level: 2, Inner: ValueDeep3{Level: 3, Inner: ValueDeep4{Level: 4, Inner: ValueDeep5{Level: 5}}}}},
		{},
	}
	roundTripBoth(t, NewValueDeep1ArrowWriter(newPool(t)), func(rec arrow.RecordBatch) (loader[ValueDeep1], error) {
		return NewValueDeep1ArrowReader(rec)
	}, in, derefs(in), pqarrow.DefaultWriterProps())
}

// TestMixedContainerNestingRoundTrip interleaves lists, maps, fixed-size
// arrays and pointers between struct levels, ten levels from the record to
// the innermost scalar, and includes empty and nil containers at inner
// levels. The Parquet leg stores the Arrow schema because the fixture holds
// a fixed-size array (TestFixedSizeArraysThroughParquet).
func TestMixedContainerNestingRoundTrip(t *testing.T) {
	in := []*Mixed{
		{
			Items: []MixedItem{
				{
					Attrs: map[string][]MixedSub{
						"first": {
							{
								Points: [2]MixedPoint{
									{X: 1, Y: 2, Meta: &MixedMeta{Labels: []string{"a", "b"}, Weights: map[string]float64{"w": 0.5}}},
									{X: 3, Y: 4},
								},
								Tags: map[string]map[string][]string{
									"outer": {"inner": {"deepest"}, "empty": {}},
								},
							},
							{Tags: map[string]map[string][]string{}},
						},
						"empty": {},
						"nil":   nil,
					},
				},
				{Attrs: map[string][]MixedSub{}},
				{},
			},
		},
		{Items: []MixedItem{}},
		{},
	}
	roundTripBoth(t, NewMixedArrowWriter(newPool(t)), func(rec arrow.RecordBatch) (loader[Mixed], error) {
		return NewMixedArrowReader(rec)
	}, in, derefs(in), storedSchemaProps)
}
