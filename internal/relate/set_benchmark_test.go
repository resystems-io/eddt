package relate

import (
	"fmt"
	"testing"

	flatbuffers "github.com/google/flatbuffers/go"

	"go.resystems.io/eddt/internal/relate/assertion"
)

func Benchmark_kv_set_merge(b *testing.B) {
	sizes := []int{1, 2, 4, 10, 100, 1000, 10000, 100000, 1000000}

	reuse := flatbuffers.NewBuilder(1024)

	for _, size := range sizes {
		b.Run(fmt.Sprintf("merge into set with %d relations", size), func(b *testing.B) {
			// Create an initial set
			initialSet := createRelationSet(size)

			// Create an assertion to merge
			a := newAssertionT("sid", "st", "si", "dt", "di_new", 200)
			aBytes := packAssertion(a)
			aAssertion := assertion.GetRootAsAssertion(aBytes, 0)

			b.Run("new-builder", func(b *testing.B) {
				// Run the benchmark loop.
				b.SetBytes(int64(size))
				for b.Loop() {
					kv_set_merge(aAssertion, initialSet)
				}
			})

			b.Run("reuse-builder", func(b *testing.B) {
				// Run the benchmark loop.
				b.SetBytes(int64(size))
				for b.Loop() {
					reuse.Reset()
					kv_set_merge_builder(aAssertion, initialSet, reuse)
				}
			})

		})
	}
}
