// Package genericnested holds a generic instantiated with another generic,
// and a named slice of a generic instantiation.
package genericnested

type Pair[K, V any] struct {
	Key   K
	Value V
}

type List[T any] struct {
	Items []T
}

type Item struct {
	N int64
}

type PairList []Pair[string, int64]

type NestedGeneric struct {
	Nested Pair[string, List[Item]]
}

type NamedSliceOfGeneric struct {
	Pairs PairList
}
