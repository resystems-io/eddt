// Package genericsliceargs holds a generic instantiated with a slice type
// argument, whose derived name is not a valid Go identifier.
package genericsliceargs

type Pair[K, V any] struct {
	Key   K
	Value V
}

type SliceArg struct {
	P Pair[[]int32, int32]
}
