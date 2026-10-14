// Package genericcollision holds two instantiations of a generic whose
// derived names collide, because the derived name of a type argument that
// is itself a generic instantiation drops that argument's own arguments.
package genericcollision

type Pair[K, V any] struct {
	Key   K
	Value V
}

type Colliding struct {
	A Pair[string, Pair[int32, int32]]
	B Pair[string, Pair[string, string]]
}
