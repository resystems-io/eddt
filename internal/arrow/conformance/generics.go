package conformance

//go:generate go run go.resystems.io/eddt/cmd/arrow-writer-gen -p . -s GenericRecord -o generics-arrow-writer.go -n conformance -v
//go:generate go run go.resystems.io/eddt/cmd/arrow-reader-gen -p . -s GenericRecord -o generics-arrow-reader.go -n conformance -v

// Pair, List and Optional are the generic shapes the fixtures instantiate.
// The generator resolves each instantiation to a concrete struct with a
// derived name (Pair[string, int64] becomes Pair_String_Int64).
type Pair[K, V any] struct {
	Key   K
	Value V
}

type List[T any] struct {
	Items []T
}

type Optional[T any] struct {
	Value *T
	Set   bool
}

// GenItem is the concrete struct the generic fixtures are instantiated with.
type GenItem struct {
	N int64
}

// GenericRecord instantiates the generic types every way a record plausibly
// would: two instantiations of the same type with the arguments swapped,
// pointer and slice and map wrappers around an instantiation, and a type
// argument that is itself a pointer. A generic nested inside a generic is
// absent on purpose: the generator emits code for it that does not compile,
// which generator_test.go shows.
type GenericRecord struct {
	SI        Pair[string, int64]
	IS        Pair[int64, string]
	Items     List[GenItem]
	PtrItems  List[*GenItem]
	Opt       Optional[string]
	OptStruct Optional[GenItem]
	PPair     *Pair[string, int64]
	Pairs     []Pair[string, int64]
	PairMap   map[string]Pair[string, int64]
}
