package conformance

//go:generate go run go.resystems.io/eddt/cmd/arrow-writer-gen -p . -s ScalarSlices,StructSlices,Jagged,MapsRecord,NamedContainers,PointerContainers,ArraysRecord,ScaleRecord -o containers-arrow-writer.go -n conformance -v
//go:generate go run go.resystems.io/eddt/cmd/arrow-reader-gen -p . -s ScalarSlices,StructSlices,Jagged,MapsRecord,NamedContainers,PointerContainers,ArraysRecord,ScaleRecord -o containers-arrow-reader.go -n conformance -v

import "time"

// ContItem is the struct element used by every container fixture in this
// file.
type ContItem struct {
	N int32
	S string
}

// Tags, Config, MAC, Blob and Items are named container types. The generator
// unwraps them to their underlying Arrow representation while the generated
// code keeps the named Go type.
type Tags []string

type Config map[string]int64

type MAC [6]byte

type Blob []byte

type Items []ContItem

// ScalarSlices covers slices of every scalar kind, including the two that
// get special treatment: []byte becomes one Binary value, and []time.Time
// applies the timestamp conversion per element.
type ScalarSlices struct {
	Ints      []int64
	Strs      []string
	Bools     []bool
	Floats    []float64
	Bytes     []byte
	BytesList [][]byte
	Runes     []rune
	Times     []time.Time
	Durs      []time.Duration
}

// StructSlices covers slices of structs by value, by pointer (with nil
// elements) and doubly nested.
type StructSlices struct {
	Vals   []ContItem
	Ptrs   []*ContItem
	Nested [][]ContItem
}

// Jagged covers list nesting three deep. A list of maps is absent on
// purpose: the reader the generator emits for it does not compile, which
// generator_test.go shows.
type Jagged struct {
	D2 [][]int32
	D3 [][][]string
}

// MapsRecord covers the map shapes: scalar, integer, boolean and float keys,
// struct and pointer-struct values (with nil values), binary and timestamp
// values, list values, and maps nested two and three deep.
type MapsRecord struct {
	Scalars    map[string]int64
	IntKeys    map[int64]string
	BoolKeys   map[bool]int32
	FloatKeys  map[float64]string
	Structs    map[string]ContItem
	PtrStructs map[string]*ContItem
	Bytes      map[string][]byte
	Times      map[string]time.Time
	Lists      map[string][]int64
	OfMaps     map[string]map[int64]ContItem
	Deep       map[string]map[string]map[string]int64
}

// NamedContainers uses the named container types declared above.
type NamedContainers struct {
	Tags   Tags
	Config Config
	MAC    MAC
	Blob   Blob
	Items  Items
}

// PointerContainers covers the pointer-wrapped compounds the documentation
// lists: pointer to slice, pointer to map, pointer to byte slice, pointer to
// pointer, and pointer to a slice of structs.
type PointerContainers struct {
	PSlice       *[]string
	PMap         *map[string]int64
	PBytes       *[]byte
	PP           **int64
	PStructSlice *[]ContItem
}

// ArraysRecord covers fixed-size arrays: bytes, structs, strings and a
// two-dimensional array. Arrays and slices whose elements are pointers to
// scalars are absent on purpose: the reader the generator emits for them
// does not compile, which generator_test.go shows.
type ArraysRecord struct {
	Tag    [4]byte
	Items  [3]ContItem
	Strs   [2]string
	Matrix [2][2]int32
}

// ScaleRecord holds a slice sized into the tens of thousands by the tests,
// to check that nothing degrades with volume.
type ScaleRecord struct {
	Items []ContItem
}
