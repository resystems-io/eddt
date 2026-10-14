package conformance

//go:generate go run go.resystems.io/eddt/cmd/arrow-writer-gen -p . -s Deep1,ValueDeep1,Mixed -o nesting-arrow-writer.go -n conformance -v
//go:generate go run go.resystems.io/eddt/cmd/arrow-reader-gen -p . -s Deep1,ValueDeep1,Mixed -o nesting-arrow-reader.go -n conformance -v

// Deep1 through Deep8 form a chain of nullable struct pointers eight levels
// deep, well past the three levels production records use today. Every
// level carries a value so a test can tell which level went missing.
type Deep1 struct {
	Value string
	Next  *Deep2
}

type Deep2 struct {
	Value string
	Next  *Deep3
}

type Deep3 struct {
	Value string
	Next  *Deep4
}

type Deep4 struct {
	Value string
	Next  *Deep5
}

type Deep5 struct {
	Value string
	Next  *Deep6
}

type Deep6 struct {
	Value string
	Next  *Deep7
}

type Deep7 struct {
	Value string
	Next  *Deep8
}

type Deep8 struct {
	Value string
}

// ValueDeep1 through ValueDeep5 nest by value rather than by pointer, so
// no level can be null and the generator has to take the address of each
// level to append it.
type ValueDeep1 struct {
	Level int32
	Inner ValueDeep2
}

type ValueDeep2 struct {
	Level int32
	Inner ValueDeep3
}

type ValueDeep3 struct {
	Level int32
	Inner ValueDeep4
}

type ValueDeep4 struct {
	Level int32
	Inner ValueDeep5
}

type ValueDeep5 struct {
	Level int32
}

// Mixed interleaves every container kind between struct levels: a list of
// structs holding a map of lists of structs holding a fixed-size array of
// structs holding a pointer to a struct holding a list and a map. From the
// top-level record to the innermost scalar that is ten levels.
type Mixed struct {
	Items []MixedItem
}

type MixedItem struct {
	Attrs map[string][]MixedSub
}

type MixedSub struct {
	Points [2]MixedPoint
	Tags   map[string]map[string][]string
}

type MixedPoint struct {
	X, Y float64
	Meta *MixedMeta
}

type MixedMeta struct {
	Labels  []string
	Weights map[string]float64
}
