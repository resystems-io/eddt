package conformance

//go:generate go run go.resystems.io/eddt/cmd/arrow-writer-gen -p . -s SkippedRecord -o skipped-arrow-writer.go -n conformance -v
//go:generate go run go.resystems.io/eddt/cmd/arrow-reader-gen -p . -s SkippedRecord -o skipped-arrow-reader.go -n conformance -v

import "fmt"

// Shape is a named interface with a method, declared in the input package.
type Shape interface {
	Area() float64
}

// SkippedRecord surrounds two ordinary fields with every field kind the
// documentation says the generators reject or skip, plus the interface and
// anonymous-struct shapes it does not mention. The generated schema must
// hold exactly Keep and Keep2, and the test that runs the generator itself
// records what each dropped field produced on stderr.
type SkippedRecord struct {
	Keep     string
	Callback func()
	Ch       chan int
	Addr     uintptr
	C128     complex128
	Any      any
	Iface    interface{}
	Shape    Shape
	Stringer fmt.Stringer
	Err      error
	Anon     struct{ X int64 }
	AnyList  []any
	AnyMap   map[string]any
	Keep2    int64
}
