// Package emptystruct holds a struct with no fields, used by value, by
// pointer and as a list element.
package emptystruct

type Empty struct{}

type EmptyRecord struct {
	ID string
	E  Empty
	PE *Empty
	L  []Empty
}
