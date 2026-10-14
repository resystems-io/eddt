// Package external holds types that conformance deliberately keeps out of
// the generators' --pkg list. A field typed with one of them therefore takes
// the external-type fallback path (TextMarshaler, then Stringer, then
// BinaryMarshaler on the write side, TextUnmarshaler or BinaryUnmarshaler on
// the read side) instead of native struct resolution, which is the path every
// third-party type in a production record takes.
package external

import (
	"errors"
	"fmt"
	"strings"
)

// TextOnly implements encoding.TextMarshaler and encoding.TextUnmarshaler,
// the first rung of the fallback chain, and nothing else.
type TextOnly struct {
	V string
}

func (t TextOnly) MarshalText() ([]byte, error) {
	return []byte(t.V), nil
}

func (t *TextOnly) UnmarshalText(b []byte) error {
	t.V = string(b)
	return nil
}

// StringerOnly implements fmt.Stringer only, the second rung, which has no
// unmarshal inverse.
type StringerOnly struct {
	V string
}

func (s StringerOnly) String() string {
	return "stringer:" + s.V
}

// BinaryOnly implements encoding.BinaryMarshaler and
// encoding.BinaryUnmarshaler, the third rung.
type BinaryOnly struct {
	V []byte
}

func (b BinaryOnly) MarshalBinary() ([]byte, error) {
	return b.V, nil
}

func (b *BinaryOnly) UnmarshalBinary(data []byte) error {
	b.V = append([]byte(nil), data...)
	return nil
}

// TextAndBinary implements every rung, so a test can see which one the
// generator picks. Each encoding carries its own prefix to make the choice
// visible in the column.
type TextAndBinary struct {
	V string
}

func (t TextAndBinary) MarshalText() ([]byte, error) {
	return []byte("text:" + t.V), nil
}

func (t *TextAndBinary) UnmarshalText(b []byte) error {
	t.V = strings.TrimPrefix(string(b), "text:")
	return nil
}

func (t TextAndBinary) String() string {
	return "string:" + t.V
}

func (t TextAndBinary) MarshalBinary() ([]byte, error) {
	return []byte("binary:" + t.V), nil
}

func (t *TextAndBinary) UnmarshalBinary(b []byte) error {
	t.V = strings.TrimPrefix(string(b), "binary:")
	return nil
}

// PointerText declares MarshalText on the pointer receiver only, the way
// types that marshal a large value often do.
type PointerText struct {
	V string
}

func (p *PointerText) MarshalText() ([]byte, error) {
	return []byte(p.V), nil
}

func (p *PointerText) UnmarshalText(b []byte) error {
	p.V = string(b)
	return nil
}

// Strict accepts only text carrying the "ok:" prefix, so a test can hand the
// generated reader a malformed payload and watch its error path.
type Strict struct {
	V string
}

func (s Strict) MarshalText() ([]byte, error) {
	return []byte("ok:" + s.V), nil
}

func (s *Strict) UnmarshalText(b []byte) error {
	text := string(b)
	if !strings.HasPrefix(text, "ok:") {
		return fmt.Errorf("strict: %q lacks the ok: prefix", text)
	}
	s.V = strings.TrimPrefix(text, "ok:")
	return nil
}

// FailingMarshal returns an error from MarshalText while Fail is set, to
// show what the generated writer does with a marshal error.
type FailingMarshal struct {
	V    string
	Fail bool
}

func (f FailingMarshal) MarshalText() ([]byte, error) {
	if f.Fail {
		return nil, errors.New("marshal refused")
	}
	return []byte(f.V), nil
}

func (f *FailingMarshal) UnmarshalText(b []byte) error {
	f.V = string(b)
	f.Fail = false
	return nil
}

// MarshalOnly has MarshalText and no UnmarshalText. It is referenced only by
// the generator tests under testdata, which show what arrow-reader-gen emits
// for a type with no inverse.
type MarshalOnly struct {
	V string
}

func (m MarshalOnly) MarshalText() ([]byte, error) {
	return []byte(m.V), nil
}

// Level is a named integer with a String method, the shape of every
// protoc-generated enum. Kept external so a field of this type resolves
// through the fallback chain rather than as a native int32 column.
type Level int32

const (
	LevelLow Level = iota
	LevelHigh
)

func (l Level) String() string {
	switch l {
	case LevelLow:
		return "low"
	case LevelHigh:
		return "high"
	default:
		return fmt.Sprintf("Level(%d)", int32(l))
	}
}

// Levels is a named slice of Level. Its element type reaches the generator
// through the type-checker resolution path rather than the syntax-tree path a
// bare Level field takes, which is exactly the difference the tests measure.
type Levels []Level
