package assert

import (
	"reflect"
	"testing"
)

func NoError(t *testing.T, err error) {
	if err != nil {
		t.Errorf("expected no error, was <%v>", err)
	}
}

// IsNil checks if the given `any` variable holds a nil value.
// It handles cases where the underlying concrete type is a nil pointer,
// slice, map, channel, or function.
//
// The handle the fact that if `any` variable has a non nil concrete
// underlying type, then even if the value is nil, the expression `v == nil`
// will return false.
func IsNil(v any) bool {
	// First, a standard check for a truly nil interface. This is a quick
	// and common case.
	if v == nil {
		return true
	}

	// Use the reflect package to check the value's kind.
	// We wrap this in a defer block to recover from a potential panic
	// if the value is not a type that can be nil (e.g., int, bool).
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer, reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Slice:
		return rv.IsNil()
	default:
		// For other kinds (like struct, int, string), they can't be nil
		// in this context, so we return false.
		return false
	}
}

func Nil(t *testing.T, check any) {
	if !IsNil(check) {
		t.Errorf("expected nil, was <%v> <type:%T>", check, check)
	}
}

func NotNil(t *testing.T, check any) {
	if check == nil {
		t.Errorf("expected non-nil")
	}
}

func EqualS(t *testing.T, lhs, rhs string) {
	if lhs != rhs {
		t.Errorf("expected <%s> was <%s>", lhs, rhs)
	}
}

func EqualI64(t *testing.T, lhs, rhs int64) {
	if lhs != rhs {
		t.Errorf("expected <%d> was <%d>", lhs, rhs)
	}
}

func EqualI(t *testing.T, lhs, rhs int) {
	if lhs != rhs {
		t.Errorf("expected <%d> was <%d>", lhs, rhs)
	}
}

func True(t *testing.T, truth bool) {
	if !truth {
		t.Errorf("expected true")
	}
}

func False(t *testing.T, truth bool) {
	if truth {
		t.Errorf("expected false")
	}
}
