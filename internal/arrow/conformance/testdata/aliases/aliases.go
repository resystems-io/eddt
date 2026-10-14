// Package aliases holds type aliases to a local struct and to time.Time.
package aliases

import "time"

type Local struct {
	A int64
}

type LocalAlias = Local

type TimeAlias = time.Time

type AliasRecord struct {
	L LocalAlias
	T TimeAlias
	P Local
}
