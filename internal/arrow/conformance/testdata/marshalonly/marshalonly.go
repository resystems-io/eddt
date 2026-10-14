// Package marshalonly holds a field whose external type implements
// MarshalText and no UnmarshalText.
package marshalonly

import "go.resystems.io/eddt/internal/arrow/conformance/external"

type MarshalOnlyRecord struct {
	M external.MarshalOnly
}
