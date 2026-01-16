package nats

import (
	"strings"

	"github.com/nats-io/nats.go"
)

// NatsIsErr checks for a match to and error string.
//
// Unfortunately `errors.Is(err, nats.ErrKeyNotFound)` does not work with NATS errors.
func NatsIsErr(err error, nerr error) bool {
	if err == nil || nerr == nil {
		return false
	}
	return strings.Contains(err.Error(), nerr.Error())
}

// NatsIsErr checks if this error matches the key not found message.
//
// Unfortunately `errors.Is(err, nats.ErrKeyNotFound)` does not work with NATS errors.
func NatsIsErrKeyNotFound(err error) bool {
	return NatsIsErr(err, nats.ErrKeyNotFound)
}
