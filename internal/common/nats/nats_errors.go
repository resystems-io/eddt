package nats

import (
	"fmt"
	"strings"

	"github.com/nats-io/nats.go"
)

// NatsIsErr checks for a substring match to an error string.
//
// Unfortunately `errors.Is(err, nats.ErrKeyNotFound)` does not work with NATS errors.
func NatsIsErr(err error, nerr error) bool {
	if err == nil || nerr == nil {
		return false
	}
	return strings.Contains(err.Error(), nerr.Error())
}

// NatsIsErrString checks for a substring match to an error string.
//
// Unfortunately `errors.Is(err, nats.ErrKeyNotFound)` does not work with NATS errors.
func NatsIsErrString(err error, nerr string) bool {
	if err == nil || len(nerr) == 0 {
		return false
	}
	return strings.Contains(err.Error(), nerr)
}

// NatsIsErr checks if the given error matches the key not found message.
//
// Unfortunately `errors.Is(err, nats.ErrKeyNotFound)` does not work with NATS errors.
func NatsIsErrKeyNotFound(err error) bool {
	return NatsIsErr(err, nats.ErrKeyNotFound)
}

var _wrong_last_sequence_error_code string

func init() {
	_wrong_last_sequence_error_code = fmt.Sprintf("err_code=%d", nats.JSErrCodeStreamWrongLastSequence)
}

// NatsIsErrWrongLastSequence tests if the given error relates to a bad sequence or revision check.
func NatsIsErrWrongLastSequence(err error) bool {
	return NatsIsErrString(err, _wrong_last_sequence_error_code)
}
