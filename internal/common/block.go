package common

import (
	"log"

	"github.com/nats-io/nats.go"
)

// CommonBlock provides the basic common building block for elements.
//
// It signals completion, includes logging and binds to the event broker.
type CommonBlock struct {
	Done   <-chan struct{}
	Logger *log.Logger
	NC     *nats.Conn
}
