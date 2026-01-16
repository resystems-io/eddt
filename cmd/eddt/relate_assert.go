package main

import (
	"time"

	"github.com/nats-io/nats.go"
	"go.resystems.io/eddt/internal/relate"
)

func RunAssertionProcessing(end <-chan struct{}) error {

	// connect to NATS
	nats_config.URLS = nats.DefaultURL

	opts := []nats.Option{nats.Name("EDDT - relationship compiler")}
	opts = setupNATSConnOptions(opts)
	opts = setupNATSConfigOptions(opts)

	nc, err := nats.Connect(nats_config.URLS, opts...)
	if err != nil {
		return err
	}

	// create the asserter
	asserter := &relate.RelationAsserter{
		NC:    nc,
	}
	ready := make(chan struct{})
	go asserter.Launch(end, ready)

	// TODO we need to be able to reconfigure the compiler at runtime
	// (i.e. rules should be posted via rules->compiler.Profiles)

	// wait for ready
	select {
	case <-ready:
	case <-end:
	case <-time.After(time.Second * 5):
		panic("Timed out waiting for asserter to become ready.")
	}

	// wait for completion
	<-asserter.Done

	return nil
}
