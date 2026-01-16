package main

import (
	"time"

	"github.com/nats-io/nats.go"

	"go.resystems.io/eddt/contract"
	"go.resystems.io/eddt/internal/relate"
)

func RunObservationCompilations(rules []contract.CompilerRule, end <-chan struct{}) error {

	// connect to NATS
	nats_config.URLS = nats.DefaultURL

	opts := []nats.Option{nats.Name("EDDT - relationship compiler")}
	opts = setupNATSConnOptions(opts)
	opts = setupNATSConfigOptions(opts)

	nc, err := nats.Connect(nats_config.URLS, opts...)
	if err != nil {
		return err
	}

	// create the compiler
	compiler := &relate.RelationCompiler{
		NC:    nc,
		Rules: rules,
	}
	ready := make(chan struct{})
	go compiler.Launch(end, ready)

	// TODO we need to be able to reconfigure the compiler at runtime
	// (i.e. rules should be posted via rules->compiler.Profiles)

	// wait for ready
	select {
	case <-ready:
	case <-end:
	case <-time.After(time.Second * 5):
		panic("Timed out waiting for compiler to become ready.")
	}

	// wait for completion
	<-compiler.Done

	return nil
}
