package main

import (
	"encoding/json"
	"io"
	"os"
	"time"

	"github.com/nats-io/nats.go"

	"go.resystems.io/eddt/contract"
	"go.resystems.io/eddt/internal/relate"
)

type RelateCompilerConfig struct {
	RulesFile string
	Group     string
}

func (cfg *RelateCompilerConfig) LoadRules() ([]contract.CompilerRule, error) {
	// load rules from file
	var rules []contract.CompilerRule

	if relate_compiler_config.RulesFile != "" {
		jsonFile, err := os.Open(relate_compiler_config.RulesFile)
		if err != nil {
			return nil, err
		}
		defer jsonFile.Close()

		byteValue, err := io.ReadAll(jsonFile)
		if err != nil {
			return nil, err
		}
		json.Unmarshal(byteValue, &rules)
	}

	return rules, nil
}

func RunObservationCompilations(end <-chan struct{}, cfg RelateCompilerConfig) error {

	// fetch the rules
	rules, err := cfg.LoadRules()
	if err != nil {
		return err
	}

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
		Group: cfg.Group,
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
