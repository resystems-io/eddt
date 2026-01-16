package main

import (
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

type NATSConfig struct {
	URLS            string // server URLs (separated by commas)
	Creds           string // user credentials file
	NKey            string // NKey for seed file
	TLS_Client_Cert string // TLS auth client cert
	TLS_Client_Key  string // TLS auth client key
	TLS_CA_Cert     string // TLS CA cert
}

var nats_config NATSConfig

// Configure NATS based on built-in defaults
func setupNATSConnOptions(opts []nats.Option) []nats.Option {
	totalWait := 10 * time.Minute
	reconnectDelay := time.Second

	opts = append(opts, nats.ReconnectWait(reconnectDelay))
	opts = append(opts, nats.MaxReconnects(int(totalWait/reconnectDelay)))
	opts = append(opts, nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
		log.Printf("Disconnected due to:%s, will attempt reconnects for %.0fm", err, totalWait.Minutes())
	}))
	opts = append(opts, nats.ReconnectHandler(func(nc *nats.Conn) {
		log.Printf("Reconnected [%s]", nc.ConnectedUrl())
	}))
	opts = append(opts, nats.ClosedHandler(func(nc *nats.Conn) {
		log.Fatalf("Exiting: %v", nc.LastError())
	}))
	return opts
}

// Configure NATS based on command line options
func setupNATSConfigOptions(opts []nats.Option) []nats.Option {
	if false { // don't force auth
		if nats_config.Creds != "" && nats_config.NKey != "" {
			log.Fatal("specify -seed or -creds")
		}
	}

	// Use UserCredentials
	if nats_config.Creds != "" {
		opts = append(opts, nats.UserCredentials(nats_config.Creds))
	}

	// Use TLS client authentication
	if nats_config.TLS_Client_Cert != "" && nats_config.TLS_Client_Key != "" {
		opts = append(opts, nats.ClientCert(nats_config.TLS_Client_Cert, nats_config.TLS_Client_Key))
	}

	// Use specific CA certificate
	if nats_config.TLS_CA_Cert != "" {
		opts = append(opts, nats.RootCAs(nats_config.TLS_CA_Cert))
	}

	// Use Nkey authentication.
	if nats_config.NKey != "" {
		opt, err := nats.NkeyOptionFromSeed(nats_config.NKey)
		if err != nil {
			log.Fatal(err)
		}
		opts = append(opts, opt)
	}

	return opts
}
