package nats

import (
	"testing"
	"time"

	nats_server "github.com/nats-io/nats-server/v2/server"
	nats_test "github.com/nats-io/nats-server/v2/test"
)

// RunRandClientPortJSServer starts an embedded NATS server with JetStream enabled.
//
// The server starts on a random port, and the disk storage uses a temporary directory.
//
// ``` go
//
//	// Set up a NATS server in-memory.
//	s := RunRandClientPortJSServer(t)
//	defer s.Shutdown()
//
//	// Create a NATS client connection
//	t.Logf("Connecting to NATS: %v", s.ClientURL())
//	nc, err := nats.Connect(s.ClientURL())
//	assert.NoError(t, err)
//	defer nc.Close()
//
// ```
func RunRandClientPortJSServer(t *testing.T) *nats_server.Server {
	opts := nats_test.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	s := nats_test.RunServer(&opts)

	if !s.ReadyForConnections(4 * time.Second) {
		t.Fatalf("NATS server did not start in time")
	}

	return s
}
