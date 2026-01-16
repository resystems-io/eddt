package relate

import (
	"log"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"

	"go.resystems.io/eddt/contract"
	"go.resystems.io/eddt/internal/common"
)

func TestCompile(t *testing.T) {
	// start a NATS test server
	s := test.RunRandClientPortServer()
	defer s.Shutdown()

	// NATS connection
	nc, err := nats.Connect(s.ClientURL())
	// nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	t.Run("compile-one", func(t *testing.T) {

		// Compiler rule for the test
		rule := contract.CompilerRule{
			Match:            "resys.sol.test.obs.>",
			TTL:              time.Second * 10,
			TTLQuantisation:  time.Second,
			SourceType:       "foo",
			DestinationType:  "bar",
			SourceToken:      5,
			DestinationToken: 7,
		}

		// Create a test message
		msg := nats.NewMsg("resys.sol.test.obs.foo.source_id.bar.destination_id")

		// Create a channel to receive assertions
		assertionCh := make(chan *nats.Msg, 10)

		// Create a mock assertion subscriber
		_, err = nc.ChanSubscribe("resys.sol.*.r.assert.>", assertionCh)
		if err != nil {
			t.Errorf("Failure: %v", err)
		}

		// Create the compiler
		compiler := &RelationCompiler{
			NC:     nc,
			Logger: log.New(common.NewTestWriter(t), "[COMPILER-TESTLOG]: ", log.LstdFlags),
			Rules:  []contract.CompilerRule{rule},
		}

		// Launch the compiler
		end := make(chan struct{})
		ready := make(chan struct{})
		go compiler.Launch(end, ready)
		select {
		case <-ready:
		case <-time.After(time.Second * 2):
			t.Fatal("Timed out waiting for compiler ready")
		}

		// Publish the test message
		err = nc.PublishMsg(msg)
		if err != nil {
			t.Errorf("Failure: %v", err)
		}

		// Wait for the assertion
		select {
		case assertionMsg := <-assertionCh:
			// TODO: Add assertion validation
			t.Logf("Received assertion: [%d] %v", len(assertionMsg.Data), assertionMsg.Header)
		case <-time.After(time.Second * 2):
			t.Fatal("Timed out waiting for assertion")
		}

		// Shutdown the compiler
		close(end)
		<-compiler.Done
	})
}
