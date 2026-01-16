package relate

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"go.resystems.io/eddt/internal/common"
	"go.resystems.io/eddt/internal/common/assert"
	nats_helper "go.resystems.io/eddt/internal/common/nats"
	"go.resystems.io/eddt/internal/relate/relationset"
)

const (
	TEST_ASSERTION_SUBJECT = "resys.sol.a.r.assert.b.c"
)

func TestRelationAsserter(t *testing.T) {
	// start a NATS test server
	s := test.RunRandClientPortServer()
	s.EnableJetStream(nil)
	defer s.Shutdown()

	// NATS connection
	const useExternalNATS = false
	var url string
	if useExternalNATS {
		t.Log("WARNING using external NATS for testing, rather than an embedded server.")
		url = nats.DefaultURL
	} else {
		url = s.ClientURL()
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	if !useExternalNATS {
		// nats kv add EDDT-R-SETS --history=10 --storage=file --ttl=24h --marker-ttl=1h --replicas=1 --max-value-size=100KiB --max-bucket-size=1GiB
		js, err := nc.JetStream()
		if err != nil {
			t.Fatalf("failed to get JetStream context: %v", err)
		}
		_, err = js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket:       RELATIONSET_BUCKET,
			Description:  "created by test",
			MaxValueSize: 100 * 1024,
			History:      10,
			TTL:          24 * time.Hour,
			MaxBytes:     1 * 1024 * 1024 * 1024,
			Replicas:     1,
			Storage:      nats.MemoryStorage, // overrides --storage=file
		})
		if err != nil {
			t.Fatalf("failed to create key-value store: %v", err)
		}

		// Enable AllowMsgTTL on the underlying stream to support per-key TTL assertions.
		sname := "KV_" + RELATIONSET_BUCKET
		si, err := js.StreamInfo(sname)
		if err != nil {
			t.Fatalf("failed to get stream info for %s: %v", sname, err)
		}
		cfg := si.Config
		cfg.AllowMsgTTL = true
		_, err = js.UpdateStream(&cfg)
		if err != nil {
			t.Fatalf("failed to update stream config for %s: %v", sname, err)
		}
	}

	// create a relation asserter
	asserter := &RelationAsserter{
		NC:     nc,
		Logger: log.New(common.NewTestWriter(t), "[ASSERTER-TESTLOG]: ", log.LstdFlags),
	}

	// launch the asserter
	ready := make(chan struct{})
	end := make(chan struct{})
	defer close(end)
	go func() {
		if err := asserter.Launch(end, ready); err != nil {
			t.Errorf("asserter failed to launch: %v", err)
		}
	}()
	<-ready

	// create a test assertion
	a1 := newAssertionT("sid", "st", "si", "dt", "di1", 100)
	buf := packAssertion(a1)

	// publish the assertion
	if err := nc.Publish(TEST_ASSERTION_SUBJECT, buf); err != nil {
		t.Fatalf("failed to publish assertion: %v", err)
	}

	// check the key-value store
	ctx := context.Background()
	kv, err := asserter.js.KeyValue(ctx, RELATIONSET_BUCKET)
	if err != nil {
		t.Fatalf("failed to get key-value store: %v", err)
	}

	// check that the set is updated
	key := "resys.sol.sid.r.kv.st.si.dt.set"

	// watch for a key change (to avoid needing to sleep)
	watcher, err := kv.Watch(ctx, key)
	if err != nil {
		t.Fatalf("failed to watch key %s: %v", key, err)
	}
	var entry jetstream.KeyValueEntry
	var ok bool
	timeout := time.After(5 * time.Second)
Loop:
	for {
		select {
		case entry, ok = <-watcher.Updates():
			if !ok {
				// Watcher channel closed unexpectedly.
				t.Log("Watcher channel closed. Attempting fallback verification.")
				break Loop
			} else if entry == nil {
				t.Log("Watcher received nil entry, ignoring...")
				continue Loop
			} else {
				t.Logf("Watcher received update for key %s, op: %v", entry.Key(), entry.Operation())
				break Loop
			}
		case <-timeout:
			t.Errorf("timeout waiting for relation set to update")
			break Loop
		}
	}

	// Double check with direct Get if we didn't get a valid entry from watcher
	if entry == nil {
		var err error
		entry, err = kv.Get(ctx, key)
		t.Logf("Is key not found error: %v", nats_helper.NatsIsErrKeyNotFound(err))
		if err != nil {
			t.Fatalf("failed to get key %s: %v", key, err)
		}
	}

	// check the value of the entry
	set := relationset.GetRootAsRelationSet(entry.Value(), 0)
	assert.EqualS(t, string(set.Sid()), "sid")
	assert.EqualS(t, string(set.St()), "st")
	assert.EqualS(t, string(set.Si()), "si")
	assert.EqualS(t, string(set.Dt()), "dt")
	assert.EqualI(t, set.RLength(), 1)

	var r relationset.Relation
	assert.True(t, set.R(&r, 0))

	assert.EqualS(t, string(r.Di()), "di1")
	assert.EqualI64(t, r.Ttl(), 100)
}
