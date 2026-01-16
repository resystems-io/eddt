package route_test

import (
	"context"
	"log"
	"sync"
	"testing"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"go.resystems.io/eddt/internal/common"
	"go.resystems.io/eddt/internal/common/assert"
	nats_help "go.resystems.io/eddt/internal/common/nats"
	"go.resystems.io/eddt/internal/relate"
	"go.resystems.io/eddt/internal/relate/relationset"
	"go.resystems.io/eddt/internal/route"
)

func TestRouterRelationFollower(t *testing.T) {
	// 1. Set up a NATS server in-memory.
	s := nats_help.RunRandClientPortJSServer(t)
	defer s.Shutdown()

	// Create a NATS client connection
	t.Logf("Connecting to NATS: %v", s.ClientURL())
	nc, err := nats.Connect(s.ClientURL())
	assert.NoError(t, err)
	defer nc.Close()

	// 2. Create the RELATIONSET_BUCKET key-value store.
	t.Logf("Connecting to jetstream")
	js, err := jetstream.New(nc)
	assert.NoError(t, err)
	ctx := context.Background()
	t.Logf("Creating bucket: %v", relate.RELATIONSET_BUCKET)
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: relate.RELATIONSET_BUCKET,
	})
	assert.NoError(t, err)
	if kv == nil {
		t.Fatalf("nil kv")
	}

	// 3. Create a RouterRelationFollower instance.
	follower := &route.RouterRelationFollower{
		NC:     nc,
		Logger: log.New(common.NewTestWriter(t), "[FOLLOWER-TESTLOG]: ", log.LstdFlags),
	}

	// 4. Launch the follower.
	end := make(chan struct{})
	defer close(end)
	ready := make(chan struct{})
	err = follower.Launch(end, ready)
	assert.NoError(t, err)
	<-ready // wait for follower to be ready

	// 5. Push some RelationSet objects to the bucket.
	wg := sync.WaitGroup{}
	go func() {
		// track updates processed by the follower
		for {
			select {
			case <-end:
				break
			case r := <-follower.Updates:
				t.Logf("Follower update: %v", r)
				wg.Done()
			}
		}
	}()
	builder := flatbuffers.NewBuilder(1024)
	key1 := "ue-1"
	relationset.RelationSetStart(builder)
	rset1 := relationset.RelationSetEnd(builder)
	builder.Finish(rset1)
	rset1_bytes := builder.FinishedBytes()
	// Put the relationsets into the key-value store
	wg.Add(1)
	_, err = kv.Put(ctx, key1, rset1_bytes)
	assert.NoError(t, err)

	builder.Reset()

	key2 := "ue-2"
	relationset.RelationSetStart(builder)
	rset2 := relationset.RelationSetEnd(builder)
	builder.Finish(rset2)
	rset2_bytes := builder.FinishedBytes()
	// Put the relationsets into the key-value store
	wg.Add(1)
	_, err = kv.Put(ctx, key2, rset2_bytes)
	assert.NoError(t, err)

	// Allow some time for the watcher to process the updates
	// time.Sleep(100 * time.Millisecond)
	processed := make(chan struct{})
	go func() {
		wg.Wait()
		close(processed)
	}()
	select {
	case <-processed:
	case <-time.After(5 * time.Second):
		t.Errorf("Timeout while waiting for the follower to receive relationset updates")
	}

	// 6. Use the Get method to retrieve the values and assert they are correct.
	retrievedRset1, ok := follower.Get(key1)
	t.Logf("Key [%s] retrieved: %v", key1, ok)
	assert.True(t, ok)
	assert.NotNil(t, retrievedRset1)
	t.Logf("Key [%s] set size: %d", key1, retrievedRset1.RLength())

	retrievedRset2, ok := follower.Get(key2)
	t.Logf("Key [%s] retrieved: %v", key2, ok)
	assert.True(t, ok)
	assert.NotNil(t, retrievedRset2)
	retrievedRset1.RLength()
	t.Logf("Key [%s] set size: %d", key2, retrievedRset2.RLength())

	// 7. Test Get with a non-existent key.
	non_existent := "non-existent-key"
	var noRset *relationset.RelationSet = nil
	if noRset == nil {
		t.Logf("noRset == nil → true")
	}
	if noRset != nil {
		t.Logf("noRset != nil → true")
	}
	assert.Nil(t, noRset)
	noRset, ok = follower.Get(non_existent)
	t.Logf("Key [%s] retrieved: %v", non_existent, ok)
	assert.False(t, ok)
	assert.Nil(t, noRset)
}
