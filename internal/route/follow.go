package route

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"go.resystems.io/eddt/internal/common"
	nats_helper "go.resystems.io/eddt/internal/common/nats"
	"go.resystems.io/eddt/internal/relate"
	"go.resystems.io/eddt/internal/relate/relationset"
)

type Relations struct {
	Key string
	Set *relationset.RelationSet
}

// RouterRelationFollower tracks the relation maps in the K-V store and keeps an in memory version available.
//
// This implements F4 in the functional decomposition.
type RouterRelationFollower struct {
	Done   <-chan struct{}
	Logger *log.Logger
	NC     *nats.Conn

	Updates <-chan Relations

	// relations holds *relationset.RelationSet flatbuffers keyed by the relevant domain twin identifier.
	relations sync.Map

	js      jetstream.JetStream
	kv_sets jetstream.KeyValue

	ender     <-chan struct{}
	ender_ctx context.Context
}

// Launch runs the follower in the background and tracks relation sets.
func (r *RouterRelationFollower) Launch(end <-chan struct{}, ready chan<- struct{}) error {
	// set up a logger
	if r.Logger == nil {
		r.Logger = common.NewLogger("[FOLLOWER]: ")
	}

	// coordination
	done := make(chan struct{})
	r.Done = done
	updates := make(chan Relations, 3)
	r.Updates = updates
	// create a local context for use with JetStream that is controlled by 'end'
	r.ender = end
	r.ender_ctx = common.EndContext(end)

	// drain and log errors
	// we add a buffer to errors to reduce likelihood of deadlock on shutdown
	// (ideally we should wait for all components to finish before stopping the error drainer)
	errors := make(chan error, 100)
	common.DrainAndLogErrors("A follower", end, errors, r.Logger)

	// connect to jetstream
	js, err := jetstream.New(r.NC)
	if err != nil {
		r.Logger.Printf("Failed to connect to JetStream: %v", err)
		return err
	}
	r.js = js

	// connect to the storage bucket
	kv, err := js.KeyValue(r.ender_ctx, relate.RELATIONSET_BUCKET)
	if err != nil {
		r.Logger.Printf("Failed to connect to KeyValue Bucket [%s]: %v", relate.RELATIONSET_BUCKET, err)
		return err
	}
	r.kv_sets = kv

	// simply run a follower
	go func() {
		// signal completion when the follower returns
		defer close(done)
		// follow relations
		r.follow(r.ender_ctx, end, ready, updates, errors)
	}()

	return nil
}

// Get returns the latest relation set for the given key.
//
// If there is a cache miss in the local map, an attempt is made to fetch
// the value from the underlying KV bucket.
//
// See: `RelationSetSource`
func (r *RouterRelationFollower) Get(key string) (*relationset.RelationSet, bool) {
	v, ok := r.relations.Load(key)
	if !ok {
		if false {
			r.Logger.Printf("cache miss for key '%s', fetching from bucket", key)
		}
		// miss detected, try doing an explicit fetch from the bucket
		entry, err := r.kv_sets.Get(r.ender_ctx, key)
		if err != nil {
			if nats_helper.NatsIsErrKeyNotFound(err) {
				// could be jetstream.ErrKeyNotFound, which is fine
				return nil, false
			} else {
				r.Logger.Printf("Error fetching missed key <%s>: %v", key, err)
				return nil, false
			}
		}

		// we got it, so absorb it, store it, and return it
		rset := relationset.GetRootAsRelationSet(entry.Value(), 0)
		r.relations.Store(key, rset)
		return rset, true
	}

	s, ok := v.(*relationset.RelationSet)
	return s, ok
}

// follow runs until 'end' and watches relationsets.
func (r *RouterRelationFollower) follow(
	nctx context.Context,
	end <-chan struct{},
	ready chan<- struct{},
	updates chan<- Relations,
	errors chan<- error,
) {

	// watch the relations K-V getting the latest version and watching for changes
	opts := make([]jetstream.WatchOpt, 0, 8)
	if false {
		// since we don't have "updates only" we don't really need to the full history... just the latest
		opts = append(opts, jetstream.IncludeHistory())
	}
	watcher, err := r.kv_sets.WatchAll(nctx, opts...)
	if err != nil {
		errors <- err
		return
	}

	defer func() {
		err := watcher.Stop()
		if err != nil {
			errors <- err
		}
	}()

	// signal that we are ready and watching
	close(ready)

	// process the K-V entries and update the map with wrapped flatbuffers
	initialised := false
process:
	for {
		select {
		case <-end:
			break process
		case ent := <-watcher.Updates():
			if ent == nil {
				if !initialised {
					initialised = true
					r.Logger.Printf("nil entry received after initial values")
				} else {
					r.Logger.Printf("nil entry received (ignoring)")
				}
				continue process
			}
			err := r.absorb(ent, updates)
			if err != nil {
				errors <- err
			}
		}
	}
}

// absorb records the relation set provided in the entry
func (r *RouterRelationFollower) absorb(
	ent jetstream.KeyValueEntry,
	updates chan<- Relations,
) error {

	if ent == nil {
		panic("nil key-value entry")
	}

	// simply pick up the payload and wrap the flatbuffer
	rkey := ent.Key()
	defer r.Logger.Printf("stored set for [%v]", rkey)
	if len(ent.Value()) == 0 {
		return fmt.Errorf("empty value for key [%s]", rkey)
	}
	rset := relationset.GetRootAsRelationSet(ent.Value(), 0)

	// TODO decide if we need to check the revision and perform CAS updates?
	r.relations.Store(rkey, rset)

	// echo the update to the update channel
	select {
	case updates <- Relations{rkey, rset}:
	default:
		// no reader... ignore
	}


	return nil
}
