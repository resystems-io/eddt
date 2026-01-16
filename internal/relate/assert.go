package relate

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	nats_helper "go.resystems.io/eddt/internal/common/nats"
	"go.resystems.io/eddt/internal/relate/assertion"
	"go.resystems.io/eddt/internal/relate/relationset"
)

// Example observations subjects
//
// resys.sol.%s.r.assert.*.*
// resys.sol.%s.r.assert.>

const (
	ASSERTION_SUBSCRIPTION = "resys.sol.*.r.assert.>"
	ASSERTION_RETRIES      = 5
	RELATIONSET_BUCKET     = "EDDT-R-SETS"
)

// RelationAsserter monitoring observation subjects and generates assertions.
type RelationAsserter struct {
	Done <-chan struct{}
	NC   *nats.Conn

	Logger *log.Logger

	Group string

	js      jetstream.JetStream
	kv_sets jetstream.KeyValue
}

func (c *RelationAsserter) Launch(end <-chan struct{}, ready chan<- struct{}) error {
	// set up a logger
	if c.Logger == nil {
		c.Logger = log.New(os.Stderr, "[ASSERTER]: ", log.LstdFlags)
	}
	c.Logger.Printf("Asserter starting...")

	// coordination
	done := make(chan struct{})
	c.Done = done
	errors := make(chan error)
	defer close(ready)

	// track the compiler completion
	var finwg, readywg sync.WaitGroup
	finwg.Add(1)
	readywg.Add(1)

	// wait for the FiN group the complete before signaling Done
	go func() {
		defer close(done)
		finwg.Wait()
	}()

	// connect to jetstream
	js, err := jetstream.New(c.NC)
	if err != nil {
		c.Logger.Printf("Failed to connect to JetStream: %v", err)
		return err
	}
	c.js = js

	// create a local context for use with JetStream that is controlled by 'end'
	jsCtx := context.Background()
	lCtx, lCancel := context.WithCancel(jsCtx)
	go func() {
		<-end
		lCancel()
	}()

	// connect to the storage bucket
	kv, err := js.KeyValue(lCtx, RELATIONSET_BUCKET)
	if err != nil {
		c.Logger.Printf("Failed to connect to KeyValue Bucket [%s]: %v", RELATIONSET_BUCKET, err)
		return err
	}
	c.kv_sets = kv

	// drain and log errors
	go func() {
		for {
			select {
			case <-end:
				return
			case err := <-errors:
				c.Logger.Printf("A asserter glitched: %v", err)
			}
		}
	}()

	// start an asserter
	for range 1 {
		go func() {
			defer finwg.Done()
			// wait for the asserter to be ready
			cready := make(chan struct{})
			go func() {
				select {
				case <-end:
				case <-cready:
					readywg.Done()
				}
			}()
			// run the asserter
			err := c.assert(jsCtx, end, cready, errors)
			if err != nil {
				c.Logger.Printf("Asserter failed: %v", err)
			}
		}()
	}
	c.Logger.Printf("Asserter started.")

	// wait for readywg before signalling Ready
	readywg.Wait()
	c.Logger.Printf("Asserter ready.")

	return nil
}

func (c *RelationAsserter) assert(ctx context.Context, end <-chan struct{}, ready chan<- struct{}, errs chan<- error) error {

	create := func(key string, a *assertion.Assertion) error {
		buf := kv_set_create(a)
		// The per-key TTL can only be set when the key is first created.
		// Therefore, we assume that the TTL will remain unchanged over the lifetime of a given key.
		// Note, as per nats-server/server/stream.go:parseMessageTTL(string) the TTL must be at least 1 second.
		ttl := time.Duration(a.Ttl()) * time.Millisecond
		var kvopts []jetstream.KVCreateOpt = make([]jetstream.KVCreateOpt, 0, 1)
		if ttl > 0 {
			if ttl < time.Second {
				// fmt.Printf("fixing small TTL=%d\n", ttl)
				ttl = 2 * time.Second
			}
			ttlop := jetstream.KeyTTL(ttl)
			kvopts = append(kvopts, ttlop)
		}
		_, err := c.kv_sets.Create(ctx, key, buf, kvopts...)
		return err
	}

	merge := func(key string, a *assertion.Assertion, entry jetstream.KeyValueEntry) error {
		// note the revision for checking later
		revision := entry.Revision()
		// bind to the relationset buffer
		set := relationset.GetRootAsRelationSet(entry.Value(), 0)
		// merge in the assertion (updating TTLs or removing expired entries)
		updated := kv_set_merge(a, set)
		// publish the relation set (using CAS) or retry
		_, err := c.kv_sets.Update(ctx, key, updated, revision)
		return err
	}

	// Process each assertion.
	assert := func(m *nats.Msg) {
		// bind to the assertion buffer
		assertion := assertion.GetRootAsAssertion(m.Data, 0)
		// build the kv key
		key := kv_set_key(assertion)

		// spin and retry in case there is contention
	cas:
		for range ASSERTION_RETRIES {
			// fetch the relation set for the source element
			entry, err := c.kv_sets.Get(ctx, key)
			if err != nil {
				if nats_helper.NatsIsErrKeyNotFound(err) {
					// create when not found
					err := create(key, assertion)
					if err != nil {
						errs <- fmt.Errorf("assert retrying after failing to create relation set [%s]: %w", key, err)
						continue cas
					}
					// create success
					return
				} else {
					// oops
					errs <- fmt.Errorf("assert failed to get value [%s]: [%T] %w", key, err, err)
					return
				}
			} else {
				// merge when found
				err := merge(key, assertion, entry)
				if err != nil {
					if nats_helper.NatsIsErrWrongLastSequence(err) {
						errs <- fmt.Errorf("assert retrying after conflict on value update [%s]: %w", key, err)
						continue cas
					}
					// oops
					errs <- fmt.Errorf("assert failed to update value [%s]: %w", key, err)
					return
				}
				// merge success
				return
			}
		}
		errs <- fmt.Errorf("assert failed to update value [%s]: too many retries", key)
		return
	}

	// Subscribe to NATS using the match filter.
	// (Note, we are not using a channel subscription, so NATS will create a go routine for us.)
	sub, err := c.NC.QueueSubscribe(ASSERTION_SUBSCRIPTION, c.Group, assert)
	if err != nil {
		errs <- err
		return err
	}
	c.Logger.Printf("Assertions subscribed: [%s]", ASSERTION_SUBSCRIPTION)
	complete := make(chan struct{})
	sub.SetClosedHandler(func(subj string) {
		c.Logger.Printf("Assertions subscription drained/closed: [%s]", subj)
		close(complete)
	})

	// Signal that this asserter is ready.
	c.Logger.Printf("Assertions ready: [%s]", ASSERTION_SUBSCRIPTION)
	close(ready)

	// Wait for end and drain.
	//go func() {
	select {
	case <-end:
		c.Logger.Printf("Assertions ending... [%s]", ASSERTION_SUBSCRIPTION)
		sub.Drain()
	}
	//}()

	// Wait for subscription completion.
	<-complete
	c.Logger.Printf("Assertions ended [%s]", ASSERTION_SUBSCRIPTION)

	return nil
}
