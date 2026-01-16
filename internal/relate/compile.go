package relate

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"log"
	"sync"

	"github.com/nats-io/nats.go"

	"go.resystems.io/eddt/contract"
	"go.resystems.io/eddt/internal/common"
)

// Example observations subjects
//
// resys.sol.test-solution.obs.ip.AQEBAQ==.ip.Cqmzgg==.packet
// resys.sol.%s.obs.3gpp.imei.%s.create
// resys.sol.%s.obs.3gpp.imei.%s.dispose
// resys.sol.%s.obs.3gpp.imei.%s.imsi.%s.attach
// resys.sol.%s.obs.3gpp.imei.%s.imsi.%s.detach
// resys.sol.%s.obs.3gpp.imsi.%s.ip.%s.session.create
// resys.sol.%s.obs.3gpp.imsi.%s.ip.%s.session.dispose
// resys.sol.%s.obs.3gpp.imsi.%s.flow.create
// resys.sol.%s.obs.3gpp.imsi.%s.flow.dispose
// resys.sol.%s.obs.ip.%s.ip.%s.packet

// Assertion subject.
//
// Note, we include the source and destination types. This enables the options sharding related to the underlying K-V
// bucket in the cases that we need a separate function instance to manage the relation sets for different types of
// relations.
const ASSERTION_SUBJECT_PATTERN = "resys.sol.%s.r.assert.%s.%s"

// RelationCompiler monitoring observation subjects and generates assertions.
type RelationCompiler struct {
	Done <-chan struct{}
	NC   *nats.Conn

	Rules []contract.CompilerRule // TODO change to a channel so that rules can be configured at runtime

	Logger *log.Logger

	sol_cache sync.Map // Note, this cache is logically unbounded and will grow with the number of solutions
}

func (c *RelationCompiler) Launch(end <-chan struct{}, ready chan<- struct{}) error {
	// set up a logger
	if c.Logger == nil {
		c.Logger = common.NewLogger("[COMPILER]: ")
	}

	// coordination
	done := make(chan struct{})
	c.Done = done
	defer close(ready)
	errors := make(chan error)

	// track the compiler completion
	var finwg sync.WaitGroup
	var readywg sync.WaitGroup
	finwg.Add(len(c.Rules))
	readywg.Add(len(c.Rules))

	// wait for the group the complete
	go func() {
		defer close(done)
		finwg.Wait()
	}()

	// drain and log errors
	common.DrainAndLogErrors("A compiler", end, errors, c.Logger)

	// simply launch a compiler gorouting for each rule
	for _, r := range c.Rules {
		go func() {
			defer finwg.Done()
			// wait for the each compiler to be ready
			cr := make(chan struct{})
			go func() {
				select {
				case <-end:
				case <-cr:
					readywg.Done()
				}
			}()
			err := c.compile(end, cr, errors, r)
			if err != nil {
				c.Logger.Printf("Compiler %v failed: %v", r, err)
			}
		}()
	}

	// wait for all the compilers to start
	readywg.Wait()
	c.Logger.Printf("Compilers started: %d", len(c.Rules))

	return nil
}

// compile - starts a subscription and compiles assertions for a single rule.
//
// Returns once the subscription is terminated.
func (c *RelationCompiler) compile(end <-chan struct{}, ready chan<- struct{}, errors chan<- error, rule contract.CompilerRule) error {

	default_buflen := 100
	bootstrap := default_buflen

	// obtain the precomputed assertion subject
	out := func(sid string, st, dt string) string {
		subj, ok := c.sol_cache.Load(sid)
		if !ok {
			subj = fmt.Sprintf(ASSERTION_SUBJECT_PATTERN, sid, st, dt)
			c.sol_cache.Store(sid, subj)
		}
		if s, ok := subj.(string); ok {
			return s
		} else {
			return ""
		}
	}

	process := func(m *nats.Msg) {

		// apply the rule and create a flatbuffer
		sid, buf, err := subject_to_assertion(&rule, m.Subject, bootstrap)
		if err != nil {
			errors <- err
			return
		}

		bootstrap = max(default_buflen, len(buf))

		// compute the SHA1
		// (note, the hash will only change if the relationship parties are different
		//  or if the expiry moves to the next quantisation boundary.)
		sum := sha1.Sum(buf)

		// compute the message dedup token
		dedup := hex.EncodeToString(sum[:])

		// compute the assertion subject
		assertions := out(sid, string(rule.SourceType), string(rule.DestinationType))

		// generate an assertion message
		msg := nats.NewMsg(assertions)
		msg.Header.Set(nats.MsgIdHdr, dedup)
		msg.Data = buf

		// publish the message
		err = c.NC.PublishMsg(msg)
		if err != nil {
			errors <- err
		}
	}

	// Subscribe to NATS using the match filter.
	// (Note, we are not using a channel subscription, so NATS will create a go routine for us.)
	sub, err := c.NC.Subscribe(rule.Match, process)
	if err != nil {
		return err
	}
	c.Logger.Printf("Rule subscribed: [%s]", rule.Match)
	complete := make(chan struct{})
	sub.SetClosedHandler(func(subj string) {
		c.Logger.Printf("Rule subscribed drained/closed: [%s]", subj)
		close(complete)
	})

	// Signal that this compiler is ready.
	close(ready)

	// Wait for end.
	//go func() {
	select {
	case <-end:
		c.Logger.Printf("Rule ending... [%s]", rule.Match)
		sub.Drain()
	}
	//}()

	// Wait for subscription completion.
	<-complete
	c.Logger.Printf("Rule ended [%s]", rule.Match)

	return nil
}
