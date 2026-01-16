package route

import (
	"log"
	"sync"

	"github.com/nats-io/nats.go"

	"go.resystems.io/eddt/internal/common"
	"go.resystems.io/eddt/contract"
)

// DomainRouter reroutes domain notifications by applying subject expansion or elision relative to relation-sets.
//
// See F5 of the functional decomposition.
type DomainRouter struct {
	Done   <-chan struct{}
	Logger *log.Logger
	NC     *nats.Conn

	Relations RelationSetSource
	Routes    chan<- contract.Route

	// Define the shared NATS queue group when multiple routers are running.
	//
	// Leave blank if the router should not join a group.
	Group string

	// -- internals

	active_routes sync.Map // map[RouteID]*route_state
}

type route_registration struct {
	route    *contract.Route
	compiled *CompiledRoute
	sub      *nats.Subscription
}

func (r *DomainRouter) get_route(id contract.RouteID) (*route_registration, bool) {
	v, ok := r.active_routes.Load(id)
	if !ok {
		return nil, false
	}
	vr, ok := v.(*route_registration)
	return vr, ok
}

func (r *DomainRouter) remove_route(id contract.RouteID) bool {
	_, ok := r.active_routes.LoadAndDelete(id)
	return ok
}

// Launch runs the follower in the background and tracks relation sets.
func (r *DomainRouter) Launch(end <-chan struct{}, ready chan<- struct{}) error {
	// set up a logger
	if r.Logger == nil {
		r.Logger = common.NewLogger("[ROUTER]: ")
	}
	defer close(ready)

	// coordination
	done := make(chan struct{})
	r.Done = done
	routes := make(chan contract.Route, 3)
	r.Routes = routes
	var subscriptions_complete sync.WaitGroup

	// drain and log errors
	// we add a buffer to errors to reduce likelihood of deadlock on shutdown
	// (ideally we should wait for all components to finish before stopping the error drainer)
	errors := make(chan error, 100)
	common.DrainAndLogErrors("A router", end, errors, r.Logger)

	// create handlers
	process_factory := func(rr *route_registration) (func(*nats.Msg), func(string)) {
		process := func(m *nats.Msg) {
			// handle incoming messages to be routed
			expanded, err := rr.compiled.execute(m.Subject, r.Relations)
			if err != nil {
				r.Logger.Printf("route expansion for <%s>: %v", m.Subject, err)
			}
			for _, s := range expanded {
				// forward the message to the expanded subjects
				mm := nats.Msg{
					Subject: s,
					Header:  m.Header,
					Data:    m.Data,
				}
				err := r.NC.PublishMsg(&mm)
				if err != nil {
					r.Logger.Printf("route publication failed to <%s>: %v", s, err)
				}
				if false {
					r.Logger.Printf("rerouted [%s] [%s] → [%s]", rr.route.ID, m.Subject, s)
				}
			}
		}

		closer := func(subj string) {
			r.Logger.Printf("route subscription closed %s", rr.route.ID)
			subscriptions_complete.Done()
		}

		return process, closer
	}

	// processing incoming routes
	// (record active routes, compile the route, subscribe the match)
	reconfigure_complete := make(chan struct{})
	go func() {
		defer close(reconfigure_complete)
	reconfigure:
		for {
			select {
			case <-end:
				// stop all the active subscriptions
				r.active_routes.Range(func(k, v any) bool {
					vr, ok := v.(*route_registration)
					if !ok {
						r.Logger.Printf("Unable to drain bad route type [%v]: %T", k, v)
					}
					vr.sub.Drain()
					return true
				})
				r.active_routes.Clear()
				break reconfigure
			case route := <-routes:
				r.Logger.Printf("Updating router [%v]", route)
				if route.Disabled {
					// remove disabled routes
					rs, ok := r.get_route(route.ID)
					if !ok {
						r.Logger.Printf("Unable to disable non-existent route: %v", route.ID)
						continue reconfigure
					}
					rs.sub.Drain()
					r.remove_route(route.ID)
				} else {
					// compile the incoming route
					compiled, err := Compile(&route)
					if err != nil {
						r.Logger.Printf("Unable to compile route [%v] <%v>: %v", route.ID, route, err)
						continue reconfigure
					}

					// create a new publication
					incoming_route := &route_registration{
						route:    &route,
						compiled: compiled,
						sub:      nil,
					}

					// subscribe with the new route
					route_handler, close_handler := process_factory(incoming_route)
					sub, err := r.NC.QueueSubscribe(route.Match, r.Group, route_handler)
					if err != nil {
						r.Logger.Printf("Unable to subscribe relative to route [%v] <%v>: %v", route.ID, route, err)
						continue reconfigure
					}
					incoming_route.sub = sub
					subscriptions_complete.Add(1)
					sub.SetClosedHandler(close_handler)

					// install the new route handling
					outgoing_route_any, previously_loaded := r.active_routes.Swap(route.ID, incoming_route)
					if previously_loaded {
						// close down the old route subscription
						if outgoing_route_any == nil {
							r.Logger.Printf("Previously loaded route registration was nil %v", route.ID)
							continue reconfigure
						}
						outgoing_route, ok := outgoing_route_any.(*route_registration)
						if !ok {
							r.Logger.Printf("Previously loaded route registration was the wrong type %v: %T", route.ID, outgoing_route_any)
							continue reconfigure
						}
						outgoing_route.sub.Drain()
					}
				}
				continue reconfigure
			}
		}
	}()

	// wait for the completion of all subscribers
	go func() {
		// signal completion when the follower returns
		defer close(done)

		// wait for the reconfigure handler to complete
		<-reconfigure_complete
		// waiting for all the route subscribers to complete
		subscriptions_complete.Wait()
	}()

	return nil
}
