package simulation

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

// Simulation Clock - used to receive simulation epochs so as to
// coordinate a complete simulation that spans many elements.
//
// Each epoch defines a given window of simulation time.
// The simulation element must generate all its events
// for the given window, and then close the epoch.
//
// Once all simulation elements have closed their epoch
// the clock will release the next window.
//
// The simulation clock can also choose to tie the simulation
// clock to wall clock, should the simulation be expected to
// run at a realistic pace.
//
// Similarly, the simulation clock can choose to only release
// epochs when other coordinating simulation instances have
// also concluded the epoch on their side. In this way we will
// be able to run a distributed simulation if needed.
type Clock struct {
	Span       SimulationSpan
	WindowSize time.Duration
	Done       <-chan struct{}

	// mu protects the sessions map
	mu sync.Mutex
	// sessions tracks all attached simulation elements
	sessions map[int]*Session
	// nextID is the next available ID for a session
	nextID int
	// epoch and session logger
	simulationLogger *log.Logger
}

// SimulationSpan represents a span of time in the simulation.
//
// Generally this will be interpreted as a half-open range [start, end).
type SimulationSpan struct {
	// The simulation time at the start of the window.
	Start time.Time
	// The size of the simulation window (zero if unbounded).
	Duration time.Duration
}

// End the first point in time after the given span.
func (s *SimulationSpan) End() time.Time {
	return s.Start.Add(s.Duration)
}

// Logger sets the logger used by the simulation
func (c *Clock) Logger(l *log.Logger) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if l == nil {
		panic("nil logger")
	}
	c.simulationLogger = l
}

func (c *Clock) init_logger() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.simulationLogger == nil {
		c.simulationLogger = log.New(os.Stderr, "[SIMULATION]: ", log.LstdFlags)
	}
}

func (c *Clock) Run(ready chan<- struct{}, end <-chan struct{}) {
	c.init_logger()
	done := make(chan struct{})
	c.Done = done
	// Signal ready once we have started to create epochs
	defer close(ready)
	c.simulationLogger.Printf("Simulation Clock - starting...")

	close_all := func() {
		// If the end channel is closed, shut down the clock.
		c.mu.Lock()
		// Close all session channels to signal to the elements that the simulation is over.
		for _, session := range c.sessions {
			close(session.e)
		}
		// Clear the sessions map.
		c.sessions = make(map[int]*Session)
		c.mu.Unlock()
	}

	go func() {
		defer close(done)

		// clean up and close attached sessions
		defer close_all()

		// tick is the simulation tick counter
		var tick int64
	epochs:
		for {
			select {
			case <-end:
				c.simulationLogger.Printf("Simulation Clock - ending...")
				break epochs
			default:
				// Create the epoch for the current tick.
				start := c.Span.Start.Add(time.Duration(tick) * c.WindowSize)
				if start.After(c.Span.End()) {
					c.simulationLogger.Printf("Simulation Clock - epoch [%v] reached end.", tick)
					break epochs
				}

				c.mu.Lock()
				// The current number of sessions determines the size of the barrier.
				numSessions := len(c.sessions)
				// The barrier is a buffered channel that acts as a semaphore.
				barrier := make(chan struct{}, numSessions)

				epoch := Epoch{
					Log:              c.simulationLogger,
					Tick:             tick,
					SimulationWindow: c.Span,
					EpochWindow: SimulationSpan{
						Start:    start,
						Duration: c.WindowSize,
					},
					barrier: barrier,
				}
				c.simulationLogger.Printf("Simulation Clock - epoch [%v] starting [%d]...", epoch, numSessions)

				// Publish the epoch to all attached sessions.
				for _, session := range c.sessions {
					session.e <- epoch
				}
				c.mu.Unlock()

				// Wait for all sessions to complete their work for the epoch.
			wait:
				for range numSessions {
					select {
					case <-end:
						// If we need to end, then drop out of waiting, but continue to the top to reach end logic.
						continue epochs
					case <-barrier:
						continue wait
					}
				}
				c.simulationLogger.Printf("Simulation Clock - epoch [%v] completed.", epoch)
				// Increment the tick counter.
				tick++
			}
		}
		c.simulationLogger.Printf("Simulation Clock - completed %d ticks.", tick)
	}()
}

// Attach - used to register an element with the simulation clock.
//
// Each simulation element attaches to the clock and is provided a session via which
// it coordinates with the clock and other elements. The simulation clock will publish
// an `Epoch` to each of the elements Attached at the start of the new epoch. Any elements
// that attach during a given epoch will only be enrolled into the next epoch.
//
// All sessions must complete and close their respective `Epoch` before the simulation
// clock can proceed with generate an new `Epoch`.
func (c *Clock) Attach() *Session {
	c.init_logger()
	c.mu.Lock()
	defer c.mu.Unlock()

	// Initialize the sessions map if it's nil.
	if c.sessions == nil {
		c.sessions = make(map[int]*Session)
	}

	// Create a new session.
	session := &Session{
		Log:   c.simulationLogger,
		id:    c.nextID,
		clock: c,
		// e is the channel on which the clock sends epochs to the session.
		e:    make(chan Epoch, 1),
		done: make(chan struct{}),
	}
	// Assign the public channel E.
	session.E = session.e

	// Add the session to the sessions map.
	c.sessions[c.nextID] = session
	c.nextID++

	return session
}

// -- sessions

// Simulation Session - provides a source of simulation epochs, and the mechanisms by which
// to coordinate with the rest of the simulation.
type Session struct {
	// Logger for simulation elements
	Log *log.Logger

	// E is the channel on which the clock sends epochs to the session.
	E <-chan Epoch

	// id is the unique identifier for the session.
	id int
	// clock is a reference to the simulation clock.
	clock *Clock
	// e is the internal channel for the clock to send epochs.
	e chan Epoch
	// done is the internal channel that signals when the session is closed
	done chan struct{}
}

// Close - explicitly end the session and detach from the simulation clock.
func (s *Session) Close() error {
	s.clock.mu.Lock()
	defer s.clock.mu.Unlock()

	// Note, we do not close the internal channel when closed from outside

	// Remove the session from the sessions map.
	delete(s.clock.sessions, s.id)
	close(s.done)

	return nil
}

// -- epochs

// Simulation Epoch - defines a window in time for which the simulation element
// must generate simulation events. Once all events are generated the element
// must close the epoch.
type Epoch struct {

	// Log provides a logger to be used by a simulation element.
	Log *log.Logger

	// Tick is incremented each time a new epoch starts.
	Tick int64

	// The window of time spanning the entire simulation.
	// (Note, it an unbounded simulation is indicated by a zero duration value.)
	SimulationWindow SimulationSpan

	// The window of time spanning this epoch in the simulation.
	// (Note, is all epochs must have a finite non-zero duration value.)
	EpochWindow SimulationSpan

	// barrier is the channel used to signal that the element has finished its computation for the epoch.
	barrier chan<- struct{}
}

// Close - signal that the element has finished its computation for the epoch.
func (e *Epoch) Close() error {
	// Send a value to the barrier channel to signal completion.
	e.barrier <- struct{}{}
	return nil
}

// String - return a user friendly string for the epoch
func (e Epoch) String() string {
	return fmt.Sprintf(
		"Epoch[tick=%d, window-start=%v, window-duration=%v]",
		e.Tick,
		e.EpochWindow.Start,
		e.EpochWindow.Duration,
	)
}
