package simulation

import (
	"fmt"
)

// BlockClock - this simulation element waits until manually released.
//
// This can be used to pause and resume a simulation.
type BlockClock struct {
	Done <-chan struct{}

	state chan bool
}

const (
	PAUSED  = false
	RUNNING = true
)

func (s *BlockClock) Pause() {
	// Note, we explicitly write in a 'false' which will cause a "spurious" wake-up,
	// however, after wake-up the pause will not be republished internally.
	s.Set(PAUSED)
}

func (s *BlockClock) Resume() {
	s.Set(RUNNING)
}

func (s *BlockClock) Set(running bool) {
	// check that we haven't completed
	select {
	case <-s.Done:
		panic(fmt.Sprintf("Set [%v] after finished.", running))
	default:
	}
	for {
		// drain and discard the current state
		select {
		case <-s.state:
		default:
		}
		// write in a running state
		select {
		case s.state <- running:
			// updated
			return
		default:
			// would block - someone else filled the slot... try again
		}
	}
}

func (s *BlockClock) Run(simulation *Clock, ready chan<- struct{}, end <-chan struct{}) {
	session := simulation.Attach()
	done := make(chan struct{})
	s.Done = done

	// state holds a single boolean
	// when true we write it back and allow the simulation to continue
	// when false we leave the state channel empty
	s.state = make(chan bool, 1)

	// Let the simulation know we are up and running.
	defer close(ready)

	go func() {
		defer session.Close()
		defer close(done)

	process:
		for {
			select {
			case <-end:
				break process

			case epoch, ok := <-session.E:
				if !ok {
					// The simulation is over.
					return
				}

			wait:
				for {
					// monitor the state and hang if paused
					select {
					// fetch the state
					case running := <-s.state:
						if running {
							// end the epoch
							epoch.Close()
							// write the state back for the next epoch
							select {
							case s.state <- running:
							default:
								// someone else filled the slot
							}
							break wait
						}
					case <-end:
						break process
					}
				}
			}
		}
	}()
}
