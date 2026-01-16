package simulation

import (
	"math"
	"time"
)

// WallClock - this simulation element always waits for a fraction of wall-clock time.
//
// This can be used to speed up, or slow down your simulation.
type WallClock struct {
	// Values greater than 1.0 will cause the simulation to run faster than wall-clock time if possible.
	// Values less than 1.0 will cause the simulation to run slower than wall-clock time if possible.
	// For convenience, a value of zero 0.0 will set the speed to 1.0.
	Speed float64
}

func (s *WallClock) Run(simulation *Clock, ready chan<- struct{}, end <-chan struct{}) {
	// Let the simulation know we are up and running.
	defer close(ready)

	session := simulation.Attach()

	// Configure the default speed.
	speed := s.Speed
	if speed == 0.0 {
		speed = 1.0
	}

	go func() {
		defer session.Close()
		var ticker *time.Ticker

		for {
			select {
			case <-end:
				// The simulation is over.
				if ticker != nil {
					ticker.Stop()
				}
				return

			case epoch, ok := <-session.E:
				if !ok {
					// The simulation is over.
					if ticker != nil {
						ticker.Stop()
					}
					return
				}

				if ticker == nil {
					// Create a ticker that will fire at a rate that is synchronised with the epoch window
					// and scaled by the speed of the wall-clock.
					scaled := float64(epoch.EpochWindow.Duration) / speed
					ticker = time.NewTicker(time.Duration(math.Ceil(scaled)))
				}

				// Wait for the epoch to be released by the wall-clock.
				select {
				case <-end:
				case <-ticker.C:
				}

				// Signal that we are done with the epoch.
				epoch.Close()
			}
		}
	}()
}
