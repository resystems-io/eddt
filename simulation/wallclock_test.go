package simulation

import (
	"io"
	"log"
	"testing"
	"time"
)

func TestWallClock(t *testing.T) {
	// Create an ender channel for the simulation.
	end := make(chan struct{})

	// Create a new clock with a small epoch window for testing.
	clock := &Clock{
		WindowSize: time.Millisecond * 10,
		Span: SimulationSpan{
			Start:    time.Now(),
			Duration: time.Second * 1,
		},
	}
	if !testing.Verbose() {
		devnull := log.New(io.Discard, ">", log.LstdFlags)
		clock.Logger(devnull)
	}

	// Run the clock for a few epochs.
	clockReady := make(chan struct{})
	clock.Run(clockReady, end)
	<-clockReady

	t.Logf("Simulation Clock ready.")

	// Create a wall clock with a speed of 10.0 (10x faster than real-time).
	wallClock := &WallClock{Speed: 10.0}

	// Create channels for synchronization.
	wallClockReady := make(chan struct{})

	// Run the wall clock in a separate goroutine.
	go wallClock.Run(clock, wallClockReady, end)

	// Wait for the wall clock to be ready.
	<-wallClockReady
	t.Logf("Simulation WallClock ready.")

	// Attach a session to the clock to receive epochs.
	session := clock.Attach()
	defer session.Close()

	// Receive a few epochs and check the timing.
	for range 3 {
		select {
		case epoch, ok := <-session.E:
			if !ok {
				t.Fatal("Epoch channel closed unexpectedly")
			}
			epoch.Close()
		case <-time.After(time.Millisecond * 20):
			t.Fatal("Timed out waiting for epoch")
		}
	}

	// Close the end channel to stop the wall clock.
	close(end)
}
