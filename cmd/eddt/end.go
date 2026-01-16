package main

import (
	"os"
	"os/signal"
	"syscall"
)

func end_on_interrupt() chan struct{} {
		// end on Ctrl-C or terminate
		end := make(chan struct{})
		signalChan := make(chan os.Signal, 1)
		signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-signalChan
			close(end)
		}()

		return end
}
