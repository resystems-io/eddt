package common

import (
	"log"
	"os"
)

// set up a EDDT logger
func NewLogger(prefix string) *log.Logger {
	return log.New(os.Stderr, prefix, log.LstdFlags)
}

// drain and log errors
func DrainAndLogErrors(name string, end <-chan struct{}, errs <-chan error, l *log.Logger) {
	go func() {
		for {
			select {
			case <-end:
				return
			case err := <-errs:
				l.Printf("%s glitched: %v", name, err)
			}
		}
	}()
}
