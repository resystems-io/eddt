package common

import (
	"bytes"
	"sync"
	"testing"
)

// The custom writer that redirects output to t.Log
type TestWriter struct {
	mu  sync.Mutex
	log func(args ...any)
}

// NewTestWriter creates a new TestWriter instance.
func NewTestWriter(t *testing.T) *TestWriter {
	// We wrap t.Log so that our Write method can be safely called
	// in a concurrent environment, as t.Log is not goroutine-safe.
	return &TestWriter{
		log: t.Log,
	}
}

// The Write method that satisfies the io.Writer interface.
func (tw *TestWriter) Write(p []byte) (int, error) {
	// The log.Logger will call this method with a byte slice.
	// We simply pass the string to t.Log.
	tw.mu.Lock()
	defer tw.mu.Unlock()
	tw.log(string(bytes.TrimSpace(p)))
	return len(p), nil
}
