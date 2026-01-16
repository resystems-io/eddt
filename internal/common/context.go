package common

import (
	"context"
)

// EndContext creates a background context that is canceled when the `end` is signaled.
func EndContext(end <-chan struct{}) context.Context {
	// create a local context that is controlled by 'end'
	ctx := context.Background()
	eCtx, lCancel := context.WithCancel(ctx)
	go func() {
		<-end
		lCancel()
	}()
	return eCtx
}
