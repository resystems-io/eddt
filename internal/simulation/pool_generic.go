package simulation

import (
	"fmt"
	"sync"
)

// (Note, we rely on the concrete testing of IMEIPool to test this logic.)

// Provides access to a pool of values.
//
// Values can be acquired or leased.
type GenericPool[T comparable] struct {
	// The total number of available values set for the pool.
	//
	// Note, if zero, then the pool is unlimited.
	Limit int

	// Kind managed by the pool
	Kind string

	mutex  sync.Mutex
	issued int
	visited map[T]bool
	free []T
}

type Pool interface {
	Size() int
	Issued() int
	Cap() int
}

type RandomGeneric[T comparable] func() T

// Issued provides the number of unique values that are currently issued by the pool.
func (p *GenericPool[T]) Issued() int {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.init()
	return p.issued
}

func (p *GenericPool[T]) init() {
	if p.visited == nil {
		p.visited = make(map[T]bool)
	}
	if p.free == nil {
		p.free = make([]T,0,128)
	}
}

func (p *GenericPool[T]) Size() int {
	return p.Issued()
}

func (p *GenericPool[T]) Cap() int {
	return p.Limit
}

// Acquire provide a valid entry from the pool.
//
// The pool will return an error if the size limit is reached.
// The pool will generate a new random value if needed.
// The pool will never issue the same value twice at the same time.
// The pool may reissue the same value if it was previously released.
//
// Acquire is thread-safe.
func (p *GenericPool[T]) AcquireWithGenerator(generator RandomGeneric[T]) (T, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.init()

	if p.Limit > 0 && p.issued >= p.Limit {
		var zero T
		return zero, fmt.Errorf("%s pool limit [%d] reached", p.Kind, p.Limit)
	}

	issue_new := func() T {
		for {
			v := generator()
			if !p.visited[v] {
				p.visited[v] = true
				p.issued++
				return v
			}
		}
	}

	if p.Limit == 0 {
		// unbounded... so just find a new unique value
		v := issue_new()
		return v, nil
	} else {
		if len(p.visited) < p.Limit {
			// we have not yet reached the full coverage so issue a new value
			v := issue_new()
			return v, nil
		} else {
			// we can't issue any new values so we need to find a free value
			if len(p.free) == 0 {
				var zero T
				return zero, fmt.Errorf("%s pool limit [%d] reached (non free)", p.Kind, p.Limit)
			}
			l := len(p.free)-1
			v := p.free[l]
			p.free = p.free[:l]
			return v, nil
		}
	}
}

// Release previously acquired value back to the pool.
//
// The pool will return an error the value was not previous issued.
//
// Release is thread-safe.
func (p *GenericPool[T]) Release(v T) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.init()

	if !p.visited[v] {
		return fmt.Errorf("%s %v not found in pool", p.Kind, v)
	}

	if p.Limit == 0 {
		p.issued--
		delete(p.visited, v)
	} else {
		p.issued--
		p.visited[v] = false
		p.free = append(p.free, v)
	}

	return nil
}
