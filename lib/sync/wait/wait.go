package wait

import (
	"sync"
	"time"
)

// Wait is the extension of WaitGroup with timeout
type Wait struct {
	wg sync.WaitGroup
}

// Add adds delta which means the counter
func (w *Wait) Add(delta int) {
	w.wg.Add(delta)
}

// Done equals Add(-1)
func (w *Wait) Done() {
	w.wg.Done()
}

// Wait blocks until the WaitGroup counter is zero
func (w *Wait) Wait() {
	w.wg.Wait()
}

// WaitWithTimeout block until WaitGroup counter is zero or timeout
func (w *Wait) WaitWithTimeout(timeout time.Duration) bool {
	c := make(chan bool, 1)
	go func() {
		defer close(c)
		w.wg.Wait()
		c <- true
	}()
	select {
	case <-c:
		return false // complete normally
	case <-time.After(timeout):
		return true // timeout
	}
}
