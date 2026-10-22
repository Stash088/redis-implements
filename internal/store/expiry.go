package store

import (
	"log"
	"time"
)

// Expirer runs periodic active-expiration passes against a set of Stores
// (the server's databases).
type Expirer struct {
	stores   []*Store
	interval time.Duration
	stop     chan struct{}
	done     chan struct{}
}

// NewExpirer returns an Expirer that will run every interval until Stop is
// called, expiring the keys of every given store. The caller must call Start
// to begin.
func NewExpirer(stores []*Store, interval time.Duration) *Expirer {
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	return &Expirer{
		stores:   stores,
		interval: interval,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Start launches the background expiry loop in a new goroutine.
func (e *Expirer) Start() {
	go e.run()
}

// Stop halts the background loop and waits for it to exit.
func (e *Expirer) Stop() {
	close(e.stop)
	<-e.done
}

func (e *Expirer) run() {
	defer close(e.done)
	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()
	for {
		select {
		case <-e.stop:
			return
		case <-ticker.C:
			for _, st := range e.stores {
				if removed := st.ExpireCycle(); removed > 0 {
					log.Printf("store: expired %d key(s)", removed)
				}
			}
		}
	}
}
