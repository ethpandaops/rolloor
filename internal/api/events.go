package api

import (
	"sync"

	"github.com/ethpandaops/rolloor/internal/reconcile"
)

// Broadcaster fans events out to live subscribers. A subscriber that falls
// behind is closed rather than slowing the controller; the stream tells the
// client so it can replay from the store.
type Broadcaster struct {
	mu   sync.Mutex
	subs map[chan reconcile.Event]struct{}
}

// NewBroadcaster returns an empty broadcaster.
func NewBroadcaster() *Broadcaster {
	return &Broadcaster{subs: map[chan reconcile.Event]struct{}{}}
}

var _ reconcile.Notifier = (*Broadcaster)(nil)

// Publish delivers to every subscriber without blocking.
func (b *Broadcaster) Publish(e *reconcile.Event) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for ch := range b.subs {
		select {
		case ch <- *e:
		default:
			delete(b.subs, ch)
			close(ch)
		}
	}
}

// Subscribe returns a channel and a function to stop.
func (b *Broadcaster) Subscribe() (events <-chan reconcile.Event, stop func()) {
	ch := make(chan reconcile.Event, 64)

	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()

	return ch, func() {
		b.mu.Lock()
		defer b.mu.Unlock()

		if _, live := b.subs[ch]; live {
			delete(b.subs, ch)
			close(ch)
		}
	}
}

// Subscribers is the current count, for metrics and tests.
func (b *Broadcaster) Subscribers() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return len(b.subs)
}
