package indexer

import (
	"sync"
	"time"
)

type startSignal struct {
	mu    sync.Mutex
	ch    chan struct{}
	fired bool
}

func (s *startSignal) done() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ch == nil {
		s.ch = make(chan struct{})
		if s.fired {
			close(s.ch)
		}
	}
	return s.ch
}

func (s *startSignal) fire() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fired {
		return
	}
	s.fired = true
	if s.ch != nil {
		close(s.ch)
	}
}

// Attached is closed once the indexer's JetStream consumer is consuming.
func (i *Indexer) Attached() <-chan struct{} { return i.attached.done() }

// Attached is closed once the re-index JetStream consumer is consuming.
func (r *ReindexConsumer) Attached() <-chan struct{} { return r.attached.done() }

// LastProgress is when the batcher's flush loop last completed a turn, or the
// zero time if it has not started.
func (b *Batcher) LastProgress() time.Time {
	b.progressMu.Lock()
	defer b.progressMu.Unlock()
	return b.lastProgress
}

func (b *Batcher) markProgress() {
	b.progressMu.Lock()
	b.lastProgress = time.Now()
	b.progressMu.Unlock()
}
