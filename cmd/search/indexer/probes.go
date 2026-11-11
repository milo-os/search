package indexer

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type progressSource interface {
	LastProgress() time.Time
}

type probes struct {
	stallThreshold time.Duration
	now            func() time.Time

	cacheSynced       atomic.Bool
	consumersAttached atomic.Bool

	mu      sync.RWMutex
	sources map[string]progressSource
}

func newProbes(stallThreshold time.Duration) *probes {
	return &probes{
		stallThreshold: stallThreshold,
		now:            time.Now,
		sources:        make(map[string]progressSource),
	}
}

func (p *probes) watch(name string, source progressSource) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sources[name] = source
}

func (p *probes) readyz(w http.ResponseWriter, _ *http.Request) {
	var failures []string
	if !p.cacheSynced.Load() {
		failures = append(failures, "policy cache has not synced")
	}
	if !p.consumersAttached.Load() {
		failures = append(failures, "JetStream consumers are not attached")
	}
	respond(w, failures)
}

func (p *probes) healthz(w http.ResponseWriter, _ *http.Request) {
	p.mu.RLock()
	names := make([]string, 0, len(p.sources))
	for name := range p.sources {
		names = append(names, name)
	}
	sort.Strings(names)

	now := p.now()
	var failures []string
	for _, name := range names {
		last := p.sources[name].LastProgress()
		if last.IsZero() {
			continue
		}
		if stalled := now.Sub(last); stalled > p.stallThreshold {
			failures = append(failures, fmt.Sprintf("%s batcher has made no progress for %s", name, stalled.Truncate(time.Second)))
		}
	}
	p.mu.RUnlock()

	respond(w, failures)
}

func respond(w http.ResponseWriter, failures []string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if len(failures) > 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintln(w, strings.Join(failures, "\n"))
		return
	}
	fmt.Fprintln(w, "ok")
}

func (p *probes) mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", p.readyz)
	mux.HandleFunc("/healthz", p.healthz)
	return mux
}
