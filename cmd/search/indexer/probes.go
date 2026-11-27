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

type attachmentSource interface {
	DetachedSince() time.Time
}

type probes struct {
	stallThreshold time.Duration
	detachGrace    time.Duration
	now            func() time.Time

	cacheSynced       atomic.Bool
	consumersAttached atomic.Bool

	mu        sync.RWMutex
	sources   map[string]progressSource
	consumers map[string]attachmentSource
}

func newProbes(stallThreshold, detachGrace time.Duration) *probes {
	return &probes{
		stallThreshold: stallThreshold,
		detachGrace:    detachGrace,
		now:            time.Now,
		sources:        make(map[string]progressSource),
		consumers:      make(map[string]attachmentSource),
	}
}

func (p *probes) watchConsumer(name string, source attachmentSource) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.consumers[name] = source
}

func (p *probes) detachedConsumers(longerThan func(time.Duration) bool) []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	now := p.now()
	var failures []string
	for _, name := range sortedKeys(p.consumers) {
		since := p.consumers[name].DetachedSince()
		if since.IsZero() {
			continue
		}
		if detached := now.Sub(since); longerThan(detached) {
			failures = append(failures, fmt.Sprintf("%s consumer has been detached for %s", name, detached.Truncate(time.Second)))
		}
	}
	return failures
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
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
	failures = append(failures, p.detachedConsumers(func(time.Duration) bool { return true })...)
	respond(w, failures)
}

func (p *probes) healthz(w http.ResponseWriter, _ *http.Request) {
	p.mu.RLock()
	now := p.now()
	var failures []string
	for _, name := range sortedKeys(p.sources) {
		last := p.sources[name].LastProgress()
		if last.IsZero() {
			continue
		}
		if stalled := now.Sub(last); stalled > p.stallThreshold {
			failures = append(failures, fmt.Sprintf("%s batcher has made no progress for %s", name, stalled.Truncate(time.Second)))
		}
	}
	p.mu.RUnlock()

	failures = append(failures, p.detachedConsumers(func(d time.Duration) bool { return d > p.detachGrace })...)
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
