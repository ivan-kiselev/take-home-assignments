package main

import (
	"container/list"
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// flushTimeout bounds a single batch write so a wedged ClickHouse can't block
// the flusher (and thus shutdown) forever.
const flushTimeout = 30 * time.Second

// IngesterConfig tunes the async batching writer. Zero values fall back to
// sensible defaults in NewIngester.
type IngesterConfig struct {
	// MaxBatchPoints flushes the buffer once this many datapoints accumulate.
	// Large batches mean few, big ClickHouse parts instead of many small ones.
	MaxBatchPoints int
	// FlushInterval flushes the buffer at least this often, bounding both
	// visibility latency and (under low load) the number of parts created.
	FlushInterval time.Duration
	// QueueCapacity bounds the in-flight enqueue channel; a full queue applies
	// backpressure to Enqueue.
	QueueCapacity int
	// DedupCacheSize bounds the LRU of already-inserted fingerprints used to
	// suppress repeat metadata inserts.
	DedupCacheSize int
}

func (c IngesterConfig) withDefaults() IngesterConfig {
	if c.MaxBatchPoints <= 0 {
		c.MaxBatchPoints = 50_000
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = 200 * time.Millisecond
	}
	if c.QueueCapacity <= 0 {
		c.QueueCapacity = 1024
	}
	if c.DedupCacheSize <= 0 {
		c.DedupCacheSize = 1_000_000
	}
	return c
}

type ingestItem struct {
	metadata []SeriesMetadata
	points   []DataPoint
}

// Ingester decouples the gRPC Export path from ClickHouse writes. Export enqueues
// mapped metadata + points and returns immediately (ack-on-enqueue); a single
// background flusher accumulates rows across many requests and writes them in
// large batches. This trades a small crash-loss window (buffered-but-unflushed
// data) for far higher throughput and cross-request metadata deduplication.
//
// Concurrency: Enqueue is safe to call from many goroutines. The flusher owns
// all mutable batch state and the dedup cache, so neither needs locking. Close
// must be called only after all Enqueue callers have stopped (the gRPC server's
// GracefulStop and the load harness's worker join both guarantee this).
type Ingester struct {
	store MetricsStore
	cfg   IngesterConfig

	queue     chan ingestItem
	flushReq  chan chan struct{}
	done      chan struct{}
	closeOnce sync.Once

	// Owned exclusively by the flusher goroutine.
	pendingPoints []DataPoint
	pendingMeta   []SeriesMetadata
	seen          *fingerprintCache

	// Observability counters.
	flushedPoints atomic.Int64
	flushedMeta   atomic.Int64
	flushErrors   atomic.Int64
}

// NewIngester starts an Ingester and its background flusher.
func NewIngester(store MetricsStore, cfg IngesterConfig) *Ingester {
	cfg = cfg.withDefaults()
	i := &Ingester{
		store:    store,
		cfg:      cfg,
		queue:    make(chan ingestItem, cfg.QueueCapacity),
		flushReq: make(chan chan struct{}),
		done:     make(chan struct{}),
		seen:     newFingerprintCache(cfg.DedupCacheSize),
	}
	go i.run()
	return i
}

// Enqueue hands a request's mapped records to the flusher. It blocks only when
// the queue is full (backpressure); the context can cancel that wait.
func (i *Ingester) Enqueue(ctx context.Context, metadata []SeriesMetadata, points []DataPoint) error {
	if len(metadata) == 0 && len(points) == 0 {
		return nil
	}
	select {
	case i.queue <- ingestItem{metadata: metadata, points: points}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Flush forces a synchronous flush of whatever is currently buffered and waits
// for it to complete. Useful for tests and for read-after-write checks.
func (i *Ingester) Flush(ctx context.Context) error {
	done := make(chan struct{})
	select {
	case i.flushReq <- done:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close stops accepting new items, drains the queue, performs a final flush, and
// returns. It is idempotent. Callers must ensure no Enqueue runs concurrently
// with or after Close.
func (i *Ingester) Close(ctx context.Context) error {
	i.closeOnce.Do(func() { close(i.queue) })
	select {
	case <-i.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (i *Ingester) run() {
	ticker := time.NewTicker(i.cfg.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case item, ok := <-i.queue:
			if !ok {
				i.flush() // final drain on Close
				close(i.done)
				return
			}
			i.accumulate(item)
			if len(i.pendingPoints) >= i.cfg.MaxBatchPoints {
				i.flush()
			}
		case req := <-i.flushReq:
			i.flush()
			close(req)
		case <-ticker.C:
			i.flush()
		}
	}
}

func (i *Ingester) accumulate(item ingestItem) {
	i.pendingPoints = append(i.pendingPoints, item.points...)
	for _, series := range item.metadata {
		// The cache is a suppression optimization only: an evicted-then-re-seen
		// fingerprint causes a redundant insert, never an error, because
		// otel_metrics_meta is a ReplacingMergeTree of byte-identical rows.
		if i.seen.add(series.Fingerprint) {
			i.pendingMeta = append(i.pendingMeta, series)
		}
	}
}

// flush writes the buffered points and metadata as two large batches. Points are
// written first: a datapoint whose metadata has not landed yet is simply not
// joinable via the view until it does (Fingerprint is only a join key), so
// prioritizing the volume table is safe.
func (i *Ingester) flush() {
	if len(i.pendingPoints) == 0 && len(i.pendingMeta) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), flushTimeout)
	defer cancel()

	if len(i.pendingPoints) > 0 {
		if err := i.store.InsertPoints(ctx, i.pendingPoints); err != nil {
			// Async errors can't be returned to the original caller; log, count,
			// and drop the batch (accepted data-loss limitation).
			slog.Error("flushing points failed", slog.Any("error", err), slog.Int("count", len(i.pendingPoints)))
			i.flushErrors.Add(1)
		} else {
			i.flushedPoints.Add(int64(len(i.pendingPoints)))
		}
	}
	if len(i.pendingMeta) > 0 {
		if err := i.store.InsertMetadata(ctx, i.pendingMeta); err != nil {
			slog.Error("flushing metadata failed", slog.Any("error", err), slog.Int("count", len(i.pendingMeta)))
			i.flushErrors.Add(1)
		} else {
			i.flushedMeta.Add(int64(len(i.pendingMeta)))
		}
	}

	i.pendingPoints = i.pendingPoints[:0]
	i.pendingMeta = i.pendingMeta[:0]
}

// fingerprintCache is a bounded LRU set of fingerprints. It is accessed only by
// the flusher goroutine, so it needs no synchronization.
type fingerprintCache struct {
	capacity int
	items    map[uint64]*list.Element
	order    *list.List // front = most recently seen
}

func newFingerprintCache(capacity int) *fingerprintCache {
	return &fingerprintCache{
		capacity: capacity,
		items:    make(map[uint64]*list.Element),
		order:    list.New(),
	}
}

// add records a fingerprint and reports whether it was newly added (i.e. not
// already present), so the caller can decide whether a metadata insert is needed.
func (c *fingerprintCache) add(fingerprint uint64) bool {
	if element, ok := c.items[fingerprint]; ok {
		c.order.MoveToFront(element)
		return false
	}
	c.items[fingerprint] = c.order.PushFront(fingerprint)
	if c.order.Len() > c.capacity {
		oldest := c.order.Back()
		if oldest != nil {
			c.order.Remove(oldest)
			delete(c.items, oldest.Value.(uint64))
		}
	}
	return true
}
