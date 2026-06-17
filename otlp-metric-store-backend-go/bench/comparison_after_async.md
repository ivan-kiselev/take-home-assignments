# Load test: baseline → split → split + async writer

Same harness, same workload (2,000 series × 200 datapoints = **400,000 datapoints**,
4,000 RPCs, 8 workers), against a throwaway ClickHouse via testcontainers.

- **baseline** - wide row: every datapoint stores a full copy of its metadata.
- **split** - one metadata row per series (`otel_metrics_meta`, ReplacingMergeTree)
  + thin datapoints (`otel_metrics_point`), referencing it by fingerprint.
  Synchronous per-request inserts; view read path uses `FINAL`.
- **split + async** - same schema, plus the async batching `Ingester` (Export
  acks on enqueue; one flusher writes large batches with cross-request metadata
  dedup); view uses `LIMIT 1 BY` instead of `FINAL`; two-step `QueryRange` hot path.

## Headline

| Metric | Baseline | Split (sync) | Split + async | vs. baseline |
|---|---:|---:|---:|---|
| Ingest throughput | 4,385 dp/s | 2,399 dp/s | **269,677 dp/s** | **61× faster** |
| Export RPC p50 | 214 ms | 343 ms | **0.31 ms** | ack-on-enqueue |
| Query p99 - hot path (two-step) | 4.5 ms | - | **4.0 ms** | **beats baseline** |
| Query p99 - convenience view | 4.5 ms | 19.6 ms | 7.9 ms | ~1.8× slower (join) |
| Compressed bytes / datapoint | 5.08 | 1.76 | 1.76 | **2.9× smaller** |
| Compressed on disk | 1.94 MiB | 0.67 MiB | 0.67 MiB | **2.9× smaller** |
| Uncompressed | 82.5 MiB | 12.6 MiB | 12.6 MiB | **6.5× smaller** |
| Parts written (pre-merge) | ~4,000 | ~8,000 | **9** | the root-cause fix |

All goals met: ingest far exceeds baseline, hot-path query beats baseline, and the
storage win is fully retained.

## Why ingest went from 2,399 → 269,677 dp/s

The bottleneck was never the schema - it was the **number of small INSERTs**, each
creating a ClickHouse part. Baseline did 4,000 inserts; the sync split did ~8,000
(metadata + points per request). The async writer batches across requests:

- **9 parts** written for the whole run (vs thousands) - buffered points flush in
  ~50k-row batches, so ClickHouse builds a handful of large parts instead of
  thousands of tiny ones. This is the dominant win.
- **Cross-request metadata dedup**: the 2,000 distinct series are inserted ~once
  total instead of being re-sent on all 4,000 requests.
- **Export latency collapses** to enqueue cost (p50 0.31 ms) because the RPC no
  longer waits on ClickHouse. (p99 109 ms reflects backpressure when the bounded
  queue is full - the intended flow-control signal, not per-request DB latency.)

The trade-off, by design: Export acks before the data is durably written, so
buffered-but-unflushed data is lost on a crash. Bounded by queue + batch size and
the 200 ms flush interval.

## Why query improved

The split-sync view used `JOIN (… FINAL)`, forcing a merge-on-read (19.6 ms p99).
Two fixes:

- **Two-step hot path** (`QueryRange`): resolve fingerprints from the small meta
  table (`DISTINCT`, no `FINAL`), then range-scan points by `(Fingerprint, TimeUnix)`.
  No join, no `FINAL` → **4.0 ms p99, beating the baseline's 4.5 ms.**
- **Convenience view** now uses `LIMIT 1 BY Fingerprint` instead of `FINAL` to
  dedup for the join cheaply → 7.9 ms p99 (down from 19.6). Still does a join, so
  slightly above baseline; the two-step path is the one to use when latency matters.

## Storage (unchanged from the split - async doesn't affect on-disk layout)

| Table | Rows | Compressed | Uncompressed |
|---|---:|---:|---:|
| `otel_metrics_meta` | 2,000 | 32 KiB | 399 KiB |
| `otel_metrics_point` | 400,000 | 655 KiB | 12.2 MiB |

## Takeaway

The split delivered the storage goal (2.9× compressed, 6.5× uncompressed, 200×
fewer metadata copies, proven lossless by the DB-backed reconstruction property
test). The async batching writer then turned the two expected regressions into
wins: ingest is 61× the baseline by collapsing thousands of small inserts into ~9
large parts, and the two-step read path beats the baseline's query latency. The
remaining cost is the accepted ack-on-enqueue durability window.
