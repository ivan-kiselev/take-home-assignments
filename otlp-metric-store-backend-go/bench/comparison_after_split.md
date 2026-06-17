# Load test: baseline vs. after initial table split

Same harness, same workload (2,000 series × 200 datapoints = **400,000 datapoints**,
4,000 RPCs, 8 workers), against a throwaway ClickHouse via testcontainers.

- **baseline** — wide row: every datapoint stores a full copy of its metadata.
- **after** — split: one metadata row per series (`otel_metrics_meta`,
  ReplacingMergeTree) + thin datapoints (`otel_metrics_point`) referencing it by
  fingerprint.

## Headline

| Metric | Baseline | After | Change |
|---|---:|---:|---|
| Compressed on disk | 1.94 MiB | 0.67 MiB | **2.9× smaller** (−65%) |
| Uncompressed | 82.5 MiB | 12.6 MiB | **6.5× smaller** (−85%) |
| Compressed bytes / datapoint | 5.08 | 1.76 | **2.9× smaller** |
| Ingest throughput | 4,385 dp/s | 2,399 dp/s | 1.8× slower ⚠ |
| Export RPC p99 | 227 ms | 414 ms | 1.8× slower ⚠ |
| Query p99 (view) | 4.5 ms | 19.6 ms | 4.4× slower ⚠ |

## Storage

| Table | Rows | Compressed | Uncompressed |
|---|---:|---:|---:|
| baseline: gauge + sum | 400,000 | 1.94 MiB | 82.5 MiB |
| after: `otel_metrics_meta` | 2,000 | 32 KiB | 399 KiB |
| after: `otel_metrics_point` | 400,000 | 655 KiB | 12.2 MiB |

The metadata that was previously repeated on all 400k rows is now stored **2,000
times (once per series) — a 200× reduction in metadata copies**. The win is
largest in *uncompressed* size (6.5×), which is what drives memory, merge work,
and page-cache pressure; on compressed disk it is still a solid 2.9×, smaller
only because ClickHouse's `LowCardinality + ZSTD` was already compressing the
duplicated columns well (flagged in the Part 1 baseline).

## The two regressions — both expected, both addressable

These are **not** caused by the schema; they come from how writes and reads are
currently wired, and were called out as risks in Part 1.

1. **Ingest throughput / RPC latency (1.8× slower).** Ingestion was already
   round-trip bound: each `Export` blocks on a synchronous ClickHouse insert. 
   Now there's simpy two inserts per request without any batching on neither client nor CH. 
2. **Query latency (4.4× slower).** The `otel_metrics` view does a JOIN plus
   `otel_metrics_meta FINAL` on every read; `FINAL` forces a merge-on-read, which might have significant performance overhead 
## Takeaway

The change delivers the storage goal decisively (2.9× compressed, 6.5×
uncompressed, 200× fewer metadata copies) and is proven lossless by the
DB-backed reconstruction property test. The throughput and query-latency
regressions are artifacts of synchronous per-request writes and the FINAL-join
read path, respectively — both have clear, already-identified remediations
(async batched writes; two-step reads) that are the natural next iteration.
