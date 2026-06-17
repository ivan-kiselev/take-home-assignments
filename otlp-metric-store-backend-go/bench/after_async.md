# Load test: after_async

- **Schema:** split + async batched writer
- **Workload:** 2000 series × 200 datapoints = 400000 datapoints across 4000 requests (8 workers)

## Ingest

- Duration: 1.48s
- Throughput (incl. drain): **269677 datapoints/sec** (2697 req/sec)
- Export RPC latency (ms): p50=0.31 p95=0.77 p99=109.07 max=268.98

## Storage

- Active parts before OPTIMIZE: **9**

| Table | Rows | Compressed | Uncompressed |
|---|---:|---:|---:|
| otel_metrics_meta | 2000 | 32.25 KiB | 398.52 KiB |
| otel_metrics_point | 400000 | 654.67 KiB | 12.21 MiB |

- **Total compressed (after OPTIMIZE FINAL):** 686.92 KiB
- **Total uncompressed:** 12.60 MiB
- **Compressed bytes / datapoint:** **1.76**

## Query (time-bounded + filtered by service/metric)

- Convenience view (ms): p50=6.28 p95=7.92 p99=7.92 max=13.63
- Two-step hot path (ms): p50=3.57 p95=4.00 p99=4.00 max=4.20
