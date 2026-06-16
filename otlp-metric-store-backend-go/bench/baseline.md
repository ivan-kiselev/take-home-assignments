# Load test: baseline

- **Schema:** wide-row (metadata duplicated per datapoint)
- **Workload:** 2000 series × 200 datapoints = 400000 datapoints across 4000 requests (8 workers)

## Ingest

- Duration: 90.34s
- Throughput: **4428 datapoints/sec** (44 req/sec)
- Export RPC latency (ms): p50=208.03 p95=224.50 p99=227.88 max=317.40

## Storage (after OPTIMIZE FINAL)

| Table | Rows | Compressed | Uncompressed |
|---|---:|---:|---:|
| otel_metrics_gauge | 200000 | 992.05 KiB | 40.79 MiB |
| otel_metrics_sum | 200000 | 994.71 KiB | 41.74 MiB |

- **Total compressed:** 1.94 MiB
- **Total uncompressed:** 82.53 MiB
- **Compressed bytes / datapoint:** **5.09**

## Query (time-bounded + filtered, otel_metrics_gauge)

- Latency (ms): p50=2.39 p95=3.89 p99=3.89 max=10.89
