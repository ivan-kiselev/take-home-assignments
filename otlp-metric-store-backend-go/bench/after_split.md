# Load test: after

- **Schema:** split (metadata lookup + datapoint references by fingerprint)
- **Workload:** 2000 series × 200 datapoints = 400000 datapoints across 4000 requests (8 workers)

## Ingest

- Duration: 166.71s
- Throughput: **2399 datapoints/sec** (24 req/sec)
- Export RPC latency (ms): p50=343.30 p95=403.96 p99=414.03 max=624.99

## Storage (after OPTIMIZE FINAL)

| Table | Rows | Compressed | Uncompressed |
|---|---:|---:|---:|
| otel_metrics_meta | 2000 | 32.12 KiB | 398.52 KiB |
| otel_metrics_point | 400000 | 654.78 KiB | 12.21 MiB |

- **Total compressed:** 686.90 KiB
- **Total uncompressed:** 12.60 MiB
- **Compressed bytes / datapoint:** **1.76**

## Query (time-bounded + filtered, otel_metrics_gauge)

- Latency (ms): p50=7.18 p95=19.59 p99=19.59 max=22.64
