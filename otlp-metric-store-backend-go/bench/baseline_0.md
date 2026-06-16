# Load test: baseline

- **Schema:** wide-row (metadata duplicated per datapoint)
- **Workload:** 2000 series × 200 datapoints = 400000 datapoints across 4000 requests (8 workers)

## Ingest

- Duration: 91.22s
- Throughput: **4385 datapoints/sec** (44 req/sec)
- Export RPC latency (ms): p50=213.59 p95=225.09 p99=227.47 max=229.68

## Storage (after OPTIMIZE FINAL)

| Table | Rows | Compressed | Uncompressed |
|---|---:|---:|---:|
| otel_metrics_gauge | 200000 | 990.97 KiB | 40.79 MiB |
| otel_metrics_sum | 200000 | 993.98 KiB | 41.74 MiB |

- **Total compressed:** 1.94 MiB
- **Total uncompressed:** 82.53 MiB
- **Compressed bytes / datapoint:** **5.08**

## Query (time-bounded + filtered, otel_metrics_gauge)

- Latency (ms): p50=2.88 p95=4.47 p99=4.47 max=16.33
