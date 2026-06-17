//go:build loadtest

// Package main load harness.
//
// This file is compiled only with `-tags loadtest` so it never runs during
// the normal unit/integration test suite. It spins up a real ClickHouse via
// testcontainers, drives synthetic OTLP traffic through the real gRPC Export
// path, and emits a machine-readable + human-readable artifact under bench/.
//
// The same harness is run before and after the metadata/datapoint split so the
// two artifacts are directly comparable. The headline metrics are:
//   - sustained ingest throughput (datapoints/sec)
//   - on-disk bytes per datapoint (the storage win we expect from dedup)
//   - Export RPC latency percentiles
//   - representative time-bounded query latency
//
// Run with: make loadtest  (or: go test -tags loadtest -run TestLoadBaseline)
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// loadConfig controls the synthetic workload. All knobs are env-overridable so
// the same harness can be tuned without recompiling, but the defaults are fixed
// and deterministic so before/after runs are comparable out of the box.
type loadConfig struct {
	Services             int // distinct service.name values
	MetricsPerService    int // distinct metric names per service
	AttrCombosPerMetric  int // distinct datapoint attribute sets per metric (the cardinality knob)
	DatapointsPerSeries  int // timestamps emitted per unique series
	DatapointsPerRequest int // datapoints packed into a single Export RPC
	Workers              int // concurrent gRPC senders
}

func loadConfigFromEnv() loadConfig {
	config := loadConfig{
		Services:             20,
		MetricsPerService:    10,
		AttrCombosPerMetric:  10,
		DatapointsPerSeries:  200,
		DatapointsPerRequest: 100,
		Workers:              8,
	}
	readEnvInt := func(key string, target *int) {
		if raw := os.Getenv(key); raw != "" {
			if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
				*target = parsed
			}
		}
	}
	readEnvInt("LOAD_SERVICES", &config.Services)
	readEnvInt("LOAD_METRICS", &config.MetricsPerService)
	readEnvInt("LOAD_ATTR_COMBOS", &config.AttrCombosPerMetric)
	readEnvInt("LOAD_DATAPOINTS", &config.DatapointsPerSeries)
	readEnvInt("LOAD_DP_PER_REQ", &config.DatapointsPerRequest)
	readEnvInt("LOAD_WORKERS", &config.Workers)
	return config
}

func (config loadConfig) totalSeries() int {
	return config.Services * config.MetricsPerService * config.AttrCombosPerMetric
}

func (config loadConfig) totalDatapoints() int {
	return config.totalSeries() * config.DatapointsPerSeries
}

// loadReport is serialized to bench/<label>.json and rendered to markdown.
type loadReport struct {
	Label                       string         `json:"label"`
	Schema                      string         `json:"schema"`
	Config                      loadConfig     `json:"config"`
	TotalSeries                 int            `json:"total_series"`
	TotalDatapoints             int            `json:"total_datapoints"`
	TotalRequests               int            `json:"total_requests"`
	IngestSeconds               float64        `json:"ingest_seconds"`
	DatapointsPerSecond         float64        `json:"datapoints_per_sec"`
	RequestsPerSecond           float64        `json:"requests_per_sec"`
	RPCLatencyMs                latencyStats   `json:"rpc_latency_ms"`
	Storage                     []tableStorage `json:"storage"`
	TotalCompressedBytes        uint64         `json:"total_compressed_bytes"`
	TotalUncompressedBytes      uint64         `json:"total_uncompressed_bytes"`
	CompressedBytesPerDatapoint float64        `json:"compressed_bytes_per_datapoint"`
	QueryLatencyMs              latencyStats   `json:"query_latency_ms"`
}

type latencyStats struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	Max float64 `json:"max"`
}

type tableStorage struct {
	Table             string `json:"table"`
	Rows              uint64 `json:"rows"`
	CompressedBytes   uint64 `json:"compressed_bytes"`
	UncompressedBytes uint64 `json:"uncompressed_bytes"`
}

// loadBaseTime is a fixed point in time so generated partitions are stable and
// runs are reproducible. Datapoints fan out forward from here.
var loadBaseTime = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

var loadRegions = []string{"us-east-1", "us-west-2", "eu-central-1", "ap-south-1"}

// buildRequest deterministically constructs one Export request for a slice of a
// single series' datapoints. seriesID encodes (service, metric, attrCombo);
// metrics with an even index are emitted as Gauge, odd as Sum, so both
// implemented paths get exercised.
func buildRequest(config loadConfig, seriesID, firstDatapoint, datapointCount int) *colmetricspb.ExportMetricsServiceRequest {
	seriesPerService := config.MetricsPerService * config.AttrCombosPerMetric
	serviceIndex := seriesID / seriesPerService
	withinService := seriesID % seriesPerService
	metricIndex := withinService / config.AttrCombosPerMetric
	attrComboIndex := withinService % config.AttrCombosPerMetric
	isGauge := metricIndex%2 == 0

	datapoints := make([]*metricspb.NumberDataPoint, datapointCount)
	for offset := 0; offset < datapointCount; offset++ {
		datapointIndex := firstDatapoint + offset
		timestamp := uint64(loadBaseTime.Add(time.Duration(datapointIndex) * 10 * time.Second).UnixNano())
		datapoints[offset] = &metricspb.NumberDataPoint{
			Attributes: []*commonpb.KeyValue{
				{Key: "region", Value: stringValue(loadRegions[attrComboIndex%len(loadRegions)])},
				{Key: "shard", Value: stringValue(fmt.Sprintf("shard-%d", attrComboIndex))},
			},
			StartTimeUnixNano: uint64(loadBaseTime.UnixNano()),
			TimeUnixNano:      timestamp,
			Value:             &metricspb.NumberDataPoint_AsDouble{AsDouble: float64(datapointIndex) + float64(seriesID)*0.001},
		}
	}

	metric := &metricspb.Metric{
		Name:        fmt.Sprintf("metric.%d", metricIndex),
		Description: "synthetic load metric",
		Unit:        "1",
	}
	if isGauge {
		metric.Data = &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: datapoints}}
	} else {
		metric.Data = &metricspb.Metric_Sum{Sum: &metricspb.Sum{
			AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
			IsMonotonic:            true,
			DataPoints:             datapoints,
		}}
	}

	return &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{Key: "service.name", Value: stringValue(fmt.Sprintf("svc-%d", serviceIndex))},
						{Key: "host.name", Value: stringValue(fmt.Sprintf("host-%d", serviceIndex))},
						{Key: "deployment.environment", Value: stringValue("production")},
					},
				},
				SchemaUrl: "https://opentelemetry.io/schemas/1.4.0",
				ScopeMetrics: []*metricspb.ScopeMetrics{
					{
						Scope:   &commonpb.InstrumentationScope{Name: "load.scope", Version: "1.0.0"},
						Metrics: []*metricspb.Metric{metric},
					},
				},
			},
		},
	}
}

func stringValue(value string) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}}
}

// requestJob is a unit of work: a contiguous slice of one series' datapoints.
type requestJob struct {
	seriesID       int
	firstDatapoint int
	datapointCount int
}

func buildJobs(config loadConfig) []requestJob {
	var jobs []requestJob
	for seriesID := 0; seriesID < config.totalSeries(); seriesID++ { // 200 iterations with defaults
		for start := 0; start < config.DatapointsPerSeries; start += config.DatapointsPerRequest { // 200 iterations with default
			count := config.DatapointsPerRequest
			if start+count > config.DatapointsPerSeries {
				count = config.DatapointsPerSeries - start
			}
			jobs = append(jobs, requestJob{seriesID: seriesID, firstDatapoint: start, datapointCount: count})
		}
	}
	return jobs
}

// runLoad executes the workload against an already-wired gRPC client and
// returns the populated report (minus storage/query stats, filled by caller).
func runLoad(t *testing.T, config loadConfig, client colmetricspb.MetricsServiceClient) loadReport {
	t.Helper()
	jobs := buildJobs(config)

	pendingJobs := make(chan requestJob, len(jobs))
	for _, oneJob := range jobs {
		pendingJobs <- oneJob
	}
	close(pendingJobs)

	var (
		workers        sync.WaitGroup
		latenciesMutex sync.Mutex
		latencies      = make([]time.Duration, 0, len(jobs))
		failedRequests atomic.Int64
	)

	startedAt := time.Now()
	for worker := 0; worker < config.Workers; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			workerLatencies := make([]time.Duration, 0, len(jobs)/config.Workers+1)
			for currentJob := range pendingJobs {
				request := buildRequest(config, currentJob.seriesID, currentJob.firstDatapoint, currentJob.datapointCount)
				requestStartedAt := time.Now()
				_, err := client.Export(context.Background(), request)
				requestDuration := time.Since(requestStartedAt)
				if err != nil {
					failedRequests.Add(1)
					continue
				}
				workerLatencies = append(workerLatencies, requestDuration)
			}
			latenciesMutex.Lock()
			latencies = append(latencies, workerLatencies...)
			latenciesMutex.Unlock()
		}()
	}
	workers.Wait()
	ingestDuration := time.Since(startedAt)

	if failures := failedRequests.Load(); failures > 0 {
		t.Fatalf("%d Export RPCs failed", failures)
	}

	totalDatapoints := config.totalDatapoints()
	return loadReport{
		Config:              config,
		TotalSeries:         config.totalSeries(),
		TotalDatapoints:     totalDatapoints,
		TotalRequests:       len(jobs),
		IngestSeconds:       ingestDuration.Seconds(),
		DatapointsPerSecond: float64(totalDatapoints) / ingestDuration.Seconds(),
		RequestsPerSecond:   float64(len(jobs)) / ingestDuration.Seconds(),
		RPCLatencyMs:        percentilesMs(latencies),
	}
}

func percentilesMs(durations []time.Duration) latencyStats {
	if len(durations) == 0 {
		return latencyStats{}
	}
	sort.Slice(durations, func(left, right int) bool { return durations[left] < durations[right] })
	atPercentile := func(percentile float64) float64 {
		index := int(percentile * float64(len(durations)-1))
		return float64(durations[index].Microseconds()) / 1000.0
	}
	return latencyStats{P50: atPercentile(0.50), P95: atPercentile(0.95), P99: atPercentile(0.99), Max: atPercentile(1.0)}
}

// measureStorage forces merges (so the comparison is fair) and reads on-disk
// sizes from system.parts for the given tables.
func measureStorage(t *testing.T, store *ClickHouseMetricsStore, tables []string) []tableStorage {
	t.Helper()
	ctx := context.Background()
	for _, table := range tables {
		if err := store.conn.Exec(ctx, "OPTIMIZE TABLE "+table+" FINAL"); err != nil {
			t.Fatalf("optimize %s: %v", table, err)
		}
	}

	storageByTable := make([]tableStorage, 0, len(tables))
	for _, table := range tables {
		storage := tableStorage{Table: table}
		err := store.conn.QueryRow(ctx, `
			SELECT
				sum(rows),
				sum(data_compressed_bytes),
				sum(data_uncompressed_bytes)
			FROM system.parts
			WHERE active AND database = 'default' AND table = $1`, table,
		).Scan(&storage.Rows, &storage.CompressedBytes, &storage.UncompressedBytes)
		if err != nil {
			t.Fatalf("reading storage for %s: %v", table, err)
		}
		storageByTable = append(storageByTable, storage)
	}
	return storageByTable
}

// measureQueryLatency runs a representative time-bounded + filtered query a few
// times and reports the latency distribution. It reads through the
// reconstruction view (points joined to deduped metadata), the documented read
// path for the split schema.
func measureQueryLatency(t *testing.T, store *ClickHouseMetricsStore, config loadConfig) latencyStats {
	t.Helper()
	ctx := context.Background()
	rangeStart := loadBaseTime
	rangeEnd := loadBaseTime.Add(time.Duration(config.DatapointsPerSeries) * 10 * time.Second)

	const iterations = 20
	latencies := make([]time.Duration, 0, iterations)
	for iteration := 0; iteration < iterations; iteration++ {
		serviceName := fmt.Sprintf("svc-%d", iteration%config.Services)
		var matchedRows uint64
		var averageValue float64
		queryStartedAt := time.Now()
		err := store.conn.QueryRow(ctx, `
			SELECT count(), avg(Value)
			FROM otel_metrics
			WHERE ServiceName = $1
			  AND MetricName = 'metric.0'
			  AND TimeUnix BETWEEN $2 AND $3`, serviceName, rangeStart, rangeEnd,
		).Scan(&matchedRows, &averageValue)
		latencies = append(latencies, time.Since(queryStartedAt))
		if err != nil {
			t.Fatalf("query latency probe: %v", err)
		}
	}
	return percentilesMs(latencies)
}

func writeArtifact(t *testing.T, report loadReport) {
	t.Helper()
	directory := "bench"
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatalf("creating bench dir: %v", err)
	}

	jsonPath := filepath.Join(directory, report.Label+".json")
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatalf("marshaling report: %v", err)
	}
	if err := os.WriteFile(jsonPath, append(encoded, '\n'), 0o644); err != nil {
		t.Fatalf("writing json artifact: %v", err)
	}

	markdownPath := filepath.Join(directory, report.Label+".md")
	if err := os.WriteFile(markdownPath, []byte(renderMarkdown(report)), 0o644); err != nil {
		t.Fatalf("writing md artifact: %v", err)
	}
	t.Logf("wrote artifacts: %s, %s", jsonPath, markdownPath)
}

func renderMarkdown(report loadReport) string {
	markdown := fmt.Sprintf("# Load test: %s\n\n", report.Label)
	markdown += fmt.Sprintf("- **Schema:** %s\n", report.Schema)
	markdown += fmt.Sprintf("- **Workload:** %d series × %d datapoints = %d datapoints across %d requests (%d workers)\n",
		report.TotalSeries, report.Config.DatapointsPerSeries, report.TotalDatapoints, report.TotalRequests, report.Config.Workers)
	markdown += "\n## Ingest\n\n"
	markdown += fmt.Sprintf("- Duration: %.2fs\n", report.IngestSeconds)
	markdown += fmt.Sprintf("- Throughput: **%.0f datapoints/sec** (%.0f req/sec)\n", report.DatapointsPerSecond, report.RequestsPerSecond)
	markdown += fmt.Sprintf("- Export RPC latency (ms): p50=%.2f p95=%.2f p99=%.2f max=%.2f\n",
		report.RPCLatencyMs.P50, report.RPCLatencyMs.P95, report.RPCLatencyMs.P99, report.RPCLatencyMs.Max)
	markdown += "\n## Storage (after OPTIMIZE FINAL)\n\n"
	markdown += "| Table | Rows | Compressed | Uncompressed |\n|---|---:|---:|---:|\n"
	for _, storage := range report.Storage {
		markdown += fmt.Sprintf("| %s | %d | %s | %s |\n", storage.Table, storage.Rows, humanBytes(storage.CompressedBytes), humanBytes(storage.UncompressedBytes))
	}
	markdown += fmt.Sprintf("\n- **Total compressed:** %s\n", humanBytes(report.TotalCompressedBytes))
	markdown += fmt.Sprintf("- **Total uncompressed:** %s\n", humanBytes(report.TotalUncompressedBytes))
	markdown += fmt.Sprintf("- **Compressed bytes / datapoint:** **%.2f**\n", report.CompressedBytesPerDatapoint)
	markdown += "\n## Query (time-bounded + filtered, otel_metrics_gauge)\n\n"
	markdown += fmt.Sprintf("- Latency (ms): p50=%.2f p95=%.2f p99=%.2f max=%.2f\n",
		report.QueryLatencyMs.P50, report.QueryLatencyMs.P95, report.QueryLatencyMs.P99, report.QueryLatencyMs.Max)
	return markdown
}

func humanBytes(byteCount uint64) string {
	const unit = 1024
	if byteCount < unit {
		return fmt.Sprintf("%d B", byteCount)
	}
	divisor, exponent := uint64(unit), 0
	for remaining := byteCount / unit; remaining >= unit; remaining /= unit {
		divisor *= unit
		exponent++
	}
	return fmt.Sprintf("%.2f %ciB", float64(byteCount)/float64(divisor), "KMGTPE"[exponent])
}

// TestLoadBaseline runs the full load workload against the current schema and
// writes bench/baseline.{json,md}. Override the label via LOAD_LABEL (e.g.
// "after") to reuse the same harness post-change.
func TestLoadBaseline(t *testing.T) {
	config := loadConfigFromEnv()
	label := os.Getenv("LOAD_LABEL")
	if label == "" {
		label = "baseline"
	}
	schemaDescription := os.Getenv("LOAD_SCHEMA")
	if schemaDescription == "" {
		schemaDescription = "wide-row (metadata duplicated per datapoint)"
	}

	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	// Wire the real gRPC Export path over an in-memory listener.
	listener := bufconn.Listen(16 * 1024 * 1024)
	grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(64 * 1024 * 1024))
	colmetricspb.RegisterMetricsServiceServer(grpcServer, newServer("bufconn", store))
	go func() { _ = grpcServer.Serve(listener) }()
	defer grpcServer.Stop()

	clientConn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(64*1024*1024)),
	)
	if err != nil {
		t.Fatalf("dialing bufconn: %v", err)
	}
	defer clientConn.Close()
	client := colmetricspb.NewMetricsServiceClient(clientConn)

	t.Logf("starting load: %d series, %d datapoints, %d requests, %d workers",
		config.totalSeries(), config.totalDatapoints(), len(buildJobs(config)), config.Workers)

	report := runLoad(t, config, client)
	report.Label = label
	report.Schema = schemaDescription

	tables := []string{"otel_metrics_meta", "otel_metrics_point"}
	report.Storage = measureStorage(t, store, tables)
	for _, storage := range report.Storage {
		report.TotalCompressedBytes += storage.CompressedBytes
		report.TotalUncompressedBytes += storage.UncompressedBytes
	}
	report.CompressedBytesPerDatapoint = float64(report.TotalCompressedBytes) / float64(report.TotalDatapoints)
	report.QueryLatencyMs = measureQueryLatency(t, store, config)

	writeArtifact(t, report)

	t.Logf("ingest: %.0f dp/s, %.2f bytes/dp, query p99 %.2fms",
		report.DatapointsPerSecond, report.CompressedBytesPerDatapoint, report.QueryLatencyMs.P99)
}
