//go:build integration

package main

import (
	"context"
	"log"
	"net"
	"testing"
	"time"

	"hegel.dev/go/hegel"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestCreateTables(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	expectedObjects := []string{
		"otel_metrics_meta",
		"otel_metrics_point",
		"otel_metrics", // reconstruction view
	}

	for _, object := range expectedObjects {
		var count uint64
		err := store.conn.QueryRow(ctx,
			"SELECT count() FROM system.tables WHERE database = 'default' AND name = $1", object,
		).Scan(&count)
		if err != nil {
			t.Fatalf("querying system.tables for %s: %v", object, err)
		}
		if count != 1 {
			t.Errorf("expected object %s to exist, got count=%d", object, count)
		}
	}
}

func TestInsertGauge(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	now := uint64(time.Now().UnixNano())
	startTime := now - uint64(time.Minute)
	resourceMetrics := []*metricspb.ResourceMetrics{
		{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-service"}}},
					{Key: "host.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-host"}}},
				},
			},
			SchemaUrl: "https://opentelemetry.io/schemas/1.4.0",
			ScopeMetrics: []*metricspb.ScopeMetrics{
				{
					Scope: &commonpb.InstrumentationScope{
						Name:    "test-scope",
						Version: "1.0.0",
					},
					Metrics: []*metricspb.Metric{
						{
							Name:        "cpu.utilization",
							Description: "CPU utilization percentage",
							Unit:        "%",
							Data: &metricspb.Metric_Gauge{
								Gauge: &metricspb.Gauge{
									DataPoints: []*metricspb.NumberDataPoint{
										{
											Attributes:        []*commonpb.KeyValue{{Key: "cpu", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "0"}}}},
											StartTimeUnixNano: startTime,
											TimeUnixNano:      now,
											Value:             &metricspb.NumberDataPoint_AsDouble{AsDouble: 42.5},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	if err := insertRequest(ctx, store, resourceMetrics); err != nil {
		t.Fatalf("inserting gauge metrics: %v", err)
	}

	var (
		serviceName string
		metricName  string
		metricType  string
		value       float64
	)
	err := store.conn.QueryRow(ctx,
		"SELECT ServiceName, MetricName, MetricType, Value FROM otel_metrics WHERE MetricName = 'cpu.utilization'",
	).Scan(&serviceName, &metricName, &metricType, &value)
	if err != nil {
		t.Fatalf("querying gauge: %v", err)
	}

	if serviceName != "test-service" {
		t.Errorf("expected ServiceName=test-service, got %s", serviceName)
	}
	if metricName != "cpu.utilization" {
		t.Errorf("expected MetricName=cpu.utilization, got %s", metricName)
	}
	if metricType != metricTypeGauge {
		t.Errorf("expected MetricType=%s, got %s", metricTypeGauge, metricType)
	}
	if value != 42.5 {
		t.Errorf("expected Value=42.5, got %f", value)
	}
}

func TestInsertSum(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	now := uint64(time.Now().UnixNano())
	startTime := now - uint64(time.Minute)
	resourceMetrics := []*metricspb.ResourceMetrics{
		{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-service"}}},
					{Key: "host.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-host"}}},
				},
			},
			SchemaUrl: "https://opentelemetry.io/schemas/1.4.0",
			ScopeMetrics: []*metricspb.ScopeMetrics{
				{
					Scope: &commonpb.InstrumentationScope{
						Name:    "test-scope",
						Version: "1.0.0",
					},
					Metrics: []*metricspb.Metric{
						{
							Name:        "http.requests.total",
							Description: "Total HTTP requests",
							Unit:        "{request}",
							Data: &metricspb.Metric_Sum{
								Sum: &metricspb.Sum{
									AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
									IsMonotonic:            true,
									DataPoints: []*metricspb.NumberDataPoint{
										{
											Attributes: []*commonpb.KeyValue{
												{Key: "method", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "GET"}}},
												{Key: "status", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "200"}}},
											},
											StartTimeUnixNano: startTime,
											TimeUnixNano:      now,
											Value:             &metricspb.NumberDataPoint_AsDouble{AsDouble: 1234},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	if err := insertRequest(ctx, store, resourceMetrics); err != nil {
		t.Fatalf("inserting sum metrics: %v", err)
	}

	var (
		serviceName            string
		metricName             string
		metricType             string
		value                  float64
		aggregationTemporality int32
		isMonotonic            bool
	)
	err := store.conn.QueryRow(ctx,
		"SELECT ServiceName, MetricName, MetricType, Value, AggregationTemporality, IsMonotonic FROM otel_metrics WHERE MetricName = 'http.requests.total'",
	).Scan(&serviceName, &metricName, &metricType, &value, &aggregationTemporality, &isMonotonic)
	if err != nil {
		t.Fatalf("querying sum: %v", err)
	}

	if serviceName != "test-service" {
		t.Errorf("expected ServiceName=test-service, got %s", serviceName)
	}
	if metricName != "http.requests.total" {
		t.Errorf("expected MetricName=http.requests.total, got %s", metricName)
	}
	if metricType != metricTypeSum {
		t.Errorf("expected MetricType=%s, got %s", metricTypeSum, metricType)
	}
	if value != 1234 {
		t.Errorf("expected Value=1234, got %f", value)
	}
	if aggregationTemporality != 2 {
		t.Errorf("expected AggregationTemporality=2, got %d", aggregationTemporality)
	}
	if !isMonotonic {
		t.Errorf("expected IsMonotonic=true, got false")
	}
}

func TestGRPCToClickHouse(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	// Start gRPC server wired to the ClickHouse store.
	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	colmetricspb.RegisterMetricsServiceServer(grpcServer, newServer("bufconn", store))
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("error serving server: %v", err)
		}
	}()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("connecting to grpc server: %v", err)
	}
	defer conn.Close()

	client := colmetricspb.NewMetricsServiceClient(conn)

	// Send a gauge metric via gRPC.
	now := uint64(time.Now().UnixNano())
	_, err = client.Export(ctx, &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "e2e-service"}}},
					},
				},
				ScopeMetrics: []*metricspb.ScopeMetrics{
					{
						Scope: &commonpb.InstrumentationScope{Name: "e2e-scope"},
						Metrics: []*metricspb.Metric{
							{
								Name: "e2e.gauge",
								Data: &metricspb.Metric_Gauge{
									Gauge: &metricspb.Gauge{
										DataPoints: []*metricspb.NumberDataPoint{
											{
												TimeUnixNano: now,
												Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: 99.9},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("exporting metrics via grpc: %v", err)
	}

	// Verify the metric landed in ClickHouse via the reconstruction view.
	var (
		svcName    string
		metricName string
		value      float64
	)
	err = store.conn.QueryRow(ctx,
		"SELECT ServiceName, MetricName, Value FROM otel_metrics WHERE MetricName = 'e2e.gauge'",
	).Scan(&svcName, &metricName, &value)
	if err != nil {
		t.Fatalf("querying clickhouse: %v", err)
	}
	if svcName != "e2e-service" {
		t.Errorf("expected ServiceName=e2e-service, got %s", svcName)
	}
	if value != 99.9 {
		t.Errorf("expected Value=99.9, got %f", value)
	}
}

// insertRequest maps an OTLP request and writes both tables, the same way the
// Export handler does.
func insertRequest(ctx context.Context, store *ClickHouseMetricsStore, resourceMetrics []*metricspb.ResourceMetrics) error {
	metadata, points := MapMetrics(resourceMetrics)
	if err := store.InsertMetadata(ctx, metadata); err != nil {
		return err
	}
	return store.InsertPoints(ctx, points)
}

// TestReconstructionEquivalence is a DB-backed property test: for an arbitrary
// OTLP request, ingesting it and reading back through the reconstruction view
// must yield exactly one row per datapoint, each carrying the metadata of its
// own series. This is the end-to-end proof that splitting metadata out and
// re-joining it is lossless. Each case truncates first so Hegel's shrink-replay
// stays deterministic, and OPTIMIZE FINAL settles merges before reading.
func TestReconstructionEquivalence(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	hegel.Test(t, func(ht *hegel.T) {
		generated := drawMetrics(ht)
		if err := store.conn.Exec(ctx, "TRUNCATE TABLE otel_metrics_point"); err != nil {
			ht.Fatalf("truncate points: %v", err)
		}
		if err := store.conn.Exec(ctx, "TRUNCATE TABLE otel_metrics_meta"); err != nil {
			ht.Fatalf("truncate meta: %v", err)
		}

		metadata, points := MapMetrics(generated.resourceMetrics)
		if len(points) == 0 {
			return
		}
		if err := store.InsertMetadata(ctx, metadata); err != nil {
			ht.Fatalf("insert metadata: %v", err)
		}
		if err := store.InsertPoints(ctx, points); err != nil {
			ht.Fatalf("insert points: %v", err)
		}
		if err := store.conn.Exec(ctx, "OPTIMIZE TABLE otel_metrics_meta FINAL"); err != nil {
			ht.Fatalf("optimize meta: %v", err)
		}

		rows, err := store.conn.Query(ctx,
			"SELECT Value, ServiceName, MetricName, MetricType FROM otel_metrics")
		if err != nil {
			ht.Fatalf("querying view: %v", err)
		}
		defer rows.Close()

		reconstructed := 0
		for rows.Next() {
			var (
				value       float64
				serviceName string
				metricName  string
				metricType  string
			)
			if err := rows.Scan(&value, &serviceName, &metricName, &metricType); err != nil {
				ht.Fatalf("scanning view row: %v", err)
			}
			reconstructed++

			expectation, ok := generated.byValue[value]
			if !ok {
				ht.Fatalf("view returned unknown datapoint value %v", value)
			}
			if serviceName != expectation.serviceName {
				ht.Fatalf("ServiceName: want %q got %q", expectation.serviceName, serviceName)
			}
			if metricName != expectation.metricName {
				ht.Fatalf("MetricName: want %q got %q", expectation.metricName, metricName)
			}
			if metricType != expectation.kind {
				ht.Fatalf("MetricType: want %q got %q", expectation.kind, metricType)
			}
		}
		if err := rows.Err(); err != nil {
			ht.Fatalf("iterating view rows: %v", err)
		}
		if reconstructed != len(points) {
			ht.Fatalf("view returned %d rows, expected %d (one per datapoint)", reconstructed, len(points))
		}
	}, hegel.WithTestCases(25))
}

// TestMetadataDedup is a DB-backed property test: inserting the same series
// metadata any number of times collapses, after merges, to exactly one row per
// fingerprint. This is the storage win — repeated metadata does not accumulate.
func TestMetadataDedup(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	hegel.Test(t, func(ht *hegel.T) {
		generated := drawMetrics(ht)
		metadata, _ := MapMetrics(generated.resourceMetrics)
		if len(metadata) == 0 {
			return
		}

		if err := store.conn.Exec(ctx, "TRUNCATE TABLE otel_metrics_meta"); err != nil {
			ht.Fatalf("truncate meta: %v", err)
		}

		// Insert the deduped metadata several times, simulating the same series
		// arriving across many batches.
		repeats := hegel.Draw(ht, hegel.Integers[int](1, 4))
		for range repeats {
			if err := store.InsertMetadata(ctx, metadata); err != nil {
				ht.Fatalf("insert metadata: %v", err)
			}
		}
		if err := store.conn.Exec(ctx, "OPTIMIZE TABLE otel_metrics_meta FINAL"); err != nil {
			ht.Fatalf("optimize meta: %v", err)
		}

		var rowCount uint64
		if err := store.conn.QueryRow(ctx, "SELECT count() FROM otel_metrics_meta FINAL").Scan(&rowCount); err != nil {
			ht.Fatalf("counting meta rows: %v", err)
		}
		if rowCount != uint64(len(metadata)) {
			ht.Fatalf("expected %d deduped metadata rows, got %d", len(metadata), rowCount)
		}
	}, hegel.WithTestCases(20))
}
