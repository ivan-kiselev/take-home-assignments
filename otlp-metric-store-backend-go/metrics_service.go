package main

import (
	"context"
	"log/slog"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
)

type dash0MetricsServiceServer struct {
	addr     string
	ingester *Ingester

	colmetricspb.UnimplementedMetricsServiceServer
}

func newServer(addr string, ingester *Ingester) colmetricspb.MetricsServiceServer {
	return &dash0MetricsServiceServer{addr: addr, ingester: ingester}
}

func (m *dash0MetricsServiceServer) Export(ctx context.Context, request *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	slog.DebugContext(ctx, "Received ExportMetricsServiceRequest")
	metricsReceivedCounter.Add(ctx, 1)

	if m.ingester != nil {
		metadata, points := MapMetrics(request.GetResourceMetrics())
		// Hand off to the async batching writer and return; the flusher writes
		// metadata + points in large batches.
		if err := m.ingester.Enqueue(ctx, metadata, points); err != nil {
			return nil, err
		}
		slog.DebugContext(ctx, "enqueued metrics",
			slog.Int("series", len(metadata)),
			slog.Int("datapoints", len(points)))
	}

	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}
