package main

import (
	"context"
	"log/slog"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
)

type dash0MetricsServiceServer struct {
	addr  string
	store MetricsStore

	colmetricspb.UnimplementedMetricsServiceServer
}

func newServer(addr string, store MetricsStore) colmetricspb.MetricsServiceServer {
	return &dash0MetricsServiceServer{addr: addr, store: store}
}

func (m *dash0MetricsServiceServer) Export(ctx context.Context, request *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	slog.DebugContext(ctx, "Received ExportMetricsServiceRequest")
	metricsReceivedCounter.Add(ctx, 1)

	if m.store != nil {
		metadata, points := MapMetrics(request.GetResourceMetrics())

		// Insert metadata first so the lookup row exists before (or with) the
		// datapoints that reference it.
		if len(metadata) > 0 {
			if err := m.store.InsertMetadata(ctx, metadata); err != nil {
				return nil, err
			}
		}
		if len(points) > 0 {
			if err := m.store.InsertPoints(ctx, points); err != nil {
				return nil, err
			}
		}
		slog.DebugContext(ctx, "stored metrics",
			slog.Int("series", len(metadata)),
			slog.Int("datapoints", len(points)))
	}

	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}
