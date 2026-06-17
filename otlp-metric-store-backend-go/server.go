package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"log/slog"
	"net"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	listenAddr            = flag.String("listenAddr", "localhost:4317", "The listen address")
	maxReceiveMessageSize = flag.Int("maxReceiveMessageSize", 16777216, "The max message size in bytes the server can receive")

	clickhouseAddr     = flag.String("clickhouseAddr", "", "ClickHouse address (host:port); empty disables storage (metrics are received but not persisted)")
	clickhouseDatabase = flag.String("clickhouseDatabase", "default", "ClickHouse database")
	clickhouseUser     = flag.String("clickhouseUser", "default", "ClickHouse username")
	clickhousePassword = flag.String("clickhousePassword", "", "ClickHouse password")
)

const name = "dash0.com/otlp-log-processor-backend"

var (
	meter                  = otel.Meter(name)
	logger                 = otelslog.NewLogger(name)
	metricsReceivedCounter metric.Int64Counter
)

func init() {
	var err error
	metricsReceivedCounter, err = meter.Int64Counter("com.dash0.homeexercise.metrics.received",
		metric.WithDescription("The number of metrics received by otlp-metrics-processor-backend"),
		metric.WithUnit("{metric}"))
	if err != nil {
		panic(err)
	}
}

func main() {
	if err := run(); err != nil {
		log.Fatalln(err)
	}
}

func run() (err error) {
	slog.SetDefault(logger)
	logger.Info("Starting application")

	// Set up OpenTelemetry.
	otelShutdown, err := setupOTelSDK(context.Background())
	if err != nil {
		return
	}

	// Handle shutdown properly so nothing leaks.
	defer func() {
		err = errors.Join(err, otelShutdown(context.Background()))
	}()

	flag.Parse()

	// Wire ClickHouse storage when an address is configured. Without it the
	// server still accepts metrics but does not persist them.
	var ingester *Ingester
	if *clickhouseAddr != "" {
		store, storeErr := NewClickHouseMetricsStore(context.Background(), *clickhouseAddr, *clickhouseDatabase, *clickhouseUser, *clickhousePassword)
		if storeErr != nil {
			return storeErr
		}
		if storeErr := store.CreateTables(context.Background()); storeErr != nil {
			return storeErr
		}
		ingester = NewIngester(store, IngesterConfig{})
		// Drain the buffer before closing the connection (LIFO: ingester first).
		defer func() { err = errors.Join(err, store.Close()) }()
		defer func() { err = errors.Join(err, ingester.Close(context.Background())) }()
		slog.Info("ClickHouse storage enabled", slog.String("addr", *clickhouseAddr))
	} else {
		slog.Warn("no -clickhouseAddr set; metrics will be received but not stored")
	}

	slog.Debug("Starting listener", slog.String("listenAddr", *listenAddr))
	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.MaxRecvMsgSize(*maxReceiveMessageSize),
		grpc.Creds(insecure.NewCredentials()),
	)
	colmetricspb.RegisterMetricsServiceServer(grpcServer, newServer(*listenAddr, ingester))

	slog.Debug("Starting gRPC server")

	return grpcServer.Serve(listener)
}
