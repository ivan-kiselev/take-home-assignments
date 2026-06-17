package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// SeriesMetadata is one row of the lookup table: the identity and descriptive
// metadata of a single metric series, addressed by its Fingerprint. All
// datapoints of the series reference this row by Fingerprint instead of
// repeating the metadata.
type SeriesMetadata struct {
	Fingerprint            uint64
	MetricType             string
	ResourceAttributes     map[string]string
	ResourceSchemaUrl      string
	ScopeName              string
	ScopeVersion           string
	ScopeAttributes        map[string]string
	ScopeDroppedAttrCount  uint32
	ScopeSchemaUrl         string
	ServiceName            string
	MetricName             string
	MetricDescription      string
	MetricUnit             string
	Attributes             map[string]string
	AggregationTemporality int32 // sum only; zero for gauges
	IsMonotonic            bool  // sum only; false for gauges
}

// DataPoint is one row of the points table: a scalar observation that references
// its series by Fingerprint. This is the high-volume table.
type DataPoint struct {
	Fingerprint   uint64
	StartTimeUnix time.Time
	TimeUnix      time.Time
	Value         float64
	Flags         uint32
}

// MetricsStore defines the interface for storing metrics in ClickHouse.
type MetricsStore interface {
	CreateTables(ctx context.Context) error
	InsertMetadata(ctx context.Context, rows []SeriesMetadata) error
	InsertPoints(ctx context.Context, rows []DataPoint) error
	Close() error
}

// ClickHouseMetricsStore implements MetricsStore using a ClickHouse connection.
type ClickHouseMetricsStore struct {
	conn driver.Conn
}

// NewClickHouseMetricsStore creates a new ClickHouseMetricsStore connected to the given address.
func NewClickHouseMetricsStore(ctx context.Context, addr string, database string, username string, password string) (*ClickHouseMetricsStore, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("opening clickhouse connection: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("pinging clickhouse: %w", err)
	}
	return &ClickHouseMetricsStore{conn: conn}, nil
}

// CreateTables executes DDL for the metadata lookup table, the points table, and
// the reconstruction view.
func (s *ClickHouseMetricsStore) CreateTables(ctx context.Context) error {
	ddls := []string{
		createMetaTableSQL,
		createPointTableSQL,
		createMetricsViewSQL,
	}
	for _, ddl := range ddls {
		if err := s.conn.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("creating schema object: %w", err)
		}
	}
	return nil
}

// InsertMetadata batch-inserts series-metadata rows into otel_metrics_meta.
// Rows are expected to be deduplicated by the caller; ReplacingMergeTree
// collapses any remaining duplicates by Fingerprint during background merges.
func (s *ClickHouseMetricsStore) InsertMetadata(ctx context.Context, rows []SeriesMetadata) error {
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO otel_metrics_meta")
	if err != nil {
		return fmt.Errorf("preparing metadata batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(
			r.Fingerprint,
			r.MetricType,
			r.ResourceAttributes,
			r.ResourceSchemaUrl,
			r.ScopeName,
			r.ScopeVersion,
			r.ScopeAttributes,
			r.ScopeDroppedAttrCount,
			r.ScopeSchemaUrl,
			r.ServiceName,
			r.MetricName,
			r.MetricDescription,
			r.MetricUnit,
			r.Attributes,
			r.AggregationTemporality,
			r.IsMonotonic,
		); err != nil {
			return fmt.Errorf("appending metadata row: %w", err)
		}
	}
	return batch.Send()
}

// InsertPoints batch-inserts datapoint rows into otel_metrics_point.
func (s *ClickHouseMetricsStore) InsertPoints(ctx context.Context, rows []DataPoint) error {
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO otel_metrics_point")
	if err != nil {
		return fmt.Errorf("preparing points batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(
			r.Fingerprint,
			r.StartTimeUnix,
			r.TimeUnix,
			r.Value,
			r.Flags,
		); err != nil {
			return fmt.Errorf("appending point row: %w", err)
		}
	}
	return batch.Send()
}

// Close closes the underlying ClickHouse connection.
func (s *ClickHouseMetricsStore) Close() error {
	return s.conn.Close()
}
