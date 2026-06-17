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

// QueryRange is the two-step hot read path for a service/metric over a time
// window: first resolve the matching fingerprints from the small lookup table
// (an index range scan on the (ServiceName, MetricName, Fingerprint) sort key,
// deduped with DISTINCT — no FINAL needed), then range-scan the datapoints for
// those fingerprints. The points scan hits the (Fingerprint, TimeUnix) order key
// and prunes partitions by date, so no full scan and no JOIN/FINAL are involved.
func (s *ClickHouseMetricsStore) QueryRange(ctx context.Context, serviceName, metricName string, start, end time.Time) ([]DataPoint, error) {
	fingerprintRows, err := s.conn.Query(ctx,
		"SELECT DISTINCT Fingerprint FROM otel_metrics_meta WHERE ServiceName = $1 AND MetricName = $2",
		serviceName, metricName)
	if err != nil {
		return nil, fmt.Errorf("resolving fingerprints: %w", err)
	}
	var fingerprints []uint64
	for fingerprintRows.Next() {
		var fingerprint uint64
		if err := fingerprintRows.Scan(&fingerprint); err != nil {
			_ = fingerprintRows.Close()
			return nil, fmt.Errorf("scanning fingerprint: %w", err)
		}
		fingerprints = append(fingerprints, fingerprint)
	}
	_ = fingerprintRows.Close()
	if err := fingerprintRows.Err(); err != nil {
		return nil, fmt.Errorf("iterating fingerprints: %w", err)
	}
	if len(fingerprints) == 0 {
		return nil, nil
	}

	pointRows, err := s.conn.Query(ctx, `
		SELECT Fingerprint, StartTimeUnix, TimeUnix, Value, Flags
		FROM otel_metrics_point
		WHERE Fingerprint IN ($1) AND TimeUnix BETWEEN $2 AND $3`,
		fingerprints, start, end)
	if err != nil {
		return nil, fmt.Errorf("querying points: %w", err)
	}
	defer pointRows.Close()

	var points []DataPoint
	for pointRows.Next() {
		var p DataPoint
		if err := pointRows.Scan(&p.Fingerprint, &p.StartTimeUnix, &p.TimeUnix, &p.Value, &p.Flags); err != nil {
			return nil, fmt.Errorf("scanning point: %w", err)
		}
		points = append(points, p)
	}
	return points, pointRows.Err()
}

// Close closes the underlying ClickHouse connection.
func (s *ClickHouseMetricsStore) Close() error {
	return s.conn.Close()
}
