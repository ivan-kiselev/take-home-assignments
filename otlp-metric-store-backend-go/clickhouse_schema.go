package main

// The schema splits each metric into two tables:
//
//   - otel_metrics_meta — the lookup table. One row per distinct series,
//     addressed by Fingerprint. ReplacingMergeTree collapses duplicate inserts
//     of the same Fingerprint to a single row during background merges. Because
//     every duplicate row for a Fingerprint is byte-identical by construction,
//     reads never need FINAL for correctness (the view uses it only to avoid
//     fan-out in the JOIN). Ordered by (ServiceName, MetricName, Fingerprint) so
//     that resolving the fingerprints for a given service/metric is an index
//     range scan, not a full scan.
//
//   - otel_metrics_point — the high-volume datapoints. Partitioned by day so a
//     time-bounded query prunes to the relevant parts, and ordered by
//     (Fingerprint, TimeUnix) so each series is a contiguous, time-sorted run.
//
// Only Gauge and Sum (scalar) datapoints are modelled here; the points table is
// intentionally type-agnostic (value + timestamp) and the metric type lives in
// the metadata. Histogram/summary families would get their own points tables.

const createMetaTableSQL = `
CREATE TABLE IF NOT EXISTS otel_metrics_meta (
    Fingerprint UInt64 CODEC(ZSTD(1)),
    MetricType LowCardinality(String) CODEC(ZSTD(1)),
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ResourceSchemaUrl String CODEC(ZSTD(1)),
    ScopeName String CODEC(ZSTD(1)),
    ScopeVersion String CODEC(ZSTD(1)),
    ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeDroppedAttrCount UInt32 CODEC(ZSTD(1)),
    ScopeSchemaUrl String CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    MetricName LowCardinality(String) CODEC(ZSTD(1)),
    MetricDescription String CODEC(ZSTD(1)),
    MetricUnit String CODEC(ZSTD(1)),
    Attributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    AggregationTemporality Int32 CODEC(ZSTD(1)),
    IsMonotonic Bool CODEC(ZSTD(1)),

    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_key mapKeys(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_value mapValues(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = ReplacingMergeTree()
ORDER BY (ServiceName, MetricName, Fingerprint)
SETTINGS index_granularity = 8192;
`

const createPointTableSQL = `
CREATE TABLE IF NOT EXISTS otel_metrics_point (
    Fingerprint UInt64 CODEC(ZSTD(1)),
    StartTimeUnix DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    TimeUnix DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    Value Float64 CODEC(ZSTD(1)),
    Flags UInt32 CODEC(ZSTD(1))
) ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (Fingerprint, TimeUnix)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
`

// createMetricsViewSQL reconstructs the wide row by joining points to their
// metadata. The metadata side uses FINAL so that an as-yet-unmerged duplicate
// metadata row cannot fan a single datapoint out into multiple result rows.
const createMetricsViewSQL = `
CREATE VIEW IF NOT EXISTS otel_metrics AS
SELECT
    p.Fingerprint AS Fingerprint,
    m.MetricType AS MetricType,
    m.ResourceAttributes AS ResourceAttributes,
    m.ResourceSchemaUrl AS ResourceSchemaUrl,
    m.ScopeName AS ScopeName,
    m.ScopeVersion AS ScopeVersion,
    m.ScopeAttributes AS ScopeAttributes,
    m.ScopeDroppedAttrCount AS ScopeDroppedAttrCount,
    m.ScopeSchemaUrl AS ScopeSchemaUrl,
    m.ServiceName AS ServiceName,
    m.MetricName AS MetricName,
    m.MetricDescription AS MetricDescription,
    m.MetricUnit AS MetricUnit,
    m.Attributes AS Attributes,
    m.AggregationTemporality AS AggregationTemporality,
    m.IsMonotonic AS IsMonotonic,
    p.StartTimeUnix AS StartTimeUnix,
    p.TimeUnix AS TimeUnix,
    p.Value AS Value,
    p.Flags AS Flags
FROM otel_metrics_point AS p
INNER JOIN (SELECT * FROM otel_metrics_meta FINAL) AS m USING (Fingerprint);
`
