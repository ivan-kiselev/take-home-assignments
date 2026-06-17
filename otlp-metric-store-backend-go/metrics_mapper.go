package main

import (
	"encoding/binary"
	"fmt"
	"sort"
	"time"

	"github.com/cespare/xxhash/v2"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// Metric type discriminators stored alongside the metadata. They are part of a
// series' identity, so a gauge and a sum that are otherwise identical receive
// distinct fingerprints and never collide in the same points table.
const (
	metricTypeGauge = "gauge"
	metricTypeSum   = "sum"
)

// serviceName extracts the service.name from resource attributes, returning "" if not found.
func serviceName(resource *resourcepb.Resource) string {
	if resource == nil {
		return ""
	}
	for _, attr := range resource.GetAttributes() {
		if attr.GetKey() == "service.name" {
			return attr.GetValue().GetStringValue()
		}
	}
	return ""
}

// kvToMap converts a slice of OTLP KeyValue pairs to a Go map.
func kvToMap(attrs []*commonpb.KeyValue) map[string]string {
	m := make(map[string]string, len(attrs))
	for _, kv := range attrs {
		m[kv.GetKey()] = anyValueToString(kv.GetValue())
	}
	return m
}

// anyValueToString converts an OTLP AnyValue to its string representation.
func anyValueToString(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	switch v.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return v.GetStringValue()
	case *commonpb.AnyValue_IntValue:
		return fmt.Sprintf("%d", v.GetIntValue())
	case *commonpb.AnyValue_DoubleValue:
		return fmt.Sprintf("%g", v.GetDoubleValue())
	case *commonpb.AnyValue_BoolValue:
		return fmt.Sprintf("%t", v.GetBoolValue())
	default:
		return fmt.Sprintf("%v", v)
	}
}

// nanosToTime converts a uint64 nanoseconds-since-epoch to time.Time.
func nanosToTime(nanos uint64) time.Time {
	return time.Unix(0, int64(nanos))
}

// numberDataPointValue extracts the float64 value from a NumberDataPoint.
func numberDataPointValue(dp *metricspb.NumberDataPoint) float64 {
	switch v := dp.GetValue().(type) {
	case *metricspb.NumberDataPoint_AsDouble:
		return v.AsDouble
	case *metricspb.NumberDataPoint_AsInt:
		return float64(v.AsInt)
	default:
		return 0
	}
}

// MapMetrics converts an OTLP request into the records we persist:
//
//   - a set of unique series-metadata records (the lookup table), one per
//     distinct series identity seen in the request, and
//   - the scalar datapoints, each referencing its series by Fingerprint.
//
// A series' identity is the full set of fields that stay constant across its
// datapoints - resource, scope, metric, datapoint attributes, and type - folded
// into the Fingerprint. Metadata is deduplicated within the batch here; cross-
// batch and cross-instance duplicates are collapsed by ClickHouse's
// ReplacingMergeTree on the same Fingerprint. Only Gauge and Sum metrics are
// mapped; other metric types are skipped.
func MapMetrics(resourceMetrics []*metricspb.ResourceMetrics) ([]SeriesMetadata, []DataPoint) {
	var metadata []SeriesMetadata
	var points []DataPoint
	seenFingerprints := make(map[uint64]struct{})

	for _, rm := range resourceMetrics {
		resource := rm.GetResource()
		svcName := serviceName(resource)
		resourceAttrs := kvToMap(resource.GetAttributes())
		resourceSchemaURL := rm.GetSchemaUrl()

		for _, sm := range rm.GetScopeMetrics() {
			scope := sm.GetScope()
			scopeAttrs := kvToMap(scope.GetAttributes())

			for _, metric := range sm.GetMetrics() {
				// Series metadata shared by every datapoint of this metric,
				// before the (per-datapoint) attributes are attached.
				base := SeriesMetadata{
					ResourceAttributes:    resourceAttrs,
					ResourceSchemaUrl:     resourceSchemaURL,
					ScopeName:             scope.GetName(),
					ScopeVersion:          scope.GetVersion(),
					ScopeAttributes:       scopeAttrs,
					ScopeDroppedAttrCount: scope.GetDroppedAttributesCount(),
					ScopeSchemaUrl:        sm.GetSchemaUrl(),
					ServiceName:           svcName,
					MetricName:            metric.GetName(),
					MetricDescription:     metric.GetDescription(),
					MetricUnit:            metric.GetUnit(),
				}

				var datapoints []*metricspb.NumberDataPoint
				switch {
				case metric.GetGauge() != nil:
					base.MetricType = metricTypeGauge
					datapoints = metric.GetGauge().GetDataPoints()
				case metric.GetSum() != nil:
					sum := metric.GetSum()
					base.MetricType = metricTypeSum
					base.AggregationTemporality = int32(sum.GetAggregationTemporality())
					base.IsMonotonic = sum.GetIsMonotonic()
					datapoints = sum.GetDataPoints()
				default:
					continue
				}

				for _, dp := range datapoints {
					series := base
					series.Attributes = kvToMap(dp.GetAttributes())
					fingerprint := fingerprintSeries(series)
					series.Fingerprint = fingerprint

					points = append(points, DataPoint{
						Fingerprint:   fingerprint,
						StartTimeUnix: nanosToTime(dp.GetStartTimeUnixNano()),
						TimeUnix:      nanosToTime(dp.GetTimeUnixNano()),
						Value:         numberDataPointValue(dp),
						Flags:         dp.GetFlags(),
					})

					if _, ok := seenFingerprints[fingerprint]; !ok {
						seenFingerprints[fingerprint] = struct{}{}
						metadata = append(metadata, series)
					}
				}
			}
		}
	}
	return metadata, points
}

// fingerprintSeries computes a stable 64-bit identity for a series from every
// field that distinguishes one series from another. It is deterministic and
// order-independent (map keys are sorted), so the same logical series always
// hashes to the same value across datapoints, batches, restarts, and instances.
// All fields are length-prefixed so that no concatenation of two series can
// alias another (e.g. "ab"+"c" cannot collide with "a"+"bc").
func fingerprintSeries(s SeriesMetadata) uint64 {
	digest := xxhash.New()
	hashString(digest, s.MetricType)
	hashString(digest, s.ServiceName)
	hashMap(digest, s.ResourceAttributes)
	hashString(digest, s.ResourceSchemaUrl)
	hashString(digest, s.ScopeName)
	hashString(digest, s.ScopeVersion)
	hashMap(digest, s.ScopeAttributes)
	hashUint32(digest, s.ScopeDroppedAttrCount)
	hashString(digest, s.ScopeSchemaUrl)
	hashString(digest, s.MetricName)
	hashString(digest, s.MetricDescription)
	hashString(digest, s.MetricUnit)
	hashMap(digest, s.Attributes)
	hashUint32(digest, uint32(s.AggregationTemporality))
	hashBool(digest, s.IsMonotonic)
	return digest.Sum64()
}

func hashString(digest *xxhash.Digest, value string) {
	var lengthPrefix [8]byte
	binary.LittleEndian.PutUint64(lengthPrefix[:], uint64(len(value)))
	_, _ = digest.Write(lengthPrefix[:])
	_, _ = digest.WriteString(value)
}

func hashMap(digest *xxhash.Digest, m map[string]string) {
	var countPrefix [8]byte
	binary.LittleEndian.PutUint64(countPrefix[:], uint64(len(m)))
	_, _ = digest.Write(countPrefix[:])

	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		hashString(digest, key)
		hashString(digest, m[key])
	}
}

func hashUint32(digest *xxhash.Digest, value uint32) {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], value)
	_, _ = digest.Write(buf[:])
}

func hashBool(digest *xxhash.Digest, value bool) {
	var buf [1]byte
	if value {
		buf[0] = 1
	}
	_, _ = digest.Write(buf[:])
}
