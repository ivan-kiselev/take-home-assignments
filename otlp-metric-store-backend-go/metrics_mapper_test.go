package main

import (
	"fmt"
	"math"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"hegel.dev/go/hegel"
)

// These property-based tests pin down the correctness invariants of MapMetrics -
// the function that turns an OTLP request into exactly what we store: deduped
// per-series metadata plus the datapoints that reference it by fingerprint.
// They assert on that storage-shaped output directly, not on any intermediate
// representation.

// datapointExpectation records, for a single generated datapoint, the facts we
// expect to see reflected in its stored point + the metadata of its series.
// Datapoints are keyed by their (globally unique) Value so a stored point can be
// matched back to its source. seriesKey is a faithful, test-controlled proxy for
// the datapoint's true series identity: two datapoints share a seriesKey if and
// only if they belong to the same series (same metric, same attributes, etc.).
type datapointExpectation struct {
	seriesKey         string
	kind              string // "gauge" or "sum"
	serviceName       string
	metricName        string
	metricDescription string
	metricUnit        string
	startNanos        uint64
	timeNanos         uint64
	aggTemporality    int32 // sum only
	isMonotonic       bool  // sum only
}

// generatedMetrics is the output of the OTLP request generator: the request plus
// an oracle mapping each datapoint's unique value to its expected facts.
type generatedMetrics struct {
	resourceMetrics []*metricspb.ResourceMetrics
	byValue         map[float64]datapointExpectation
}

func kvString(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   key,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}},
	}
}

// drawStringAttributes draws a small bag of string-valued attributes. Keys are
// short enough that they can never collide with reserved keys like
// "service.name", keeping the oracle unambiguous.
func drawStringAttributes(ht *hegel.T) []*commonpb.KeyValue {
	count := hegel.Draw(ht, hegel.Integers[int](0, 4))
	attributes := make([]*commonpb.KeyValue, count)
	for index := range attributes {
		key := hegel.Draw(ht, hegel.Text().MaxSize(8))
		value := hegel.Draw(ht, hegel.Text().MaxSize(8))
		attributes[index] = kvString(key, value)
	}
	return attributes
}

// drawMetrics builds an arbitrary OTLP request: several resources, each with
// several scopes, each with several gauge/sum metrics. Each metric carries a few
// distinct attribute combinations ("series"), each with several datapoints. Two
// devices keep the oracle exact:
//   - metric names are globally unique ("m<id>"), so a series is pinned by its
//     metric, and
//   - each combo gets a unique "_series" marker attribute appended last (so it
//     wins map deduplication), making every combo's attribute set distinct.
//
// Hence seriesKey = "<metricID>/<comboIndex>" is a faithful proxy for true
// series identity.
func drawMetrics(ht *hegel.T) generatedMetrics {
	result := generatedMetrics{byValue: map[float64]datapointExpectation{}}
	nextValue := 0.0
	nextMetricID := 0

	resourceCount := hegel.Draw(ht, hegel.Integers[int](0, 3))
	for range resourceCount {
		var resourceAttributes []*commonpb.KeyValue
		serviceName := ""
		if hegel.Draw(ht, hegel.Booleans()) {
			serviceName = hegel.Draw(ht, hegel.Text().MaxSize(10))
			resourceAttributes = append(resourceAttributes, kvString("service.name", serviceName))
		}
		resourceAttributes = append(resourceAttributes, drawStringAttributes(ht)...)

		resource := &metricspb.ResourceMetrics{
			Resource:  &resourcepb.Resource{Attributes: resourceAttributes},
			SchemaUrl: hegel.Draw(ht, hegel.Text().MaxSize(12)),
		}

		scopeCount := hegel.Draw(ht, hegel.Integers[int](0, 3))
		for range scopeCount {
			scopeMetrics := &metricspb.ScopeMetrics{
				Scope: &commonpb.InstrumentationScope{
					Name:                   hegel.Draw(ht, hegel.Text().MaxSize(10)),
					Version:                hegel.Draw(ht, hegel.Text().MaxSize(6)),
					Attributes:             drawStringAttributes(ht),
					DroppedAttributesCount: hegel.Draw(ht, hegel.Integers[uint32](0, 5)),
				},
				SchemaUrl: hegel.Draw(ht, hegel.Text().MaxSize(12)),
			}

			metricCount := hegel.Draw(ht, hegel.Integers[int](0, 4))
			for range metricCount {
				metricID := nextMetricID
				nextMetricID++

				metric := &metricspb.Metric{
					Name:        fmt.Sprintf("m%d", metricID),
					Description: hegel.Draw(ht, hegel.Text().MaxSize(12)),
					Unit:        hegel.Draw(ht, hegel.Text().MaxSize(4)),
				}
				isGauge := hegel.Draw(ht, hegel.Booleans())
				aggTemporality := hegel.Draw(ht, hegel.Integers[int32](0, 2))
				isMonotonic := hegel.Draw(ht, hegel.Booleans())

				var datapoints []*metricspb.NumberDataPoint
				comboCount := hegel.Draw(ht, hegel.Integers[int](1, 3))
				for comboIndex := range comboCount {
					// A unique marker attribute, appended last so it wins map
					// dedup, guarantees this combo's attribute set is distinct.
					attributes := append(drawStringAttributes(ht), kvString("_series", fmt.Sprintf("c%d", comboIndex)))
					seriesKey := fmt.Sprintf("%d/%d", metricID, comboIndex)

					datapointCount := hegel.Draw(ht, hegel.Integers[int](1, 4))
					for range datapointCount {
						value := nextValue
						nextValue++
						// OTLP nanos fit in int64 in practice (until year 2262);
						// keep them in that range so UnixNano round-trips exactly.
						startNanos := uint64(hegel.Draw(ht, hegel.Integers[int64](0, math.MaxInt64)))
						timeNanos := uint64(hegel.Draw(ht, hegel.Integers[int64](0, math.MaxInt64)))

						datapoints = append(datapoints, &metricspb.NumberDataPoint{
							Attributes:        attributes,
							StartTimeUnixNano: startNanos,
							TimeUnixNano:      timeNanos,
							Value:             &metricspb.NumberDataPoint_AsDouble{AsDouble: value},
						})

						expectation := datapointExpectation{
							seriesKey:         seriesKey,
							serviceName:       serviceName,
							metricName:        metric.Name,
							metricDescription: metric.Description,
							metricUnit:        metric.Unit,
							startNanos:        startNanos,
							timeNanos:         timeNanos,
						}
						if isGauge {
							expectation.kind = metricTypeGauge
						} else {
							expectation.kind = metricTypeSum
							expectation.aggTemporality = aggTemporality
							expectation.isMonotonic = isMonotonic
						}
						result.byValue[value] = expectation
					}
				}

				if isGauge {
					metric.Data = &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: datapoints}}
				} else {
					metric.Data = &metricspb.Metric_Sum{Sum: &metricspb.Sum{
						AggregationTemporality: metricspb.AggregationTemporality(aggTemporality),
						IsMonotonic:            isMonotonic,
						DataPoints:             datapoints,
					}}
				}
				scopeMetrics.Metrics = append(scopeMetrics.Metrics, metric)
			}
			resource.ScopeMetrics = append(resource.ScopeMetrics, scopeMetrics)
		}
		result.resourceMetrics = append(result.resourceMetrics, resource)
	}
	return result
}

// Property: every datapoint becomes exactly one stored point - none dropped,
// none duplicated.
func TestMapMetrics_ExactlyOnePointPerDatapoint(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		generated := drawMetrics(ht)
		_, points := MapMetrics(generated.resourceMetrics)
		if len(points) != len(generated.byValue) {
			ht.Fatalf("expected %d points, got %d", len(generated.byValue), len(points))
		}
	})
}

// Property: each stored point carries the value and timestamps of its source
// datapoint, exactly once (bijection).
func TestMapMetrics_PointMatchesSourceDatapoint(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		generated := drawMetrics(ht)
		_, points := MapMetrics(generated.resourceMetrics)

		seen := map[float64]bool{}
		for _, point := range points {
			expectation, ok := generated.byValue[point.Value]
			if !ok {
				ht.Fatalf("point with unknown value %v", point.Value)
			}
			if seen[point.Value] {
				ht.Fatalf("datapoint value %v stored more than once", point.Value)
			}
			seen[point.Value] = true

			if uint64(point.TimeUnix.UnixNano()) != expectation.timeNanos {
				ht.Fatalf("TimeUnix: want %d got %d", expectation.timeNanos, point.TimeUnix.UnixNano())
			}
			if uint64(point.StartTimeUnix.UnixNano()) != expectation.startNanos {
				ht.Fatalf("StartTimeUnix: want %d got %d", expectation.startNanos, point.StartTimeUnix.UnixNano())
			}
		}
	})
}

// Property: the fingerprint is a faithful series identity - two datapoints get
// the same fingerprint if and only if they belong to the same series. This is
// the core guarantee the lookup-table design rests on: same series collapses to
// one key, different series never collide.
func TestMapMetrics_FingerprintMatchesSeriesIdentity(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		generated := drawMetrics(ht)
		_, points := MapMetrics(generated.resourceMetrics)

		fingerprintByValue := make(map[float64]uint64, len(points))
		for _, point := range points {
			fingerprintByValue[point.Value] = point.Fingerprint
		}

		fingerprintForSeries := map[string]uint64{}
		seriesForFingerprint := map[uint64]string{}
		for value, expectation := range generated.byValue {
			fingerprint := fingerprintByValue[value]

			// Same series ⇒ same fingerprint.
			if existing, ok := fingerprintForSeries[expectation.seriesKey]; ok && existing != fingerprint {
				ht.Fatalf("series %q produced two fingerprints: %d and %d", expectation.seriesKey, existing, fingerprint)
			}
			fingerprintForSeries[expectation.seriesKey] = fingerprint

			// Different series ⇒ different fingerprint (no collisions).
			if existing, ok := seriesForFingerprint[fingerprint]; ok && existing != expectation.seriesKey {
				ht.Fatalf("fingerprint %d shared by series %q and %q", fingerprint, existing, expectation.seriesKey)
			}
			seriesForFingerprint[fingerprint] = expectation.seriesKey
		}
	})
}

// Property: metadata is deduplicated to exactly one record per distinct series
// (fingerprint) seen in the batch - no duplicates in the returned slice, and one
// record per distinct point fingerprint.
func TestMapMetrics_MetadataDedupedPerSeries(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		generated := drawMetrics(ht)
		metadata, points := MapMetrics(generated.resourceMetrics)

		distinctPointFingerprints := map[uint64]struct{}{}
		for _, point := range points {
			distinctPointFingerprints[point.Fingerprint] = struct{}{}
		}

		metadataFingerprints := map[uint64]struct{}{}
		for _, series := range metadata {
			if _, dup := metadataFingerprints[series.Fingerprint]; dup {
				ht.Fatalf("duplicate metadata record for fingerprint %d", series.Fingerprint)
			}
			metadataFingerprints[series.Fingerprint] = struct{}{}
		}

		if len(metadataFingerprints) != len(distinctPointFingerprints) {
			ht.Fatalf("expected %d metadata records, got %d", len(distinctPointFingerprints), len(metadataFingerprints))
		}
		for fingerprint := range distinctPointFingerprints {
			if _, ok := metadataFingerprints[fingerprint]; !ok {
				ht.Fatalf("no metadata record for point fingerprint %d", fingerprint)
			}
		}
	})
}

// Property: each metadata record carries the descriptive fields of its series.
// We correlate a metadata record to an expected series via any datapoint that
// shares its fingerprint.
func TestMapMetrics_MetadataContentMatchesSeries(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		generated := drawMetrics(ht)
		metadata, points := MapMetrics(generated.resourceMetrics)

		expectationByFingerprint := map[uint64]datapointExpectation{}
		for _, point := range points {
			expectationByFingerprint[point.Fingerprint] = generated.byValue[point.Value]
		}

		for _, series := range metadata {
			expectation := expectationByFingerprint[series.Fingerprint]
			if series.MetricType != expectation.kind {
				ht.Fatalf("MetricType: want %q got %q", expectation.kind, series.MetricType)
			}
			if series.ServiceName != expectation.serviceName {
				ht.Fatalf("ServiceName: want %q got %q", expectation.serviceName, series.ServiceName)
			}
			if series.MetricName != expectation.metricName {
				ht.Fatalf("MetricName: want %q got %q", expectation.metricName, series.MetricName)
			}
			if series.MetricDescription != expectation.metricDescription {
				ht.Fatalf("MetricDescription: want %q got %q", expectation.metricDescription, series.MetricDescription)
			}
			if series.MetricUnit != expectation.metricUnit {
				ht.Fatalf("MetricUnit: want %q got %q", expectation.metricUnit, series.MetricUnit)
			}
			if series.MetricType == metricTypeSum {
				if series.AggregationTemporality != expectation.aggTemporality {
					ht.Fatalf("AggregationTemporality: want %d got %d", expectation.aggTemporality, series.AggregationTemporality)
				}
				if series.IsMonotonic != expectation.isMonotonic {
					ht.Fatalf("IsMonotonic: want %v got %v", expectation.isMonotonic, series.IsMonotonic)
				}
			}
		}
	})
}

// Property: mapping is deterministic - the same request maps to identical
// fingerprints on every call. Fingerprints must be stable across calls,
// batches, restarts, and instances for the lookup table to work.
func TestMapMetrics_FingerprintDeterministic(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		generated := drawMetrics(ht)
		_, firstPoints := MapMetrics(generated.resourceMetrics)
		_, secondPoints := MapMetrics(generated.resourceMetrics)

		if len(firstPoints) != len(secondPoints) {
			ht.Fatalf("nondeterministic point count: %d vs %d", len(firstPoints), len(secondPoints))
		}
		for i := range firstPoints {
			if firstPoints[i].Fingerprint != secondPoints[i].Fingerprint {
				ht.Fatalf("nondeterministic fingerprint at %d: %d vs %d", i, firstPoints[i].Fingerprint, secondPoints[i].Fingerprint)
			}
		}
	})
}
