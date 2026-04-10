package main

import (
	"context"
	"math"
	"slices"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

var (
	quantiles      = []float64{0.5, 0.75, 0.95, 0.99}
	quantileLabels = []string{"0.5", "0.75", "0.95", "0.99"}
)

type dataPlaneKey struct {
	AccountName, Database, Collection, Operation, StatusCode, PartitionKeyRangeID string
}

type dataPlaneAgg struct {
	Count     int64
	Durations []float64
	RUCharge  float64
}

// buildMinuteMetrics aggregates diagnostic records from a single minute into OTLP metrics.
// The partition mapping is used to resolve partition_key_range_id for DataPlaneRequests.
// The timestamp ts should be at :00 seconds of the minute.
func buildMinuteMetrics(records []DiagnosticRecord, mapping *partitionMapping, cluster string, ts time.Time) []metricdata.Metrics {
	dpAgg := map[dataPlaneKey]*dataPlaneAgg{}

	for i := range records {
		r := &records[i]
		switch r.Category {
		case "DataPlaneRequests":
			p := r.Properties
			rangeID := ""
			if mapping != nil {
				rangeID = mapping.resolve(getStringProp(p, "requestResourceId"))
			}
			key := dataPlaneKey{
				AccountName:         extractAccountName(r.ResourceID),
				Database:            getStringProp(p, "databaseName"),
				Collection:          getStringProp(p, "collectionName"),
				Operation:           r.OperationName,
				StatusCode:          getStringProp(p, "statusCode"),
				PartitionKeyRangeID: rangeID,
			}
			agg := dpAgg[key]
			if agg == nil {
				agg = &dataPlaneAgg{}
				dpAgg[key] = agg
			}
			agg.Count++
			if dur := getFloat64Prop(p, "duration"); dur >= 0 {
				agg.Durations = append(agg.Durations, dur/1000) // ms → seconds
			}
			if ru := getFloat64Prop(p, "requestCharge"); ru >= 0 {
				agg.RUCharge += ru
			}
		}
	}

	var metrics []metricdata.Metrics

	// DataPlaneRequests: request count + duration percentiles + RU charge.
	if len(dpAgg) > 0 {
		var countPoints []metricdata.DataPoint[float64]
		var durationPoints []metricdata.DataPoint[float64]
		var ruPoints []metricdata.DataPoint[float64]

		for key, agg := range dpAgg {
			baseAttrs := []attribute.KeyValue{
				attribute.String("cluster", cluster),
				attribute.String("account_name", key.AccountName),
				attribute.String("database", key.Database),
				attribute.String("collection", key.Collection),
				attribute.String("operation", key.Operation),
				attribute.String("status_code", key.StatusCode),
				attribute.String("partition_key_range_id", key.PartitionKeyRangeID),
			}
			countPoints = append(countPoints, metricdata.DataPoint[float64]{
				Attributes: attribute.NewSet(baseAttrs...),
				StartTime:  ts,
				Time:       ts,
				Value:      float64(agg.Count),
			})

			ruPoints = append(ruPoints, metricdata.DataPoint[float64]{
				Attributes: attribute.NewSet(baseAttrs...),
				StartTime:  ts,
				Time:       ts,
				Value:      agg.RUCharge,
			})

			// Duration: avg + percentiles + max.
			if len(agg.Durations) > 0 {
				slices.Sort(agg.Durations)

				// Avg.
				var sum float64
				for _, d := range agg.Durations {
					sum += d
				}
				durationPoints = append(durationPoints, metricdata.DataPoint[float64]{
					Attributes: attribute.NewSet(append(baseAttrs, attribute.String("quantile", "avg"))...),
					StartTime:  ts,
					Time:       ts,
					Value:      sum / float64(len(agg.Durations)),
				})

				// Percentiles.
				for i, q := range quantiles {
					durationPoints = append(durationPoints, metricdata.DataPoint[float64]{
						Attributes: attribute.NewSet(append(baseAttrs, attribute.String("quantile", quantileLabels[i]))...),
						StartTime:  ts,
						Time:       ts,
						Value:      computePercentile(agg.Durations, q),
					})
				}
				// Max (quantile "1").
				durationPoints = append(durationPoints, metricdata.DataPoint[float64]{
					Attributes: attribute.NewSet(append(baseAttrs, attribute.String("quantile", "1"))...),
					StartTime:  ts,
					Time:       ts,
					Value:      agg.Durations[len(agg.Durations)-1],
				})
			}
		}

		metrics = append(metrics, metricdata.Metrics{
			Name:        "cosmosdb_data_plane_requests_1m",
			Description: "Number of CosmosDB data plane requests per minute.",
			Data: metricdata.Gauge[float64]{
				DataPoints: countPoints,
			},
		})

		if len(durationPoints) > 0 {
			metrics = append(metrics, metricdata.Metrics{
				Name:        "cosmosdb_data_plane_request_duration_seconds",
				Description: "Duration quantiles of CosmosDB data plane requests.",
				Data: metricdata.Gauge[float64]{
					DataPoints: durationPoints,
				},
			})
		}

		if len(ruPoints) > 0 {
			metrics = append(metrics, metricdata.Metrics{
				Name:        "cosmosdb_data_plane_request_charge_ru_1m",
				Description: "RU consumed by CosmosDB data plane requests per minute.",
				Data: metricdata.Gauge[float64]{
					DataPoints: ruPoints,
				},
			})
		}
	}

	return metrics
}

// exportMinuteMetrics wraps metrics in ResourceMetrics and sends via OTLP.
func exportMinuteMetrics(ctx context.Context, exporter sdkmetric.Exporter, rm *metricdata.ResourceMetrics, cfg *Config, logger log.Logger) error {
	if err := exporter.Export(ctx, rm); err != nil {
		passwordHint := ""
		if len(cfg.OTLPPassword) > 3 {
			passwordHint = cfg.OTLPPassword[:3] + "..."
		} else if cfg.OTLPPassword != "" {
			passwordHint = "***"
		}
		level.Error(logger).Log("msg", "failed to export metrics via OTLP", "err", err, "endpoint", cfg.OTLPEndpoint, "username", cfg.OTLPUsername, "password_hint", passwordHint)
		return err
	}
	return nil
}

// countSeries returns the total number of data points (unique label combos) across all metrics.
func countSeries(metrics []metricdata.Metrics) int {
	var n int
	for _, m := range metrics {
		switch d := m.Data.(type) {
		case metricdata.Gauge[float64]:
			n += len(d.DataPoints)
		case metricdata.Sum[int64]:
			n += len(d.DataPoints)
		case metricdata.Sum[float64]:
			n += len(d.DataPoints)
		}
	}
	return n
}

// buildNullMetrics creates a copy of the given metrics with all gauge values
// set to NaN at the given timestamp. This causes the series to show as "no data"
// instead of a flat line extending from the last real point.
func buildNullMetrics(metrics []metricdata.Metrics, ts time.Time) []metricdata.Metrics {
	var result []metricdata.Metrics
	for _, m := range metrics {
		switch d := m.Data.(type) {
		case metricdata.Gauge[float64]:
			nullPoints := make([]metricdata.DataPoint[float64], len(d.DataPoints))
			for i, dp := range d.DataPoints {
				nullPoints[i] = metricdata.DataPoint[float64]{
					Attributes: dp.Attributes,
					StartTime:  ts,
					Time:       ts,
					Value:      math.NaN(),
				}
			}
			result = append(result, metricdata.Metrics{
				Name:        m.Name,
				Description: m.Description,
				Data: metricdata.Gauge[float64]{
					DataPoints: nullPoints,
				},
			})
		}
	}
	return result
}

// computePercentile returns the p-th percentile from a sorted slice.
func computePercentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return sorted[0]
	}
	idx := p * float64(len(sorted)-1)
	lower := int(idx)
	upper := lower + 1
	if upper >= len(sorted) {
		return sorted[len(sorted)-1]
	}
	frac := idx - float64(lower)
	return sorted[lower]*(1-frac) + sorted[upper]*frac
}
