package main

import (
	"context"
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
	Database, Collection, Operation, StatusCode, PartitionKeyRangeID string
}

type dataPlaneAgg struct {
	Count       int64
	Durations   []float64
	TotalCharge float64
}

type chargeKey struct {
	Database, Collection, Operation string
}

type partitionKeyRUKey struct {
	Database, Collection, PartitionKeyRangeID string
}

type partitionKeySizeKey struct {
	Database, Collection, PartitionKey string
}

type queryRuntimeKey struct {
	Database, Collection string
}

// buildMinuteMetrics aggregates diagnostic records from a single minute into OTLP metrics.
// The partition mapping is used to resolve partition_key_range_id for DataPlaneRequests.
// The timestamp ts should be at :00 seconds of the minute.
func buildMinuteMetrics(records []DiagnosticRecord, mapping *partitionMapping, ts time.Time) []metricdata.Metrics {
	dpAgg := map[dataPlaneKey]*dataPlaneAgg{}
	charges := map[chargeKey]float64{}
	ruAgg := map[partitionKeyRUKey]float64{}
	sizeAgg := map[partitionKeySizeKey]float64{}
	qrsAgg := map[queryRuntimeKey]int64{}

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
			if charge := getFloat64Prop(p, "requestCharge"); charge > 0 {
				ck := chargeKey{key.Database, key.Collection, key.Operation}
				charges[ck] += charge
			}

		case "PartitionKeyRUConsumption":
			p := r.Properties
			key := partitionKeyRUKey{
				Database:            getStringProp(p, "databaseName"),
				Collection:          getStringProp(p, "collectionName"),
				PartitionKeyRangeID: getStringProp(p, "partitionKeyRangeId"),
			}
			if charge := getFloat64Prop(p, "requestCharge"); charge > 0 {
				ruAgg[key] += charge
			}

		case "PartitionKeyStatistics":
			p := r.Properties
			key := partitionKeySizeKey{
				Database:     getStringProp(p, "databaseName"),
				Collection:   getStringProp(p, "collectionName"),
				PartitionKey: parsePartitionKeyValue(getStringProp(p, "partitionKey")),
			}
			if sizeKb := getFloat64Prop(p, "sizeKb"); sizeKb >= 0 {
				sizeAgg[key] = sizeKb * 1024
			}

		case "QueryRuntimeStatistics":
			p := r.Properties
			key := queryRuntimeKey{
				Database:   getStringProp(p, "databasename"),
				Collection: getStringProp(p, "collectionname"),
			}
			qrsAgg[key]++
		}
	}

	var metrics []metricdata.Metrics

	// DataPlaneRequests: request count + duration percentiles.
	if len(dpAgg) > 0 {
		var countPoints []metricdata.DataPoint[int64]
		var durationPoints []metricdata.DataPoint[float64]

		for key, agg := range dpAgg {
			baseAttrs := []attribute.KeyValue{
				attribute.String("database", key.Database),
				attribute.String("collection", key.Collection),
				attribute.String("operation", key.Operation),
				attribute.String("status_code", key.StatusCode),
				attribute.String("partition_key_range_id", key.PartitionKeyRangeID),
			}
			countPoints = append(countPoints, metricdata.DataPoint[int64]{
				Attributes: attribute.NewSet(baseAttrs...),
				StartTime:  ts,
				Time:       ts,
				Value:      agg.Count,
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
			Data: metricdata.Gauge[int64]{
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
	}

	// DataPlaneRequests: RU charge (aggregated across status codes).
	if len(charges) > 0 {
		var chargePoints []metricdata.DataPoint[float64]
		for key, total := range charges {
			chargePoints = append(chargePoints, metricdata.DataPoint[float64]{
				Attributes: attribute.NewSet(
					attribute.String("database", key.Database),
					attribute.String("collection", key.Collection),
					attribute.String("operation", key.Operation),
				),
				StartTime: ts,
				Time:      ts,
				Value:     total,
			})
		}
		metrics = append(metrics, metricdata.Metrics{
			Name:        "cosmosdb_data_plane_request_charge_ru_1m",
			Description: "Request units (RU) consumed by CosmosDB data plane requests per minute.",
			Data: metricdata.Gauge[float64]{
				DataPoints: chargePoints,
			},
		})
	}

	// PartitionKeyRUConsumption.
	if len(ruAgg) > 0 {
		var ruPoints []metricdata.DataPoint[float64]
		for key, total := range ruAgg {
			ruPoints = append(ruPoints, metricdata.DataPoint[float64]{
				Attributes: attribute.NewSet(
					attribute.String("database", key.Database),
					attribute.String("collection", key.Collection),
					attribute.String("partition_key_range_id", key.PartitionKeyRangeID),
				),
				StartTime: ts,
				Time:      ts,
				Value:     total,
			})
		}
		metrics = append(metrics, metricdata.Metrics{
			Name:        "cosmosdb_partition_key_ru_consumption_ru_1m",
			Description: "Request units (RU) consumed per partition key range per minute.",
			Data: metricdata.Gauge[float64]{
				DataPoints: ruPoints,
			},
		})
	}

	// PartitionKeyStatistics.
	if len(sizeAgg) > 0 {
		var sizePoints []metricdata.DataPoint[float64]
		for key, size := range sizeAgg {
			sizePoints = append(sizePoints, metricdata.DataPoint[float64]{
				Attributes: attribute.NewSet(
					attribute.String("database", key.Database),
					attribute.String("collection", key.Collection),
					attribute.String("partition_key", key.PartitionKey),
				),
				StartTime: ts,
				Time:      ts,
				Value:     size,
			})
		}
		metrics = append(metrics, metricdata.Metrics{
			Name:        "cosmosdb_partition_key_size_bytes",
			Description: "Size of a partition key in bytes.",
			Data: metricdata.Gauge[float64]{
				DataPoints: sizePoints,
			},
		})
	}

	// QueryRuntimeStatistics.
	if len(qrsAgg) > 0 {
		var qrsPoints []metricdata.DataPoint[int64]
		for key, count := range qrsAgg {
			qrsPoints = append(qrsPoints, metricdata.DataPoint[int64]{
				Attributes: attribute.NewSet(
					attribute.String("database", key.Database),
					attribute.String("collection", key.Collection),
				),
				StartTime: ts,
				Time:      ts,
				Value:     count,
			})
		}
		metrics = append(metrics, metricdata.Metrics{
			Name:        "cosmosdb_query_runtime_statistics_1m",
			Description: "Number of CosmosDB query runtime statistics records per minute.",
			Data: metricdata.Gauge[int64]{
				DataPoints: qrsPoints,
			},
		})
	}

	return metrics
}

// exportMinuteMetrics wraps metrics in ResourceMetrics and sends via OTLP.
func exportMinuteMetrics(ctx context.Context, exporter sdkmetric.Exporter, rm *metricdata.ResourceMetrics, logger log.Logger) error {
	if err := exporter.Export(ctx, rm); err != nil {
		level.Error(logger).Log("msg", "failed to export metrics via OTLP", "err", err)
		return err
	}
	return nil
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
