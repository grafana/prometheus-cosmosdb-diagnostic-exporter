package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var latencyBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0}

type Metrics struct {
	// DataPlaneRequests
	dataPlaneRequestDuration    *prometheus.HistogramVec
	dataPlaneRequestsTotal      *prometheus.CounterVec
	dataPlaneRequestChargeTotal *prometheus.CounterVec

	// PartitionKeyRUConsumption
	partitionKeyRUConsumptionTotal *prometheus.CounterVec

	// PartitionKeyStatistics
	partitionKeySizeBytes *prometheus.GaugeVec

	// QueryRuntimeStatistics
	queryRuntimeStatisticsTotal *prometheus.CounterVec

	// Exporter self-metrics
	blobsProcessedTotal           prometheus.Counter
	recordsProcessedTotal         *prometheus.CounterVec
	lastProcessedTimestampSeconds prometheus.Gauge
	processingErrorsTotal         prometheus.Counter
}

func newMetrics(reg prometheus.Registerer) *Metrics {
	return &Metrics{
		dataPlaneRequestDuration: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cosmosdb_data_plane_request_duration_seconds",
			Help:    "Duration of CosmosDB data plane requests in seconds.",
			Buckets: latencyBuckets,
		}, []string{"database", "collection", "operation", "status_code"}),

		dataPlaneRequestsTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cosmosdb_data_plane_requests_total",
			Help: "Total number of CosmosDB data plane requests.",
		}, []string{"database", "collection", "operation", "status_code"}),

		dataPlaneRequestChargeTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cosmosdb_data_plane_request_charge_total",
			Help: "Total request units (RU) consumed by CosmosDB data plane requests.",
		}, []string{"database", "collection", "operation"}),

		partitionKeyRUConsumptionTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cosmosdb_partition_key_ru_consumption_total",
			Help: "Total request units (RU) consumed per partition key range.",
		}, []string{"database", "collection", "partition_key_range_id"}),

		partitionKeySizeBytes: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cosmosdb_partition_key_size_bytes",
			Help: "Size of a partition key in bytes.",
		}, []string{"database", "collection", "partition_key"}),

		queryRuntimeStatisticsTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cosmosdb_query_runtime_statistics_total",
			Help: "Total number of CosmosDB query runtime statistics records.",
		}, []string{"database", "collection"}),

		blobsProcessedTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cosmosdb_exporter_blobs_processed_total",
			Help: "Total number of diagnostic log blobs processed.",
		}),

		recordsProcessedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cosmosdb_exporter_records_processed_total",
			Help: "Total number of diagnostic log records processed.",
		}, []string{"category"}),

		lastProcessedTimestampSeconds: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cosmosdb_exporter_last_processed_timestamp_seconds",
			Help: "Unix timestamp of the last successfully processed blob.",
		}),

		processingErrorsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cosmosdb_exporter_processing_errors_total",
			Help: "Total number of errors encountered while processing diagnostic logs.",
		}),
	}
}
