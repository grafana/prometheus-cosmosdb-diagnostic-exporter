package main

import (
	"bufio"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const dataPlaneRequestsFixture = `{ "time": "2026-04-02T16:38:34Z", "category": "DataPlaneRequests", "operationName": "Read", "properties": {"statusCode": "200","duration": "2.222700","requestCharge": "2.000000","databaseName": "warpstream","collectionName": "dynamo_adapter","requestResourceId": "/dbs/warpstream/colls/dynamo_adapter/docs/rsm_cluster"}}
{ "time": "2026-04-02T16:38:38Z", "category": "DataPlaneRequests", "operationName": "Read", "properties": {"statusCode": "404","duration": "2.213000","requestCharge": "2.000000","databaseName": "warpstream","collectionName": "rsm_logs_mimir_dev_10","requestResourceId": "/dbs/warpstream/colls/rsm_logs_mimir_dev_10/docs/000000000000000000000478401387"}}
{ "time": "2026-04-02T16:38:51Z", "category": "DataPlaneRequests", "operationName": "Create", "properties": {"statusCode": "200","duration": "2.508000","requestCharge": "5.950000","databaseName": "warpstream","collectionName": "rsm_snapshots_mimir_dev_10","requestResourceId": "/dbs/warpstream/colls/rsm_snapshots_mimir_dev_10/docs"}}
{ "time": "2026-04-02T16:38:56Z", "category": "DataPlaneRequests", "operationName": "Create", "properties": {"statusCode": "201","duration": "7.341900","requestCharge": "5.900000","databaseName": "warpstream","collectionName": "rsm_logs_chunks_mimir_dev_10","requestResourceId": "/dbs/warpstream/colls/rsm_logs_chunks_mimir_dev_10/docs"}}`

const partitionKeyRUConsumptionFixture = `{ "time": "2026-04-02T16:52:00Z", "category": "PartitionKeyRUConsumption", "properties": {"databaseName":"warpstream","collectionName":"rsm_logs_mimir_dev_10","partitionKeyRangeId":"1","requestCharge":"1.000000","partitionKey":"[\"rsmi_abc_4784\"]"}}
{ "time": "2026-04-02T16:52:00Z", "category": "PartitionKeyRUConsumption", "properties": {"databaseName":"warpstream","collectionName":"rsm_logs_mimir_dev_10","partitionKeyRangeId":"1","requestCharge":"3.500000","partitionKey":"[\"rsmi_abc_4784\"]"}}
{ "time": "2026-04-02T16:52:00Z", "category": "PartitionKeyRUConsumption", "properties": {"databaseName":"warpstream","collectionName":"rsm_logs_mimir_dev_10","partitionKeyRangeId":"2","requestCharge":"2.000000","partitionKey":"[\"rsmi_abc_4785\"]"}}`

const partitionKeyStatisticsFixture = `{ "time": "2026-04-02T16:52:38Z", "category": "PartitionKeyStatistics", "properties": {"databaseName": "warpstream", "collectionName": "rsm_logs_mimir_dev_10", "partitionKey": "[\"rsmi_c9642ecd_4d06_4865_a9f5_3ffe6acd66c3_4711\"]", "sizeKb": 100707}}
{ "time": "2026-04-02T16:52:38Z", "category": "PartitionKeyStatistics", "properties": {"databaseName": "warpstream", "collectionName": "rsm_logs_mimir_dev_10", "partitionKey": "[\"rsmi_c9642ecd_4d06_4865_a9f5_3ffe6acd66c3_4765\"]", "sizeKb": 100707}}
{ "time": "2026-04-02T16:52:23Z", "category": "PartitionKeyStatistics", "properties": {"databaseName": "warpstream", "collectionName": "rsm_snapshots_mimir_dev_10", "partitionKey": "[\"rsmi_c9642ecd_4d06_4865_a9f5_3ffe6acd66c3\"]", "sizeKb": 5430}}`

const queryRuntimeStatisticsFixture = `{ "time": "2026-04-02T16:53:03Z", "category": "QueryRuntimeStatistics", "properties": {"databasename":"warpstream","collectionname":"rsm_snapshots_mimir_dev_10","queryexecutionstatus":"Finished","querytext":"{\"query\":\"SELECT TOP 1 * FROM ROOT r\"}"}}
{ "time": "2026-04-02T16:53:05Z", "category": "QueryRuntimeStatistics", "properties": {"databasename":"warpstream","collectionname":"rsm_snapshots_mimir_dev_10","queryexecutionstatus":"Finished","querytext":"{\"query\":\"SELECT TOP 1 * FROM ROOT r\"}"}}`

var testTimestamp = time.Date(2026, 4, 2, 16, 38, 0, 0, time.UTC)

func TestBuildMinuteMetrics_DataPlaneRequests(t *testing.T) {
	records := parseRecords(t, dataPlaneRequestsFixture)
	metrics := buildMinuteMetrics(records, nil, testTimestamp)

	// Request count: 4 records across 4 unique key combinations.
	countMetric := findOTLPMetric(metrics, "cosmosdb_data_plane_requests_1m")
	require.NotNil(t, countMetric)
	sum := countMetric.Data.(metricdata.Gauge[int64])

	var totalCount int64
	for _, dp := range sum.DataPoints {
		totalCount += dp.Value
		assert.Equal(t, testTimestamp, dp.Time)
	}
	assert.Equal(t, int64(4), totalCount)

	// Verify a specific data point.
	dp := findInt64DataPoint(sum.DataPoints, map[string]string{
		"database": "warpstream", "collection": "dynamo_adapter", "operation": "Read", "status_code": "200", "partition_key_range_id": "",
	})
	require.NotNil(t, dp)
	assert.Equal(t, int64(1), dp.Value)

	// Duration percentiles + avg exist.
	durMetric := findOTLPMetric(metrics, "cosmosdb_data_plane_request_duration_seconds")
	require.NotNil(t, durMetric)
	durGauge := durMetric.Data.(metricdata.Gauge[float64])
	// Each of the 4 keys produces 6 points: avg + 4 percentiles + max.
	assert.Equal(t, 4*6, len(durGauge.DataPoints))

	// Check avg for the single Read/200 record (duration 2.2227ms → 0.0022227s).
	avg := findFloat64DataPoint(durGauge.DataPoints, map[string]string{
		"database": "warpstream", "collection": "dynamo_adapter", "operation": "Read", "status_code": "200",
		"partition_key_range_id": "", "quantile": "avg",
	})
	require.NotNil(t, avg)
	assert.InDelta(t, 0.0022227, avg.Value, 0.0001)

	// RU charge: 2.0 + 2.0 + 5.95 + 5.9 = 15.85 (aggregated by db/collection/op).
	chargeMetric := findOTLPMetric(metrics, "cosmosdb_data_plane_request_charge_ru_1m")
	require.NotNil(t, chargeMetric)
	chargeSum := chargeMetric.Data.(metricdata.Gauge[float64])
	var totalCharge float64
	for _, dp := range chargeSum.DataPoints {
		totalCharge += dp.Value
	}
	assert.InDelta(t, 15.85, totalCharge, 0.01)
}

func TestBuildMinuteMetrics_WithPartitionMapping(t *testing.T) {
	// Build mapping from PKRU records, then verify DPR records get enriched.
	pkruRecords := parseRecords(t, partitionKeyRUConsumptionFixture)
	mapping := newPartitionMapping()
	mapping.update(pkruRecords)

	dprRecords := parseRecords(t, dataPlaneRequestsFixture)
	metrics := buildMinuteMetrics(dprRecords, mapping, testTimestamp)

	countMetric := findOTLPMetric(metrics, "cosmosdb_data_plane_requests_1m")
	require.NotNil(t, countMetric)
	sum := countMetric.Data.(metricdata.Gauge[int64])

	// The Read/404 record has requestResourceId ending with doc_id=000000000000000000000478401387.
	// 478401387 / 100000 = 4784 → matches partition key "rsmi_abc_4784" → range "1".
	dp := findInt64DataPoint(sum.DataPoints, map[string]string{
		"database": "warpstream", "collection": "rsm_logs_mimir_dev_10", "operation": "Read",
		"status_code": "404", "partition_key_range_id": "1",
	})
	require.NotNil(t, dp, "should resolve numeric doc_id to partition via suffix mapping")
	assert.Equal(t, int64(1), dp.Value)

	// Create operations have requestResourceId ending in /docs (no doc_id) → empty partition.
	dp2 := findInt64DataPoint(sum.DataPoints, map[string]string{
		"database": "warpstream", "collection": "rsm_logs_chunks_mimir_dev_10", "operation": "Create",
		"status_code": "201", "partition_key_range_id": "",
	})
	require.NotNil(t, dp2, "Create ops without doc_id should have empty partition_key_range_id")
}

func TestBuildMinuteMetrics_PartitionKeyRUConsumption(t *testing.T) {
	records := parseRecords(t, partitionKeyRUConsumptionFixture)
	metrics := buildMinuteMetrics(records, nil, testTimestamp)

	ruMetric := findOTLPMetric(metrics, "cosmosdb_partition_key_ru_consumption_ru_1m")
	require.NotNil(t, ruMetric)
	ruSum := ruMetric.Data.(metricdata.Gauge[float64])

	// Partition range 1: 1.0 + 3.5 = 4.5.
	dp1 := findFloat64DataPoint(ruSum.DataPoints, map[string]string{
		"database": "warpstream", "collection": "rsm_logs_mimir_dev_10", "partition_key_range_id": "1",
	})
	require.NotNil(t, dp1)
	assert.InDelta(t, 4.5, dp1.Value, 0.01)

	// Partition range 2: 2.0.
	dp2 := findFloat64DataPoint(ruSum.DataPoints, map[string]string{
		"database": "warpstream", "collection": "rsm_logs_mimir_dev_10", "partition_key_range_id": "2",
	})
	require.NotNil(t, dp2)
	assert.InDelta(t, 2.0, dp2.Value, 0.01)
}

func TestBuildMinuteMetrics_PartitionKeyStatistics(t *testing.T) {
	records := parseRecords(t, partitionKeyStatisticsFixture)
	metrics := buildMinuteMetrics(records, nil, testTimestamp)

	sizeMetric := findOTLPMetric(metrics, "cosmosdb_partition_key_size_bytes")
	require.NotNil(t, sizeMetric)
	sizeGauge := sizeMetric.Data.(metricdata.Gauge[float64])

	// 100707 KB * 1024 = bytes.
	dp := findFloat64DataPoint(sizeGauge.DataPoints, map[string]string{
		"database": "warpstream", "collection": "rsm_logs_mimir_dev_10",
		"partition_key": "rsmi_c9642ecd_4d06_4865_a9f5_3ffe6acd66c3_4711",
	})
	require.NotNil(t, dp)
	assert.Equal(t, 100707.0*1024, dp.Value)

	// 5430 KB for snapshots collection.
	dp2 := findFloat64DataPoint(sizeGauge.DataPoints, map[string]string{
		"database": "warpstream", "collection": "rsm_snapshots_mimir_dev_10",
		"partition_key": "rsmi_c9642ecd_4d06_4865_a9f5_3ffe6acd66c3",
	})
	require.NotNil(t, dp2)
	assert.Equal(t, 5430.0*1024, dp2.Value)
}

func TestBuildMinuteMetrics_QueryRuntimeStatistics(t *testing.T) {
	records := parseRecords(t, queryRuntimeStatisticsFixture)
	metrics := buildMinuteMetrics(records, nil, testTimestamp)

	qrsMetric := findOTLPMetric(metrics, "cosmosdb_query_runtime_statistics_1m")
	require.NotNil(t, qrsMetric)
	qrsSum := qrsMetric.Data.(metricdata.Gauge[int64])

	dp := findInt64DataPoint(qrsSum.DataPoints, map[string]string{
		"database": "warpstream", "collection": "rsm_snapshots_mimir_dev_10",
	})
	require.NotNil(t, dp)
	assert.Equal(t, int64(2), dp.Value)
}

func TestBuildMinuteMetrics_UnknownCategory(t *testing.T) {
	records := []DiagnosticRecord{{Category: "UnknownCategory", Properties: map[string]any{}}}
	metrics := buildMinuteMetrics(records, nil, testTimestamp)
	assert.Empty(t, metrics)
}

func TestBuildMinuteMetrics_EmptyRecords(t *testing.T) {
	metrics := buildMinuteMetrics(nil, nil, testTimestamp)
	assert.Empty(t, metrics)
}

func TestComputePercentile(t *testing.T) {
	sorted := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	assert.Equal(t, 5.5, computePercentile(sorted, 0.5))
	assert.InDelta(t, 9.55, computePercentile(sorted, 0.95), 0.01)
	assert.Equal(t, 10.0, computePercentile(sorted, 1.0))

	// Single element.
	assert.Equal(t, 42.0, computePercentile([]float64{42}, 0.99))

	// Empty.
	assert.Equal(t, 0.0, computePercentile(nil, 0.5))
}

func TestParsePartitionKeyValue(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{`["rsmi_abc_123"]`, "rsmi_abc_123"},
		{`["some_value"]`, "some_value"},
		{``, ""},
		{`not-json`, "not-json"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.expected, parsePartitionKeyValue(tt.input))
		})
	}
}

func TestGetStringProp(t *testing.T) {
	props := map[string]any{
		"str": "hello",
		"num": 42.0,
	}

	assert.Equal(t, "hello", getStringProp(props, "str"))
	assert.Equal(t, "42", getStringProp(props, "num"))
	assert.Equal(t, "", getStringProp(props, "nonexistent"))
}

func TestGetFloat64Prop(t *testing.T) {
	props := map[string]any{
		"num":    42.5,
		"strnum": "3.14",
		"bad":    "notanumber",
	}

	assert.Equal(t, 42.5, getFloat64Prop(props, "num"))
	assert.InDelta(t, 3.14, getFloat64Prop(props, "strnum"), 0.001)
	assert.Equal(t, -1.0, getFloat64Prop(props, "bad"))
	assert.Equal(t, -1.0, getFloat64Prop(props, "nonexistent"))
}

// -- helpers --

func parseRecords(t *testing.T, data string) []DiagnosticRecord {
	t.Helper()
	var records []DiagnosticRecord
	scanner := bufio.NewScanner(strings.NewReader(data))
	for scanner.Scan() {
		var r DiagnosticRecord
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &r))
		records = append(records, r)
	}
	require.NoError(t, scanner.Err())
	return records
}

func findOTLPMetric(metrics []metricdata.Metrics, name string) *metricdata.Metrics {
	for i := range metrics {
		if metrics[i].Name == name {
			return &metrics[i]
		}
	}
	return nil
}

func matchAttributes(set attribute.Set, expected map[string]string) bool {
	if set.Len() != len(expected) {
		return false
	}
	iter := set.Iter()
	for iter.Next() {
		kv := iter.Attribute()
		v, ok := expected[string(kv.Key)]
		if !ok || v != kv.Value.AsString() {
			return false
		}
	}
	return true
}

func findInt64DataPoint(points []metricdata.DataPoint[int64], attrs map[string]string) *metricdata.DataPoint[int64] {
	for i := range points {
		if matchAttributes(points[i].Attributes, attrs) {
			return &points[i]
		}
	}
	return nil
}

func findFloat64DataPoint(points []metricdata.DataPoint[float64], attrs map[string]string) *metricdata.DataPoint[float64] {
	for i := range points {
		if matchAttributes(points[i].Attributes, attrs) {
			return &points[i]
		}
	}
	return nil
}
