package main

import (
	"bufio"
	"encoding/json"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const dataPlaneRequestsFixture = `{ "time": "2026-04-02T16:38:34Z", "category": "DataPlaneRequests", "operationName": "Read", "properties": {"statusCode": "200","duration": "2.222700","requestCharge": "2.000000","databaseName": "warpstream","collectionName": "dynamo_adapter"}}
{ "time": "2026-04-02T16:38:38Z", "category": "DataPlaneRequests", "operationName": "Read", "properties": {"statusCode": "404","duration": "2.213000","requestCharge": "2.000000","databaseName": "warpstream","collectionName": "rsm_logs_mimir_dev_10"}}
{ "time": "2026-04-02T16:38:51Z", "category": "DataPlaneRequests", "operationName": "Create", "properties": {"statusCode": "200","duration": "2.508000","requestCharge": "5.950000","databaseName": "warpstream","collectionName": "rsm_snapshots_mimir_dev_10"}}
{ "time": "2026-04-02T16:38:56Z", "category": "DataPlaneRequests", "operationName": "Create", "properties": {"statusCode": "201","duration": "7.341900","requestCharge": "5.900000","databaseName": "warpstream","collectionName": "rsm_logs_chunks_mimir_dev_10"}}`

const partitionKeyRUConsumptionFixture = `{ "time": "2026-04-02T16:52:00Z", "category": "PartitionKeyRUConsumption", "properties": {"databaseName":"warpstream","collectionName":"rsm_logs_mimir_dev_10","partitionKeyRangeId":"1","requestCharge":"1.000000"}}
{ "time": "2026-04-02T16:52:00Z", "category": "PartitionKeyRUConsumption", "properties": {"databaseName":"warpstream","collectionName":"rsm_logs_mimir_dev_10","partitionKeyRangeId":"1","requestCharge":"3.500000"}}
{ "time": "2026-04-02T16:52:00Z", "category": "PartitionKeyRUConsumption", "properties": {"databaseName":"warpstream","collectionName":"rsm_logs_mimir_dev_10","partitionKeyRangeId":"2","requestCharge":"2.000000"}}`

const partitionKeyStatisticsFixture = `{ "time": "2026-04-02T16:52:38Z", "category": "PartitionKeyStatistics", "properties": {"databaseName": "warpstream", "collectionName": "rsm_logs_mimir_dev_10", "partitionKey": "[\"rsmi_c9642ecd_4d06_4865_a9f5_3ffe6acd66c3_4711\"]", "sizeKb": 100707}}
{ "time": "2026-04-02T16:52:38Z", "category": "PartitionKeyStatistics", "properties": {"databaseName": "warpstream", "collectionName": "rsm_logs_mimir_dev_10", "partitionKey": "[\"rsmi_c9642ecd_4d06_4865_a9f5_3ffe6acd66c3_4765\"]", "sizeKb": 100707}}
{ "time": "2026-04-02T16:52:23Z", "category": "PartitionKeyStatistics", "properties": {"databaseName": "warpstream", "collectionName": "rsm_snapshots_mimir_dev_10", "partitionKey": "[\"rsmi_c9642ecd_4d06_4865_a9f5_3ffe6acd66c3\"]", "sizeKb": 5430}}`

const queryRuntimeStatisticsFixture = `{ "time": "2026-04-02T16:53:03Z", "category": "QueryRuntimeStatistics", "properties": {"databasename":"warpstream","collectionname":"rsm_snapshots_mimir_dev_10","queryexecutionstatus":"Finished","querytext":"{\"query\":\"SELECT TOP 1 * FROM ROOT r\"}"}}
{ "time": "2026-04-02T16:53:05Z", "category": "QueryRuntimeStatistics", "properties": {"databasename":"warpstream","collectionname":"rsm_snapshots_mimir_dev_10","queryexecutionstatus":"Finished","querytext":"{\"query\":\"SELECT TOP 1 * FROM ROOT r\"}"}}`

func TestProcessDataPlaneRequests(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := newMetrics(reg)

	records := parseRecords(t, dataPlaneRequestsFixture)
	for _, r := range records {
		processRecord(&r, metrics)
	}

	families := gatherMetrics(t, reg)

	// 4 records total: 2 Read (200 + 404), 2 Create (200 + 201).
	counter := findMetric(families, "cosmosdb_data_plane_requests_total")
	require.NotNil(t, counter)
	assert.Equal(t, 4.0, sumCounterValues(counter))

	// Check specific label combinations.
	assert.Equal(t, 1.0, getCounterValue(counter, map[string]string{
		"database": "warpstream", "collection": "dynamo_adapter", "operation": "Read", "status_code": "200",
	}))
	assert.Equal(t, 1.0, getCounterValue(counter, map[string]string{
		"database": "warpstream", "collection": "rsm_logs_mimir_dev_10", "operation": "Read", "status_code": "404",
	}))

	// Histogram has all 4 observations.
	hist := findMetric(families, "cosmosdb_data_plane_request_duration_seconds")
	require.NotNil(t, hist)
	assert.Equal(t, uint64(4), sumHistogramCounts(hist))

	// RU charge: 2.0 + 2.0 + 5.95 + 5.9 = 15.85.
	charge := findMetric(families, "cosmosdb_data_plane_request_charge_total")
	require.NotNil(t, charge)
	assert.InDelta(t, 15.85, sumCounterValues(charge), 0.01)
}

func TestProcessPartitionKeyRUConsumption(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := newMetrics(reg)

	records := parseRecords(t, partitionKeyRUConsumptionFixture)
	for _, r := range records {
		processRecord(&r, metrics)
	}

	families := gatherMetrics(t, reg)
	counter := findMetric(families, "cosmosdb_partition_key_ru_consumption_total")
	require.NotNil(t, counter)

	// Partition range 1: 1.0 + 3.5 = 4.5, range 2: 2.0.
	assert.InDelta(t, 4.5, getCounterValue(counter, map[string]string{
		"database": "warpstream", "collection": "rsm_logs_mimir_dev_10", "partition_key_range_id": "1",
	}), 0.01)
	assert.InDelta(t, 2.0, getCounterValue(counter, map[string]string{
		"database": "warpstream", "collection": "rsm_logs_mimir_dev_10", "partition_key_range_id": "2",
	}), 0.01)
}

func TestProcessPartitionKeyStatistics(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := newMetrics(reg)

	records := parseRecords(t, partitionKeyStatisticsFixture)
	for _, r := range records {
		processRecord(&r, metrics)
	}

	families := gatherMetrics(t, reg)
	gauge := findMetric(families, "cosmosdb_partition_key_size_bytes")
	require.NotNil(t, gauge)

	// 100707 KB * 1024 = bytes.
	assert.Equal(t, 100707.0*1024, getGaugeValue(gauge, map[string]string{
		"database": "warpstream", "collection": "rsm_logs_mimir_dev_10",
		"partition_key": "rsmi_c9642ecd_4d06_4865_a9f5_3ffe6acd66c3_4711",
	}))

	// 5430 KB for snapshots collection.
	assert.Equal(t, 5430.0*1024, getGaugeValue(gauge, map[string]string{
		"database": "warpstream", "collection": "rsm_snapshots_mimir_dev_10",
		"partition_key": "rsmi_c9642ecd_4d06_4865_a9f5_3ffe6acd66c3",
	}))
}

func TestProcessQueryRuntimeStatistics(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := newMetrics(reg)

	records := parseRecords(t, queryRuntimeStatisticsFixture)
	for _, r := range records {
		processRecord(&r, metrics)
	}

	families := gatherMetrics(t, reg)
	counter := findMetric(families, "cosmosdb_query_runtime_statistics_total")
	require.NotNil(t, counter)

	assert.Equal(t, 2.0, getCounterValue(counter, map[string]string{
		"database": "warpstream", "collection": "rsm_snapshots_mimir_dev_10",
	}))
}

func TestProcessRecordUnknownCategory(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := newMetrics(reg)

	record := &DiagnosticRecord{Category: "UnknownCategory", Properties: map[string]any{}}
	processRecord(record, metrics)

	// Should not panic and no metrics should be emitted for business metrics.
	families := gatherMetrics(t, reg)
	assert.Nil(t, findMetric(families, "cosmosdb_data_plane_requests_total"))
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

func gatherMetrics(t *testing.T, reg *prometheus.Registry) map[string]*dto.MetricFamily {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)

	result := make(map[string]*dto.MetricFamily, len(families))
	for _, f := range families {
		result[f.GetName()] = f
	}
	return result
}

func findMetric(families map[string]*dto.MetricFamily, name string) *dto.MetricFamily {
	return families[name]
}

func matchLabels(m *dto.Metric, labels map[string]string) bool {
	if len(m.GetLabel()) != len(labels) {
		return false
	}
	for _, lp := range m.GetLabel() {
		v, ok := labels[lp.GetName()]
		if !ok || v != lp.GetValue() {
			return false
		}
	}
	return true
}

func getCounterValue(family *dto.MetricFamily, labels map[string]string) float64 {
	for _, m := range family.GetMetric() {
		if matchLabels(m, labels) {
			return m.GetCounter().GetValue()
		}
	}
	return -1
}

func getGaugeValue(family *dto.MetricFamily, labels map[string]string) float64 {
	for _, m := range family.GetMetric() {
		if matchLabels(m, labels) {
			return m.GetGauge().GetValue()
		}
	}
	return -1
}

func sumCounterValues(family *dto.MetricFamily) float64 {
	var sum float64
	for _, m := range family.GetMetric() {
		sum += m.GetCounter().GetValue()
	}
	return sum
}

func sumHistogramCounts(family *dto.MetricFamily) uint64 {
	var sum uint64
	for _, m := range family.GetMetric() {
		sum += m.GetHistogram().GetSampleCount()
	}
	return sum
}
