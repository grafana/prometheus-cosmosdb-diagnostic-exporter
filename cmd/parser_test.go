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

const dataPlaneRequestsFixture = `{ "time": "2026-04-02T16:38:34Z", "resourceId": "/SUBSCRIPTIONS/00000000/RESOURCEGROUPS/MY-RG/PROVIDERS/MICROSOFT.DOCUMENTDB/DATABASEACCOUNTS/MY-ACCOUNT", "category": "DataPlaneRequests", "operationName": "Read", "properties": {"activityId": "act-200","statusCode": "200","duration": "2.222700","requestCharge": "2.000000","databaseName": "mydb","collectionName": "dynamo_adapter","requestResourceId": "/dbs/mydb/colls/dynamo_adapter/docs/my_record"}}
{ "time": "2026-04-02T16:38:38Z", "resourceId": "/SUBSCRIPTIONS/00000000/RESOURCEGROUPS/MY-RG/PROVIDERS/MICROSOFT.DOCUMENTDB/DATABASEACCOUNTS/MY-ACCOUNT", "category": "DataPlaneRequests", "operationName": "Read", "properties": {"activityId": "act-404","statusCode": "404","duration": "2.213000","requestCharge": "2.000000","databaseName": "mydb","collectionName": "table","requestResourceId": "/dbs/mydb/colls/table/docs/000000000000000000000478401387"}}
{ "time": "2026-04-02T16:38:51Z", "resourceId": "/SUBSCRIPTIONS/00000000/RESOURCEGROUPS/MY-RG/PROVIDERS/MICROSOFT.DOCUMENTDB/DATABASEACCOUNTS/MY-ACCOUNT", "category": "DataPlaneRequests", "operationName": "Create", "properties": {"activityId": "act-snap","statusCode": "200","duration": "2.508000","requestCharge": "5.950000","databaseName": "mydb","collectionName": "snapshots","requestResourceId": "/dbs/mydb/colls/snapshots/docs"}}
{ "time": "2026-04-02T16:38:56Z", "resourceId": "/SUBSCRIPTIONS/00000000/RESOURCEGROUPS/MY-RG/PROVIDERS/MICROSOFT.DOCUMENTDB/DATABASEACCOUNTS/MY-ACCOUNT", "category": "DataPlaneRequests", "operationName": "Create", "properties": {"activityId": "act-chunk","statusCode": "201","duration": "7.341900","requestCharge": "5.900000","databaseName": "mydb","collectionName": "table_chunks","requestResourceId": "/dbs/mydb/colls/table_chunks/docs"}}`

// PKRU records carry activityId at the top level; the second record shares the
// activityId "act-404" with the 404 DataPlaneRequest in dataPlaneRequestsFixture.
const partitionKeyRUConsumptionFixture = `{ "time": "2026-04-02T16:52:00Z", "activityId": "act-other", "category": "PartitionKeyRUConsumption", "properties": {"databaseName":"mydb","collectionName":"table","partitionKeyRangeId":"1","requestCharge":"1.000000","partitionKey":"[\"prefix_abc_4784\"]"}}
{ "time": "2026-04-02T16:52:00Z", "activityId": "act-404", "category": "PartitionKeyRUConsumption", "properties": {"databaseName":"mydb","collectionName":"table","partitionKeyRangeId":"1","requestCharge":"3.500000","partitionKey":"[\"prefix_abc_4784\"]"}}
{ "time": "2026-04-02T16:52:00Z", "activityId": "act-3", "category": "PartitionKeyRUConsumption", "properties": {"databaseName":"mydb","collectionName":"table","partitionKeyRangeId":"2","requestCharge":"2.000000","partitionKey":"[\"prefix_abc_4785\"]"}}`

var testTimestamp = time.Date(2026, 4, 2, 16, 38, 0, 0, time.UTC)

func TestBuildMinuteMetrics_DataPlaneRequests(t *testing.T) {
	records := parseRecords(t, dataPlaneRequestsFixture)
	metrics := buildMinuteMetrics(records, nil, "test-cluster", testTimestamp)

	// Request count: 4 records across 4 unique key combinations.
	countMetric := findOTLPMetric(metrics, "cosmosdb_data_plane_requests_1m")
	require.NotNil(t, countMetric)
	sum := countMetric.Data.(metricdata.Gauge[float64])

	var totalCount float64
	for _, dp := range sum.DataPoints {
		totalCount += dp.Value
		assert.Equal(t, testTimestamp, dp.Time)
	}
	assert.Equal(t, 4.0, totalCount)

	// Verify a specific data point.
	dp := findFloat64DataPoint(sum.DataPoints, map[string]string{
		"cluster": "test-cluster", "account_name": "my-account", "database": "mydb", "collection": "dynamo_adapter", "operation": "Read", "status_code": "200", "partition_key_range_id": "",
	})
	require.NotNil(t, dp)
	assert.Equal(t, 1.0, dp.Value)

	// Duration percentiles + avg exist.
	durMetric := findOTLPMetric(metrics, "cosmosdb_data_plane_request_duration_seconds")
	require.NotNil(t, durMetric)
	durGauge := durMetric.Data.(metricdata.Gauge[float64])
	// Each of the 4 keys produces 6 points: avg + 4 percentiles + max.
	assert.Equal(t, 4*6, len(durGauge.DataPoints))

	// Check avg for the single Read/200 record (duration 2.2227ms → 0.0022227s).
	avg := findFloat64DataPoint(durGauge.DataPoints, map[string]string{
		"cluster": "test-cluster", "account_name": "my-account", "database": "mydb", "collection": "dynamo_adapter", "operation": "Read", "status_code": "200",
		"partition_key_range_id": "", "quantile": "avg",
	})
	require.NotNil(t, avg)
	assert.InDelta(t, 0.0022227, avg.Value, 0.0001)

}

func TestBuildMinuteMetrics_WithPartitionLookup(t *testing.T) {
	// Build lookup from PKRU records, then verify DPR records get enriched
	// with partition_key_range_id via activityId join.
	pkruRecords := parseRecords(t, partitionKeyRUConsumptionFixture)
	lookup := newPartitionLookup()
	lookup.update(pkruRecords)

	dprRecords := parseRecords(t, dataPlaneRequestsFixture)
	metrics := buildMinuteMetrics(dprRecords, lookup, "test-cluster", testTimestamp)

	countMetric := findOTLPMetric(metrics, "cosmosdb_data_plane_requests_1m")
	require.NotNil(t, countMetric)
	sum := countMetric.Data.(metricdata.Gauge[float64])

	// The Read/404 DPR (activityId=act-404) joins to the PKRU record with range "1".
	dp := findFloat64DataPoint(sum.DataPoints, map[string]string{
		"cluster": "test-cluster", "account_name": "my-account", "database": "mydb", "collection": "table", "operation": "Read",
		"status_code": "404", "partition_key_range_id": "1",
	})
	require.NotNil(t, dp, "should resolve rangeID via activityId join")
	assert.Equal(t, 1.0, dp.Value)

	// DPRs with no matching PKRU record get empty range.
	dp2 := findFloat64DataPoint(sum.DataPoints, map[string]string{
		"cluster": "test-cluster", "account_name": "my-account", "database": "mydb", "collection": "table_chunks", "operation": "Create",
		"status_code": "201", "partition_key_range_id": "",
	})
	require.NotNil(t, dp2, "DPRs without a PKRU match should have empty partition_key_range_id")
}

func TestBuildMinuteMetrics_UnknownCategory(t *testing.T) {
	records := []DiagnosticRecord{{Category: "UnknownCategory", Properties: map[string]any{}}}
	metrics := buildMinuteMetrics(records, nil, "test-cluster", testTimestamp)
	assert.Empty(t, metrics)
}

func TestBuildMinuteMetrics_EmptyRecords(t *testing.T) {
	metrics := buildMinuteMetrics(nil, nil, "test-cluster", testTimestamp)
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
		{`["partition_abc_123"]`, "partition_abc_123"},
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

func TestExtractAccountName(t *testing.T) {
	assert.Equal(t, "my-account", extractAccountName("/SUBSCRIPTIONS/00000000/RESOURCEGROUPS/MY-RG/PROVIDERS/MICROSOFT.DOCUMENTDB/DATABASEACCOUNTS/MY-ACCOUNT"))
	assert.Equal(t, "my-account", extractAccountName("/subscriptions/00000000/resourcegroups/my-rg/providers/microsoft.documentdb/databaseaccounts/my-account"))
	assert.Equal(t, "", extractAccountName(""))
	assert.Equal(t, "", extractAccountName("/SUBSCRIPTIONS/00000000/RESOURCEGROUPS/MY-RG"))
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

func findFloat64DataPoint(points []metricdata.DataPoint[float64], attrs map[string]string) *metricdata.DataPoint[float64] {
	for i := range points {
		if matchAttributes(points[i].Attributes, attrs) {
			return &points[i]
		}
	}
	return nil
}
