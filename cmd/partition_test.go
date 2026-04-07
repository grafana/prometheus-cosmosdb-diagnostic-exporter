package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestExtractDocID(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"/dbs/warpstream/colls/rsm_logs_chunks/docs/rsmi_abc::uuid-123", "rsmi_abc::uuid-123"},
		{"/dbs/warpstream/colls/rsm_logs/docs/000000000000000000000478401387", "000000000000000000000478401387"},
		{"/dbs/warpstream/colls/rsm_snapshots/docs", ""},      // Create: path ends at /docs
		{"/dbs/warpstream/colls/rsm_snapshots/docs/", ""},      // Trailing slash
		{"some/random/path", ""},                                // No /docs/ segment
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.expected, extractDocID(tt.input))
		})
	}
}

func TestPartitionMapping_ExactMatch(t *testing.T) {
	mapping := newPartitionMapping()

	// Simulate chunks collection: partition key IS the doc_id.
	records := []DiagnosticRecord{
		{
			Category: "PartitionKeyRUConsumption",
			Properties: map[string]any{
				"partitionKey":        `["rsmi_abc::uuid-1"]`,
				"partitionKeyRangeId": "1",
			},
		},
		{
			Category: "PartitionKeyRUConsumption",
			Properties: map[string]any{
				"partitionKey":        `["rsmi_abc::uuid-2"]`,
				"partitionKeyRangeId": "2",
			},
		},
	}
	mapping.update(records)

	// Exact match.
	assert.Equal(t, "1", mapping.resolve("/dbs/warpstream/colls/chunks/docs/rsmi_abc::uuid-1"))
	assert.Equal(t, "2", mapping.resolve("/dbs/warpstream/colls/chunks/docs/rsmi_abc::uuid-2"))

	// No match.
	assert.Equal(t, "", mapping.resolve("/dbs/warpstream/colls/chunks/docs/rsmi_abc::uuid-unknown"))
	assert.Equal(t, "", mapping.resolve("/dbs/warpstream/colls/chunks/docs"))
}

func TestPartitionMapping_NumericSuffixMatch(t *testing.T) {
	mapping := newPartitionMapping()

	// Simulate logs collection: partition key has numeric suffix.
	records := []DiagnosticRecord{
		{
			Category: "PartitionKeyRUConsumption",
			Properties: map[string]any{
				"partitionKey":        `["rsmi_c9642ecd_4d06_4865_a9f5_3ffe6acd66c3_4784"]`,
				"partitionKeyRangeId": "1",
			},
		},
		{
			Category: "PartitionKeyRUConsumption",
			Properties: map[string]any{
				"partitionKey":        `["rsmi_c9642ecd_4d06_4865_a9f5_3ffe6acd66c3_4785"]`,
				"partitionKeyRangeId": "0",
			},
		},
	}
	mapping.update(records)

	// Numeric doc_id: 478401387 / 100000 = 4784 → matches suffix "_4784" → range "1".
	assert.Equal(t, "1", mapping.resolve("/dbs/warpstream/colls/logs/docs/000000000000000000000478401387"))

	// 478500000 / 100000 = 4785 → range "0".
	assert.Equal(t, "0", mapping.resolve("/dbs/warpstream/colls/logs/docs/000000000000000000000478500000"))

	// 999999999 / 100000 = 9999 → no match.
	assert.Equal(t, "", mapping.resolve("/dbs/warpstream/colls/logs/docs/000000000000000000000999999999"))
}

func TestPartitionMapping_IgnoresNonPKRURecords(t *testing.T) {
	mapping := newPartitionMapping()
	records := []DiagnosticRecord{
		{
			Category: "DataPlaneRequests",
			Properties: map[string]any{
				"partitionKey":        `["some_key"]`,
				"partitionKeyRangeId": "1",
			},
		},
	}
	mapping.update(records)
	assert.Equal(t, 0, len(mapping.exact))
}

func TestPartitionMapping_WithRealTestData(t *testing.T) {
	pkruData, err := os.ReadFile("testdata/partitionkeyruconsumption.json")
	if err != nil {
		t.Skip("testdata not available")
	}

	records := parseRecordsFromBytes(t, pkruData)
	mapping := newPartitionMapping()
	mapping.update(records)

	// Verify mapping was built (should have entries for multiple collections).
	assert.Greater(t, len(mapping.exact), 0, "should have exact mapping entries")
	assert.Greater(t, len(mapping.suffix), 0, "should have suffix mapping entries")
}

func TestBuildMinuteMetrics_WithRealTestData(t *testing.T) {
	dprData, err := os.ReadFile("testdata/dataplanerequests.json")
	if err != nil {
		t.Skip("testdata not available")
	}
	pkruData, err := os.ReadFile("testdata/partitionkeyruconsumption.json")
	if err != nil {
		t.Skip("testdata not available")
	}

	dprRecords := parseRecordsFromBytes(t, dprData)
	pkruRecords := parseRecordsFromBytes(t, pkruData)

	mapping := newPartitionMapping()
	mapping.update(pkruRecords)

	ts := testTimestamp
	allRecords := append(dprRecords, pkruRecords...)
	metrics := buildMinuteMetrics(allRecords, mapping, ts)

	// Should have DPR count, duration, charge, and PKRU metrics.
	countMetric := findOTLPMetric(metrics, "cosmosdb_data_plane_requests_1m")
	require.NotNil(t, countMetric)
	sum := countMetric.Data.(metricdata.Gauge[int64])
	assert.Greater(t, len(sum.DataPoints), 0)

	// Should have duration percentiles including avg.
	durMetric := findOTLPMetric(metrics, "cosmosdb_data_plane_request_duration_seconds")
	require.NotNil(t, durMetric)
	durGauge := durMetric.Data.(metricdata.Gauge[float64])
	assert.Greater(t, len(durGauge.DataPoints), 0)

	// Check that some DPR records got partition_key_range_id resolved.
	var resolved int
	for _, dp := range sum.DataPoints {
		iter := dp.Attributes.Iter()
		for iter.Next() {
			kv := iter.Attribute()
			if kv.Key == "partition_key_range_id" && kv.Value.AsString() != "" {
				resolved++
			}
		}
	}
	assert.Greater(t, resolved, 0, "some DPR data points should have partition_key_range_id resolved")

	// PKRU metric should exist.
	ruMetric := findOTLPMetric(metrics, "cosmosdb_partition_key_ru_consumption_ru_1m")
	require.NotNil(t, ruMetric)

	// Verify timestamps are correct.
	for _, dp := range sum.DataPoints {
		assert.Equal(t, ts, dp.Time)
		assert.Equal(t, ts, dp.StartTime)
	}
}

func parseRecordsFromBytes(t *testing.T, data []byte) []DiagnosticRecord {
	t.Helper()
	return parseRecords(t, string(data))
}
