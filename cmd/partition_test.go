package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestPartitionLookup_ResolvesRangeID(t *testing.T) {
	lookup := newPartitionLookup()
	lookup.update([]DiagnosticRecord{
		{
			Category:   "PartitionKeyRUConsumption",
			ActivityID: "act-1",
			Properties: map[string]any{"partitionKeyRangeId": "3"},
		},
		{
			Category:   "PartitionKeyRUConsumption",
			ActivityID: "act-2",
			Properties: map[string]any{"partitionKeyRangeId": "1"},
		},
	})

	assert.Equal(t, "3", lookup.lookup("act-1"))
	assert.Equal(t, "1", lookup.lookup("act-2"))
	assert.Equal(t, "", lookup.lookup("act-unknown"))
	assert.Equal(t, "", lookup.lookup(""))
}

func TestPartitionLookup_AmbiguousRangeClears(t *testing.T) {
	lookup := newPartitionLookup()

	// Same activityId with conflicting range IDs (e.g. cross-partition query).
	lookup.update([]DiagnosticRecord{
		{
			Category:   "PartitionKeyRUConsumption",
			ActivityID: "act-x",
			Properties: map[string]any{"partitionKeyRangeId": "0"},
		},
		{
			Category:   "PartitionKeyRUConsumption",
			ActivityID: "act-x",
			Properties: map[string]any{"partitionKeyRangeId": "2"},
		},
	})

	assert.Equal(t, "", lookup.lookup("act-x"), "conflicting range ids should clear")
}

func TestPartitionLookup_ConsistentDuplicatesPreserveValue(t *testing.T) {
	lookup := newPartitionLookup()
	lookup.update([]DiagnosticRecord{
		{
			Category:   "PartitionKeyRUConsumption",
			ActivityID: "act-1",
			Properties: map[string]any{"partitionKeyRangeId": "1"},
		},
		{
			Category:   "PartitionKeyRUConsumption",
			ActivityID: "act-1",
			Properties: map[string]any{"partitionKeyRangeId": "1"},
		},
	})

	assert.Equal(t, "1", lookup.lookup("act-1"))
}

func TestPartitionLookup_IgnoresNonPKRURecords(t *testing.T) {
	lookup := newPartitionLookup()
	lookup.update([]DiagnosticRecord{
		{
			Category:   "DataPlaneRequests",
			ActivityID: "act-1",
			Properties: map[string]any{"partitionKeyRangeId": "0"},
		},
	})

	assert.Equal(t, "", lookup.lookup("act-1"))
}

func TestPartitionLookup_WithRealTestData(t *testing.T) {
	pkruData, err := os.ReadFile("testdata/partitionkeyruconsumption.json")
	if err != nil {
		t.Skip("testdata not available")
	}

	records := parseRecordsFromBytes(t, pkruData)
	lookup := newPartitionLookup()
	lookup.update(records)

	assert.Greater(t, len(lookup.rangeIDs), 0, "should have lookup entries")
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

	lookup := newPartitionLookup()
	lookup.update(pkruRecords)

	ts := testTimestamp
	allRecords := append(dprRecords, pkruRecords...)
	metrics := buildMinuteMetrics(allRecords, lookup, "test-cluster", ts)

	// Should have DPR count and duration metrics.
	countMetric := findOTLPMetric(metrics, "cosmosdb_data_plane_requests_1m")
	require.NotNil(t, countMetric)
	sum := countMetric.Data.(metricdata.Gauge[float64])
	assert.Greater(t, len(sum.DataPoints), 0)

	// Should have duration percentiles including avg.
	durMetric := findOTLPMetric(metrics, "cosmosdb_data_plane_request_duration_seconds")
	require.NotNil(t, durMetric)
	durGauge := durMetric.Data.(metricdata.Gauge[float64])
	assert.Greater(t, len(durGauge.DataPoints), 0)

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
