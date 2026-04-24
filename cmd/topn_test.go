package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestObserveRequest_KeyedByCollectionAndOperation(t *testing.T) {
	tracker := newTopNTracker()

	tracker.observeRequest(trackedRequest{Collection: "chunks", Operation: "Read", DurationSec: 1.0, RUCharge: 10})
	tracker.observeRequest(trackedRequest{Collection: "chunks", Operation: "Create", DurationSec: 2.0, RUCharge: 20})
	tracker.observeRequest(trackedRequest{Collection: "logs", Operation: "Read", DurationSec: 3.0, RUCharge: 30})

	assert.Len(t, tracker.byCollectionOp, 3)

	chunksRead := tracker.byCollectionOp[collectionOpKey{Collection: "chunks", Operation: "Read"}]
	require.NotNil(t, chunksRead)
	assert.Len(t, chunksRead.slowest, 1)
	assert.Equal(t, 1.0, chunksRead.slowest[0].DurationSec)

	chunksCreate := tracker.byCollectionOp[collectionOpKey{Collection: "chunks", Operation: "Create"}]
	require.NotNil(t, chunksCreate)
	assert.Equal(t, 20.0, chunksCreate.costliest[0].RUCharge)
}

func TestObserveRequest_KeepsTopNOnly(t *testing.T) {
	tracker := newTopNTracker()

	// Insert more than topN requests for same (collection, operation).
	for i := 0; i < 10; i++ {
		tracker.observeRequest(trackedRequest{
			Collection:  "coll",
			Operation:   "Read",
			DurationSec: float64(i),
			RUCharge:    float64(i * 10),
		})
	}

	key := collectionOpKey{Collection: "coll", Operation: "Read"}
	c := tracker.byCollectionOp[key]

	assert.Len(t, c.slowest, topN)
	assert.Len(t, c.costliest, topN)

	// Slowest should be 9, 8, 7, 6, 5.
	for i, req := range c.slowest {
		assert.Equal(t, float64(9-i), req.DurationSec)
	}
	// Costliest should be 90, 80, 70, 60, 50.
	for i, req := range c.costliest {
		assert.Equal(t, float64((9-i)*10), req.RUCharge)
	}
}

func TestObserveRequest_DoesNotInsertBelowThreshold(t *testing.T) {
	tracker := newTopNTracker()

	// Fill with high values.
	for i := 0; i < topN; i++ {
		tracker.observeRequest(trackedRequest{
			Collection:  "coll",
			Operation:   "Read",
			DurationSec: float64(100 + i),
			RUCharge:    float64(100 + i),
		})
	}

	// This should not displace anything.
	tracker.observeRequest(trackedRequest{
		Collection:  "coll",
		Operation:   "Read",
		DurationSec: 1.0,
		RUCharge:    1.0,
	})

	key := collectionOpKey{Collection: "coll", Operation: "Read"}
	c := tracker.byCollectionOp[key]
	assert.Len(t, c.slowest, topN)
	assert.Equal(t, 100.0, c.slowest[len(c.slowest)-1].DurationSec)
}

func TestObserveQuery_KeepsTopNByRU(t *testing.T) {
	tracker := newTopNTracker()

	for i := 0; i < 10; i++ {
		tracker.observeQuery(trackedQuery{
			Collection: "logs",
			QueryText:  fmt.Sprintf("SELECT * FROM c WHERE c.id = %d", i),
			RUCharge:   float64(i * 100),
		})
	}

	assert.Len(t, tracker.queries, topN)
	// Top should be 900, 800, 700, 600, 500.
	for i, q := range tracker.queries {
		assert.Equal(t, float64((9-i)*100), q.RUCharge)
	}
}

func TestFeedMinuteRecords(t *testing.T) {
	records := parseRecords(t, dataPlaneRequestsFixture)
	lookup := newPartitionLookup()

	tracker := newTopNTracker()
	tracker.feedMinuteRecords(records, lookup)

	// 4 records across 4 unique (collection, operation) pairs.
	assert.Len(t, tracker.byCollectionOp, 4)

	// Verify a specific entry.
	key := collectionOpKey{Collection: "dynamo_adapter", Operation: "Read"}
	c := tracker.byCollectionOp[key]
	require.NotNil(t, c)
	assert.Len(t, c.slowest, 1)
	assert.InDelta(t, 0.0022227, c.slowest[0].DurationSec, 0.0001) // 2.2227ms -> s
	assert.Equal(t, "200", c.slowest[0].StatusCode)
	assert.Equal(t, "/dbs/mydb/colls/dynamo_adapter/docs/my_record", c.slowest[0].RequestResourceID)
}

func TestFeedMinuteRecords_JoinsQueries(t *testing.T) {
	// DataPlaneRequest with activityId + matching QueryRuntimeStatistics.
	fixture := `{"time":"2026-04-02T16:38:34Z","resourceId":"/SUBSCRIPTIONS/00000000/RESOURCEGROUPS/MY-RG/PROVIDERS/MICROSOFT.DOCUMENTDB/DATABASEACCOUNTS/MY-ACCOUNT","category":"DataPlaneRequests","operationName":"Query","properties":{"activityId":"act-001","statusCode":"200","duration":"50.000000","requestCharge":"150.000000","databaseName":"mydb","collectionName":"logs","requestResourceId":"/dbs/mydb/colls/logs"}}
{"time":"2026-04-02T16:38:34Z","category":"QueryRuntimeStatistics","operationName":"Query","properties":{"activityId":"act-001","collectionname":"logs","partitionkeyrangeid":"3","querytext":"{\"query\":\"SELECT * FROM c WHERE c.pk = @pk\"}","numberofrowsreturned":"42","queryexecutionstatus":"OK"}}`

	records := parseRecords(t, fixture)
	tracker := newTopNTracker()
	tracker.feedMinuteRecords(records, nil)

	require.Len(t, tracker.queries, 1)
	q := tracker.queries[0]
	assert.Equal(t, "act-001", q.ActivityID)
	assert.Equal(t, "logs", q.Collection)
	assert.Equal(t, "SELECT * FROM c WHERE c.pk = @pk", q.QueryText)
	assert.InDelta(t, 0.050, q.DurationSec, 0.001) // 50ms -> s
	assert.Equal(t, 150.0, q.RUCharge)
	assert.Equal(t, "3", q.PartitionKeyRangeID)
	assert.Equal(t, 42, q.RowsReturned)
}

func TestFeedMinuteRecords_QueryWithNoMatchingDP(t *testing.T) {
	// QueryRuntimeStatistics with no matching DataPlaneRequest should be skipped.
	fixture := `{"time":"2026-04-02T16:38:34Z","category":"QueryRuntimeStatistics","operationName":"Query","properties":{"activityId":"orphan-001","collectionname":"logs","querytext":"SELECT 1","numberofrowsreturned":"1"}}`

	records := parseRecords(t, fixture)
	tracker := newTopNTracker()
	tracker.feedMinuteRecords(records, nil)

	assert.Empty(t, tracker.queries)
}

func TestExtractQueryText(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"json envelope", `{"query":"SELECT * FROM c"}`, "SELECT * FROM c"},
		{"multiline query", `{"query":"SELECT *\nFROM c"}`, "SELECT * FROM c"},
		{"plain text", "SELECT 1", "SELECT 1"},
		{"plain multiline", "SELECT *\nFROM c", "SELECT * FROM c"},
		{"empty", "", ""},
		{"whitespace", "  ", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, extractQueryText(tt.input))
		})
	}
}

func TestShortenResourceID(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"/dbs/mydb/colls/chunks/docs/abc123", "colls/chunks/docs/abc123"},
		{"/dbs/mydb/colls/logs/docs/12345", "colls/logs/docs/12345"},
		{"short/path", "short/path"},
		{"", ""},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.expected, shortenResourceID(tt.input))
		})
	}
}

func TestEmitIfDue_NotDueBefore1Hour(t *testing.T) {
	tracker := newTopNTracker()
	tracker.observeRequest(trackedRequest{Collection: "c", Operation: "Read", DurationSec: 1.0})

	var buf bytes.Buffer
	logger := log.NewLogfmtLogger(&buf)

	tracker.emitIfDue(logger)
	assert.Empty(t, buf.String(), "should not emit before 1 hour")
	assert.Len(t, tracker.byCollectionOp, 1, "should not reset")
}

func TestEmitIfDue_EmitsAfter1Hour(t *testing.T) {
	tracker := newTopNTracker()
	tracker.lastEmit = time.Now().Add(-61 * time.Minute)
	tracker.observeRequest(trackedRequest{Collection: "c", Operation: "Read", DurationSec: 1.0, RUCharge: 5.0})

	var buf bytes.Buffer
	logger := log.NewLogfmtLogger(&buf)

	tracker.emitIfDue(logger)
	assert.Contains(t, buf.String(), "top_slow_request")
	assert.Empty(t, tracker.byCollectionOp, "should reset after emit")
}

func TestEmit_LogsAllCategories(t *testing.T) {
	tracker := newTopNTracker()
	tracker.observeRequest(trackedRequest{
		ActivityID:          "act-1",
		Collection:          "chunks",
		Operation:           "Read",
		StatusCode:          "200",
		DurationSec:         0.5,
		RUCharge:            12.3,
		PartitionKeyRangeID: "3",
		RequestResourceID:   "/dbs/mydb/colls/chunks/docs/abc",
	})
	tracker.observeQuery(trackedQuery{
		ActivityID:          "act-2",
		Collection:          "logs",
		QueryText:           "SELECT * FROM c",
		DurationSec:         0.02,
		RUCharge:            847.5,
		PartitionKeyRangeID: "7",
		RowsReturned:        100,
	})

	var buf bytes.Buffer
	logger := log.NewLogfmtLogger(&buf)
	tracker.emit(logger)

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	require.Len(t, lines, 3) // 1 slow + 1 ru_request + 1 ru_query

	// Verify slow request line.
	assert.Contains(t, lines[0], "msg=top_slow_request")
	assert.Contains(t, lines[0], "collection=chunks")
	assert.Contains(t, lines[0], "operation=Read")
	assert.Contains(t, lines[0], "activity_id=act-1")
	assert.Contains(t, lines[0], "status_code=200")
	assert.Contains(t, lines[0], "duration_sec=0.5000")
	assert.Contains(t, lines[0], "ru_charge=12.30")
	assert.Contains(t, lines[0], "partition_key_range_id=3")
	assert.Contains(t, lines[0], "document=colls/chunks/docs/abc")

	// Verify RU request line.
	assert.Contains(t, lines[1], "msg=top_ru_request")

	// Verify query line.
	assert.Contains(t, lines[2], "msg=top_ru_query")
	assert.Contains(t, lines[2], "collection=logs")
	assert.Contains(t, lines[2], "activity_id=act-2")
	assert.Contains(t, lines[2], "ru_charge=847.50")
	assert.Contains(t, lines[2], "partition_key_range_id=7")
	assert.Contains(t, lines[2], "rows_returned=100")
	assert.Contains(t, lines[2], `query="SELECT * FROM c"`)

	// Verify reset.
	assert.Empty(t, tracker.byCollectionOp)
	assert.Nil(t, tracker.queries)
}

func TestEmit_Empty(t *testing.T) {
	tracker := newTopNTracker()

	var buf bytes.Buffer
	logger := log.NewLogfmtLogger(&buf)
	tracker.emit(logger)

	assert.Empty(t, buf.String())
}
