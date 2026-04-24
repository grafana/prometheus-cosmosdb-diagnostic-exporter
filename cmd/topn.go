package main

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

const topN = 5

// collectionOpKey groups tracking by (collection, operation).
type collectionOpKey struct {
	Collection string
	Operation  string
}

type trackedRequest struct {
	ActivityID          string
	Collection          string
	Operation           string
	StatusCode          string
	DurationSec         float64
	RUCharge            float64
	PartitionKeyRangeID string
	RequestResourceID   string
}

type trackedQuery struct {
	ActivityID          string
	Collection          string
	QueryText           string
	DurationSec         float64
	RUCharge            float64
	PartitionKeyRangeID string
	RowsReturned        int
}

type collectionOpTopN struct {
	slowest   []trackedRequest // sorted by duration descending
	costliest []trackedRequest // sorted by RU descending
}

type topNTracker struct {
	byCollectionOp map[collectionOpKey]*collectionOpTopN
	queries        []trackedQuery // top N queries by RU across all collections
	lastEmit       time.Time
}

func newTopNTracker() *topNTracker {
	return &topNTracker{
		byCollectionOp: map[collectionOpKey]*collectionOpTopN{},
		lastEmit:       time.Now(),
	}
}

// observeRequest records a DataPlaneRequest, keeping only the top N by duration and RU per (collection, operation).
func (t *topNTracker) observeRequest(req trackedRequest) {
	key := collectionOpKey{Collection: req.Collection, Operation: req.Operation}
	c := t.byCollectionOp[key]
	if c == nil {
		c = &collectionOpTopN{}
		t.byCollectionOp[key] = c
	}

	// Insert into slowest if qualifies.
	if len(c.slowest) < topN || req.DurationSec > c.slowest[len(c.slowest)-1].DurationSec {
		c.slowest = append(c.slowest, req)
		slices.SortFunc(c.slowest, func(a, b trackedRequest) int {
			if a.DurationSec > b.DurationSec {
				return -1
			}
			if a.DurationSec < b.DurationSec {
				return 1
			}
			return 0
		})
		if len(c.slowest) > topN {
			c.slowest = c.slowest[:topN]
		}
	}

	// Insert into costliest if qualifies.
	if len(c.costliest) < topN || req.RUCharge > c.costliest[len(c.costliest)-1].RUCharge {
		c.costliest = append(c.costliest, req)
		slices.SortFunc(c.costliest, func(a, b trackedRequest) int {
			if a.RUCharge > b.RUCharge {
				return -1
			}
			if a.RUCharge < b.RUCharge {
				return 1
			}
			return 0
		})
		if len(c.costliest) > topN {
			c.costliest = c.costliest[:topN]
		}
	}
}

// observeQuery records a query, keeping only the top N by RU.
func (t *topNTracker) observeQuery(q trackedQuery) {
	if len(t.queries) < topN || q.RUCharge > t.queries[len(t.queries)-1].RUCharge {
		t.queries = append(t.queries, q)
		slices.SortFunc(t.queries, func(a, b trackedQuery) int {
			if a.RUCharge > b.RUCharge {
				return -1
			}
			if a.RUCharge < b.RUCharge {
				return 1
			}
			return 0
		})
		if len(t.queries) > topN {
			t.queries = t.queries[:topN]
		}
	}
}

// feedMinuteRecords processes a minute's worth of records, joining QueryRuntimeStatistics
// to DataPlaneRequests via activityId to get RU and duration for queries.
func (t *topNTracker) feedMinuteRecords(records []DiagnosticRecord, lookup *partitionLookup) {
	// Index DataPlaneRequests by activityId for query joining.
	type dpInfo struct {
		RUCharge    float64
		DurationSec float64
	}
	dpByActivity := map[string]dpInfo{}

	for i := range records {
		r := &records[i]
		if r.Category != "DataPlaneRequests" {
			continue
		}
		p := r.Properties
		dur := getFloat64Prop(p, "duration")
		ru := getFloat64Prop(p, "requestCharge")
		if dur < 0 {
			dur = 0
		}
		if ru < 0 {
			ru = 0
		}
		durSec := dur / 1000

		activityID := getStringProp(p, "activityId")
		rangeID := ""
		if lookup != nil {
			rangeID = lookup.lookup(activityID)
		}

		if activityID != "" {
			dpByActivity[activityID] = dpInfo{RUCharge: ru, DurationSec: durSec}
		}

		t.observeRequest(trackedRequest{
			ActivityID:          activityID,
			Collection:          getStringProp(p, "collectionName"),
			Operation:           r.OperationName,
			StatusCode:          getStringProp(p, "statusCode"),
			DurationSec:         durSec,
			RUCharge:            ru,
			PartitionKeyRangeID: rangeID,
			RequestResourceID:   getStringProp(p, "requestResourceId"),
		})
	}

	// Process QueryRuntimeStatistics, joining to DataPlaneRequests via activityId.
	for i := range records {
		r := &records[i]
		if r.Category != "QueryRuntimeStatistics" {
			continue
		}
		p := r.Properties
		activityID := getStringProp(p, "activityId")
		dp, ok := dpByActivity[activityID]
		if !ok {
			continue
		}

		queryText := extractQueryText(getStringProp(p, "querytext"))
		rowsReturned := int(getFloat64Prop(p, "numberofrowsreturned"))
		if rowsReturned < 0 {
			rowsReturned = 0
		}

		t.observeQuery(trackedQuery{
			ActivityID:          activityID,
			Collection:          getStringProp(p, "collectionname"),
			QueryText:           queryText,
			DurationSec:         dp.DurationSec,
			RUCharge:            dp.RUCharge,
			PartitionKeyRangeID: getStringProp(p, "partitionkeyrangeid"),
			RowsReturned:        rowsReturned,
		})
	}
}

// extractQueryText parses the query text from the JSON envelope used by CosmosDB.
func extractQueryText(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	var envelope map[string]any
	if err := json.Unmarshal([]byte(raw), &envelope); err == nil {
		if q, ok := envelope["query"].(string); ok {
			return strings.ReplaceAll(q, "\n", " ")
		}
	}
	return strings.ReplaceAll(raw, "\n", " ")
}

// emitIfDue logs top-N stats and resets if at least 1 hour has passed since last emit.
func (t *topNTracker) emitIfDue(logger log.Logger) {
	if time.Since(t.lastEmit) < time.Hour {
		return
	}
	t.emit(logger)
}

func (t *topNTracker) emit(logger log.Logger) {
	for key, c := range t.byCollectionOp {
		for rank, req := range c.slowest {
			level.Info(logger).Log(
				"msg", "top_slow_request",
				"collection", key.Collection,
				"operation", key.Operation,
				"rank", rank+1,
				"activity_id", req.ActivityID,
				"status_code", req.StatusCode,
				"duration_sec", fmt.Sprintf("%.4f", req.DurationSec),
				"ru_charge", fmt.Sprintf("%.2f", req.RUCharge),
				"partition_key_range_id", req.PartitionKeyRangeID,
				"document", shortenResourceID(req.RequestResourceID),
			)
		}
		for rank, req := range c.costliest {
			level.Info(logger).Log(
				"msg", "top_ru_request",
				"collection", key.Collection,
				"operation", key.Operation,
				"rank", rank+1,
				"activity_id", req.ActivityID,
				"status_code", req.StatusCode,
				"duration_sec", fmt.Sprintf("%.4f", req.DurationSec),
				"ru_charge", fmt.Sprintf("%.2f", req.RUCharge),
				"partition_key_range_id", req.PartitionKeyRangeID,
				"document", shortenResourceID(req.RequestResourceID),
			)
		}
	}

	for rank, q := range t.queries {
		level.Info(logger).Log(
			"msg", "top_ru_query",
			"rank", rank+1,
			"collection", q.Collection,
			"activity_id", q.ActivityID,
			"duration_sec", fmt.Sprintf("%.4f", q.DurationSec),
			"ru_charge", fmt.Sprintf("%.2f", q.RUCharge),
			"partition_key_range_id", q.PartitionKeyRangeID,
			"rows_returned", q.RowsReturned,
			"query", q.QueryText,
		)
	}

	// Reset.
	t.byCollectionOp = map[collectionOpKey]*collectionOpTopN{}
	t.queries = nil
	t.lastEmit = time.Now()
}

// shortenResourceID shortens a requestResourceId like "/dbs/mydb/colls/coll/docs/id"
// to "colls/coll/docs/id".
func shortenResourceID(resourceID string) string {
	parts := strings.Split(resourceID, "/")
	if len(parts) > 4 {
		return strings.Join(parts[len(parts)-4:], "/")
	}
	return resourceID
}
