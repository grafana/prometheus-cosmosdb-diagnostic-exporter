package main

// partitionLookup joins DataPlaneRequests to PartitionKeyRUConsumption records
// by activityId so each request can be attributed to its partitionKeyRangeId.
type partitionLookup struct {
	rangeIDs map[string]string // activityId → partitionKeyRangeId
}

func newPartitionLookup() *partitionLookup {
	return &partitionLookup{rangeIDs: make(map[string]string)}
}

// update indexes PartitionKeyRUConsumption records by activityId. If multiple
// PKRU records share an activityId (e.g. cross-partition queries) and disagree
// on rangeID, the entry is cleared to avoid emitting ambiguous attribution.
func (l *partitionLookup) update(records []DiagnosticRecord) {
	for i := range records {
		r := &records[i]
		if r.Category != "PartitionKeyRUConsumption" {
			continue
		}
		actID := recordActivityID(r)
		if actID == "" {
			continue
		}
		rangeID := getStringProp(r.Properties, "partitionKeyRangeId")

		existing, ok := l.rangeIDs[actID]
		if !ok {
			l.rangeIDs[actID] = rangeID
			continue
		}
		if existing != rangeID {
			l.rangeIDs[actID] = ""
		}
	}
}

// lookup returns the partitionKeyRangeId associated with a request's
// activityId, or "" if no unambiguous entry exists.
func (l *partitionLookup) lookup(activityID string) string {
	if activityID == "" {
		return ""
	}
	return l.rangeIDs[activityID]
}
