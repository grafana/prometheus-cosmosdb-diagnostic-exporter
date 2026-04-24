package main

import (
	"strconv"
	"strings"
)

// partitionMapping maps document IDs to physical partition range IDs using data
// from PartitionKeyRUConsumption records. It supports two matching strategies:
//   - Exact match: doc_id == parsed partition key (e.g. chunks collection)
//   - Numeric suffix match: doc_id/100000 == numeric suffix of partition key (e.g. logs collection)
type partitionMapping struct {
	exact  map[string]string // parsed partition key → range ID
	suffix map[int64]string  // numeric suffix of partition key → range ID
}

func newPartitionMapping() *partitionMapping {
	return &partitionMapping{
		exact:  make(map[string]string),
		suffix: make(map[int64]string),
	}
}

// update adds entries from PartitionKeyRUConsumption records to the mapping.
func (m *partitionMapping) update(records []DiagnosticRecord) {
	for i := range records {
		r := &records[i]
		if r.Category != "PartitionKeyRUConsumption" {
			continue
		}
		pk := parsePartitionKeyValue(getStringProp(r.Properties, "partitionKey"))
		rangeID := getStringProp(r.Properties, "partitionKeyRangeId")
		if pk == "" || rangeID == "" {
			continue
		}
		m.exact[pk] = rangeID
		// Extract numeric suffix for logs-style mapping (e.g. "prefix_4784" → 4784).
		if idx := strings.LastIndex(pk, "_"); idx >= 0 {
			if n, err := strconv.ParseInt(pk[idx+1:], 10, 64); err == nil {
				m.suffix[n] = rangeID
			}
		}
	}
}

// resolve returns the partition key range ID for a given requestResourceId.
// Returns empty string if the mapping cannot be resolved.
func (m *partitionMapping) resolve(requestResourceID string) string {
	docID := extractDocID(requestResourceID)
	if docID == "" {
		return ""
	}
	// Exact match (chunks, dynamo_adapter — partition key IS the doc ID).
	if rangeID, ok := m.exact[docID]; ok {
		return rangeID
	}
	// Numeric doc ID: try logs-style mapping (doc_id / 100000 → suffix).
	if n, err := strconv.ParseInt(docID, 10, 64); err == nil {
		logicalPartition := n / 100000
		if rangeID, ok := m.suffix[logicalPartition]; ok {
			return rangeID
		}
	}
	return ""
}

// extractDocID extracts the document ID from a requestResourceId path.
// E.g. "/dbs/mydb/colls/my_collection/docs/my_doc_id" → "my_doc_id".
func extractDocID(requestResourceID string) string {
	const prefix = "/docs/"
	idx := strings.LastIndex(requestResourceID, prefix)
	if idx < 0 {
		return ""
	}
	return requestResourceID[idx+len(prefix):]
}
