package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

type DiagnosticRecord struct {
	Time          string         `json:"time"`
	Category      string         `json:"category"`
	OperationName string         `json:"operationName"`
	Properties    map[string]any `json:"properties"`
}

func processRecord(record *DiagnosticRecord, metrics *Metrics) {
	switch record.Category {
	case "DataPlaneRequests":
		processDataPlaneRequest(record, metrics)
	case "PartitionKeyRUConsumption":
		processPartitionKeyRUConsumption(record, metrics)
	case "PartitionKeyStatistics":
		processPartitionKeyStatistics(record, metrics)
	case "QueryRuntimeStatistics":
		processQueryRuntimeStatistics(record, metrics)
	}
}

func processDataPlaneRequest(record *DiagnosticRecord, metrics *Metrics) {
	p := record.Properties
	database := getStringProp(p, "databaseName")
	collection := getStringProp(p, "collectionName")
	operation := record.OperationName
	statusCode := getStringProp(p, "statusCode")

	durationMs := getFloat64Prop(p, "duration")
	requestCharge := getFloat64Prop(p, "requestCharge")

	if durationMs >= 0 {
		metrics.dataPlaneRequestDuration.WithLabelValues(database, collection, operation, statusCode).Observe(durationMs / 1000)
	}
	metrics.dataPlaneRequestsTotal.WithLabelValues(database, collection, operation, statusCode).Inc()

	if requestCharge > 0 {
		metrics.dataPlaneRequestChargeTotal.WithLabelValues(database, collection, operation).Add(requestCharge)
	}
}

func processPartitionKeyRUConsumption(record *DiagnosticRecord, metrics *Metrics) {
	p := record.Properties
	database := getStringProp(p, "databaseName")
	collection := getStringProp(p, "collectionName")
	partitionKeyRangeID := getStringProp(p, "partitionKeyRangeId")
	requestCharge := getFloat64Prop(p, "requestCharge")

	if requestCharge > 0 {
		metrics.partitionKeyRUConsumptionTotal.WithLabelValues(database, collection, partitionKeyRangeID).Add(requestCharge)
	}
}

func processPartitionKeyStatistics(record *DiagnosticRecord, metrics *Metrics) {
	p := record.Properties
	database := getStringProp(p, "databaseName")
	collection := getStringProp(p, "collectionName")
	partitionKey := parsePartitionKeyValue(getStringProp(p, "partitionKey"))
	sizeKb := getFloat64Prop(p, "sizeKb")

	if sizeKb >= 0 {
		metrics.partitionKeySizeBytes.WithLabelValues(database, collection, partitionKey).Set(sizeKb * 1024)
	}
}

func processQueryRuntimeStatistics(record *DiagnosticRecord, metrics *Metrics) {
	p := record.Properties
	// QueryRuntimeStatistics uses lowercase property names.
	database := getStringProp(p, "databasename")
	collection := getStringProp(p, "collectionname")

	metrics.queryRuntimeStatisticsTotal.WithLabelValues(database, collection).Inc()
}

// parsePartitionKeyValue extracts the value from a JSON array string like '["value"]'.
func parsePartitionKeyValue(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}

	var parts []any
	if err := json.Unmarshal([]byte(raw), &parts); err != nil || len(parts) == 0 {
		return raw
	}
	return fmt.Sprintf("%v", parts[0])
}

func getStringProp(props map[string]any, key string) string {
	v, ok := props[key]
	if !ok {
		return ""
	}

	switch val := v.(type) {
	case string:
		return val
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	default:
		return fmt.Sprintf("%v", val)
	}
}

func getFloat64Prop(props map[string]any, key string) float64 {
	v, ok := props[key]
	if !ok {
		return -1
	}

	switch val := v.(type) {
	case float64:
		return val
	case string:
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return -1
		}
		return f
	default:
		return -1
	}
}
