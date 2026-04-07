package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

type DiagnosticRecord struct {
	Time          string         `json:"time"`
	ResourceID    string         `json:"resourceId"`
	Category      string         `json:"category"`
	OperationName string         `json:"operationName"`
	Properties    map[string]any `json:"properties"`
}

// extractAccountName extracts the database account name from a resourceId string
// like "/SUBSCRIPTIONS/.../DATABASEACCOUNTS/ACCOUNT-NAME".
func extractAccountName(resourceID string) string {
	upper := strings.ToUpper(resourceID)
	const marker = "/DATABASEACCOUNTS/"
	idx := strings.LastIndex(upper, marker)
	if idx < 0 {
		return ""
	}
	name := resourceID[idx+len(marker):]
	if i := strings.Index(name, "/"); i >= 0 {
		name = name[:i]
	}
	return strings.ToLower(name)
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
