package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseBlobPathTime(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected time.Time
	}{
		{
			name:     "standard diagnostic log path",
			path:     "resourceId=SUBSCRIPTIONS/179C4F30/RESOURCEGROUPS/DEV-RG/PROVIDERS/MICROSOFT.DOCUMENTDB/DATABASEACCOUNTS/MIMIR-DEV-10/y=2026/m=04/d=02/h=16/m=38/PT1H.json",
			expected: time.Date(2026, 4, 2, 16, 38, 0, 0, time.UTC),
		},
		{
			name:     "minimal matching path",
			path:     "y=2025/m=01/d=15/h=09/m=05/PT1H.json",
			expected: time.Date(2025, 1, 15, 9, 5, 0, 0, time.UTC),
		},
		{
			name:     "no match returns zero time",
			path:     "some/random/path.json",
			expected: time.Time{},
		},
		{
			name:     "empty path",
			path:     "",
			expected: time.Time{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseBlobPathTime(tt.path)
			assert.True(t, tt.expected.Equal(result), "expected %v, got %v", tt.expected, result)
		})
	}
}
