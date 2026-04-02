package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseBlobPathTime(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected time.Time
	}{
		{
			name:     "standard diagnostic log path",
			path:     "resourceId=SUBSCRIPTIONS/00000000/RESOURCEGROUPS/MY-RG/PROVIDERS/MICROSOFT.DOCUMENTDB/DATABASEACCOUNTS/MY-ACCOUNT/y=2026/m=04/d=02/h=16/m=38/PT1H.json",
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

func TestFilterBlobsByAge(t *testing.T) {
	now := time.Date(2026, 4, 2, 15, 35, 0, 0, time.UTC)
	prefix := "resourceId=.../y=2026/m=04/d=02/h=15/"

	blobs := []string{
		prefix + "m=20/PT1H.json", // 15 minutes ago
		prefix + "m=28/PT1H.json", // 7 minutes ago
		prefix + "m=30/PT1H.json", // 5 minutes ago
		prefix + "m=31/PT1H.json", // 4 minutes ago
	}

	// maxAge=10m → cutoff is 15:25, so m=20 is filtered out.
	result := filterBlobsByAge(blobs, 10*time.Minute, now)
	assert.Len(t, result, 3)
	assert.Contains(t, result[0], "m=28")
	assert.Contains(t, result[1], "m=30")
	assert.Contains(t, result[2], "m=31")
}

func TestGroupByMinute(t *testing.T) {
	prefix := "resourceId=.../y=2026/m=04/d=02/h=15/"
	containerBlobs := map[string][]string{
		"insights-logs-dataplanerequests": {
			prefix + "m=28/PT1M.json",
			prefix + "m=29/PT1M.json",
			prefix + "m=30/PT1M.json",
			prefix + "m=31/PT1M.json",
		},
		"insights-logs-partitionkeyruconsumption": {
			prefix + "m=28/PT1M.json",
			prefix + "m=29/PT1M.json",
			prefix + "m=30/PT1M.json",
			prefix + "m=31/PT1M.json",
		},
	}

	minutes := groupByMinute(containerBlobs)

	// Latest minute (31) excluded → 3 minutes remain.
	require.Len(t, minutes, 3)
	assert.Equal(t, time.Date(2026, 4, 2, 15, 28, 0, 0, time.UTC), minutes[0].Minute)
	assert.Equal(t, time.Date(2026, 4, 2, 15, 29, 0, 0, time.UTC), minutes[1].Minute)
	assert.Equal(t, time.Date(2026, 4, 2, 15, 30, 0, 0, time.UTC), minutes[2].Minute)

	// Each minute has blobs from both containers.
	assert.Len(t, minutes[0].Blobs, 2)
	assert.Len(t, minutes[0].Blobs["insights-logs-dataplanerequests"], 1)
	assert.Len(t, minutes[0].Blobs["insights-logs-partitionkeyruconsumption"], 1)
}

func TestGroupByMinute_SingleBlob(t *testing.T) {
	containerBlobs := map[string][]string{
		"insights-logs-dataplanerequests": {
			"resourceId=.../y=2026/m=04/d=02/h=15/m=31/PT1M.json",
		},
	}

	// Single blob = latest minute → excluded. Nothing left.
	minutes := groupByMinute(containerBlobs)
	assert.Empty(t, minutes)
}

func TestGroupByMinute_Empty(t *testing.T) {
	minutes := groupByMinute(nil)
	assert.Empty(t, minutes)
}

func TestGroupByMinute_DifferentContainerRanges(t *testing.T) {
	prefix := "resourceId=.../y=2026/m=04/d=02/h=15/"
	containerBlobs := map[string][]string{
		"insights-logs-dataplanerequests": {
			prefix + "m=28/PT1M.json",
			prefix + "m=29/PT1M.json",
			prefix + "m=30/PT1M.json",
		},
		"insights-logs-partitionkeyruconsumption": {
			prefix + "m=29/PT1M.json",
			prefix + "m=30/PT1M.json",
			prefix + "m=31/PT1M.json",
		},
	}

	// Global latest is m=31. Excluded.
	// m=28 only has DPR, m=29 and m=30 have both.
	minutes := groupByMinute(containerBlobs)
	require.Len(t, minutes, 3)

	// m=28: only DPR.
	assert.Len(t, minutes[0].Blobs, 1)
	assert.Contains(t, minutes[0].Blobs, "insights-logs-dataplanerequests")

	// m=29: both.
	assert.Len(t, minutes[1].Blobs, 2)

	// m=30: both.
	assert.Len(t, minutes[2].Blobs, 2)
}
