package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckpointLoadSaveRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint.json")

	now := time.Now().Truncate(time.Second)
	cp := &Checkpoint{
		Containers: map[string]ContainerCheckpoint{
			"container-a": {
				LastBlobPath:      "resourceId=.../y=2026/m=04/d=02/h=16/m=00/PT1H.json",
				LastProcessedTime: now,
			},
			"container-b": {
				LastBlobPath:      "resourceId=.../y=2026/m=04/d=02/h=17/m=00/PT1H.json",
				LastProcessedTime: now.Add(time.Hour),
			},
		},
	}

	require.NoError(t, saveCheckpoint(path, cp))

	loaded, err := loadCheckpoint(path)
	require.NoError(t, err)

	assert.Equal(t, cp.Containers["container-a"].LastBlobPath, loaded.Containers["container-a"].LastBlobPath)
	assert.Equal(t, cp.Containers["container-b"].LastBlobPath, loaded.Containers["container-b"].LastBlobPath)
	assert.True(t, cp.Containers["container-a"].LastProcessedTime.Equal(loaded.Containers["container-a"].LastProcessedTime))
}

func TestCheckpointLoadMissingFile(t *testing.T) {
	cp, err := loadCheckpoint(filepath.Join(t.TempDir(), "nonexistent.json"))
	require.NoError(t, err)
	assert.NotNil(t, cp.Containers)
	assert.Empty(t, cp.Containers)
}

func TestCheckpointAtomicWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint.json")

	// Save first version.
	cp1 := &Checkpoint{
		Containers: map[string]ContainerCheckpoint{
			"c": {LastBlobPath: "blob-1"},
		},
	}
	require.NoError(t, saveCheckpoint(path, cp1))

	// Save second version.
	cp2 := &Checkpoint{
		Containers: map[string]ContainerCheckpoint{
			"c": {LastBlobPath: "blob-2"},
		},
	}
	require.NoError(t, saveCheckpoint(path, cp2))

	// Verify second version persisted and no temp file remains.
	loaded, err := loadCheckpoint(path)
	require.NoError(t, err)
	assert.Equal(t, "blob-2", loaded.Containers["c"].LastBlobPath)

	_, err = os.Stat(path + ".tmp")
	assert.True(t, os.IsNotExist(err))
}
