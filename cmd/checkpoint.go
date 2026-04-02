package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"
)

type ContainerCheckpoint struct {
	LastBlobPath      string    `json:"last_blob_path"`
	LastProcessedTime time.Time `json:"last_processed_time"`
}

type Checkpoint struct {
	Containers map[string]ContainerCheckpoint `json:"containers"`
}

func loadCheckpoint(path string) (*Checkpoint, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &Checkpoint{Containers: map[string]ContainerCheckpoint{}}, nil
		}
		return nil, err
	}

	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, err
	}
	if cp.Containers == nil {
		cp.Containers = map[string]ContainerCheckpoint{}
	}
	return &cp, nil
}

func saveCheckpoint(path string, cp *Checkpoint) error {
	data, err := json.Marshal(cp)
	if err != nil {
		return err
	}

	tmpPath := path + ".tmp"
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}
