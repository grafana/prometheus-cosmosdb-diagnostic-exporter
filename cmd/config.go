package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	PollInterval time.Duration `yaml:"poll_interval"`
	MaxAge       time.Duration `yaml:"max_age"`
	CheckpointDir string      `yaml:"checkpoint_dir"`

	OTLP OTLPConfig `yaml:"otlp"`

	Cells []CellConfig `yaml:"cells"`
}

type OTLPConfig struct {
	Endpoint string `yaml:"endpoint"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type CellConfig struct {
	Cluster              string `yaml:"cluster"`
	StorageAccountURL    string `yaml:"storage_account_url"`
	SubscriptionID       string `yaml:"subscription_id"`
	StorageResourceGroup string `yaml:"storage_resource_group"`
	BlobPathPrefix       string `yaml:"blob_path_prefix"`
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	cfg := &Config{
		PollInterval:  time.Minute,
		MaxAge:        90 * time.Minute,
		CheckpointDir: "./checkpoints",
	}

	// Expand environment variables in the config file.
	expanded := os.ExpandEnv(string(data))

	if err := yaml.Unmarshal([]byte(expanded), cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}

	if len(cfg.Cells) == 0 {
		return nil, fmt.Errorf("config must define at least one cell")
	}

	for i, cell := range cfg.Cells {
		if cell.Cluster == "" {
			return nil, fmt.Errorf("cell %d: cluster is required", i)
		}
		if cell.StorageAccountURL == "" {
			return nil, fmt.Errorf("cell %d (%s): storage_account_url is required", i, cell.Cluster)
		}
		if cell.SubscriptionID == "" {
			return nil, fmt.Errorf("cell %d (%s): subscription_id is required", i, cell.Cluster)
		}
		if cell.StorageResourceGroup == "" {
			return nil, fmt.Errorf("cell %d (%s): storage_resource_group is required", i, cell.Cluster)
		}
	}

	return cfg, nil
}
