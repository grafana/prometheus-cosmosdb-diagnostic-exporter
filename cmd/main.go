package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/storage/armstorage"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

// Azure diagnostic log container names (fixed by Azure).
var containerNames = []string{
	"insights-logs-dataplanerequests",
	"insights-logs-partitionkeyruconsumption",
}

func main() {
	ctx := context.Background()

	// Init logger.
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stdout))
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)

	if len(os.Args) < 2 {
		level.Error(logger).Log("msg", "usage: exporter <config-file>")
		os.Exit(1)
	}

	cfg, err := loadConfig(os.Args[1])
	if err != nil {
		level.Error(logger).Log("msg", "failed to load config", "err", err)
		os.Exit(1)
	}

	// Configure OTLP exporter (shared across all cells).
	var opts []otlpmetrichttp.Option
	if cfg.OTLP.Endpoint != "" {
		opts = append(opts, otlpmetrichttp.WithEndpointURL(cfg.OTLP.Endpoint))
	}
	if cfg.OTLP.Username != "" || cfg.OTLP.Password != "" {
		encoded := base64.StdEncoding.EncodeToString([]byte(cfg.OTLP.Username + ":" + cfg.OTLP.Password))
		opts = append(opts, otlpmetrichttp.WithHeaders(map[string]string{
			"Authorization": "Basic " + encoded,
		}))
	}
	exporter, err := otlpmetrichttp.New(ctx, opts...)
	if err != nil {
		level.Error(logger).Log("msg", "failed to create OTLP exporter", "err", err)
		os.Exit(1)
	}
	defer exporter.Shutdown(ctx)

	res := resource.NewSchemaless(
		attribute.String("service.name", "cosmosdb-diagnostic-exporter"),
	)

	level.Info(logger).Log("msg", "exporter started", "cells", len(cfg.Cells), "poll_interval", cfg.PollInterval, "max_age", cfg.MaxAge)

	// Launch one goroutine per cell. If any fails, crash the process.
	errc := make(chan error, len(cfg.Cells))
	for _, cell := range cfg.Cells {
		go func() {
			errc <- runCell(ctx, cfg, cell, exporter, res, logger)
		}()
	}

	// Wait for the first error (any goroutine failing crashes the process).
	err = <-errc
	level.Error(logger).Log("msg", "cell failed, shutting down", "err", err)
	os.Exit(1)
}

func runCell(ctx context.Context, cfg *Config, cell CellConfig, exporter sdkmetric.Exporter, res *resource.Resource, logger log.Logger) error {
	cellLogger := log.With(logger, "cell", cell.Cluster)

	client, err := newBlobClient(ctx, cell, cellLogger)
	if err != nil {
		return fmt.Errorf("creating blob client for %s: %w", cell.Cluster, err)
	}

	checkpointFile := filepath.Join(cfg.CheckpointDir, cell.Cluster+".json")
	mapping := newPartitionMapping()

	level.Info(cellLogger).Log("msg", "cell started", "storage_account", cell.StorageAccountURL, "checkpoint", checkpointFile)

	// Run an initial poll at startup.
	poll(ctx, client, cell, cfg, checkpointFile, exporter, res, mapping, cellLogger)

	// Periodically poll for new blobs.
	ticker := time.NewTicker(cfg.PollInterval)
	for range ticker.C {
		poll(ctx, client, cell, cfg, checkpointFile, exporter, res, mapping, cellLogger)
	}

	return fmt.Errorf("unexpected ticker stop for cell %s", cell.Cluster)
}

func poll(ctx context.Context, client *azblob.Client, cell CellConfig, cfg *Config, checkpointFile string, exporter sdkmetric.Exporter, res *resource.Resource, mapping *partitionMapping, logger log.Logger) {
	checkpoint, err := loadCheckpoint(checkpointFile)
	if err != nil {
		level.Error(logger).Log("msg", "failed to load checkpoint", "err", err)
		return
	}

	if err := processMinutes(ctx, client, cell.BlobPathPrefix, cfg.MaxAge, checkpoint, checkpointFile, exporter, res, mapping, cell.Cluster, logger); err != nil {
		level.Error(logger).Log("msg", "failed to process minutes", "err", err)
	}
}

// newBlobClient fetches the storage account key via the ARM API (like az CLI does)
// and returns a blob client using Shared Key authentication.
func newBlobClient(ctx context.Context, cell CellConfig, logger log.Logger) (*azblob.Client, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("creating Azure credential: %w", err)
	}

	// Extract account name from URL (e.g. "myaccount" from "https://myaccount.blob.core.windows.net").
	parsed, err := url.Parse(cell.StorageAccountURL)
	if err != nil {
		return nil, fmt.Errorf("parsing storage account URL: %w", err)
	}
	accountName := strings.Split(parsed.Hostname(), ".")[0]

	// Fetch storage account keys via ARM.
	accountsClient, err := armstorage.NewAccountsClient(cell.SubscriptionID, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("creating ARM storage client: %w", err)
	}

	keys, err := accountsClient.ListKeys(ctx, cell.StorageResourceGroup, accountName, nil)
	if err != nil {
		return nil, fmt.Errorf("listing storage account keys: %w", err)
	}
	if len(keys.Keys) == 0 || keys.Keys[0].Value == nil {
		return nil, fmt.Errorf("no storage account keys found")
	}

	level.Info(logger).Log("msg", "fetched storage account key via ARM", "account", accountName)

	sharedKey, err := azblob.NewSharedKeyCredential(accountName, *keys.Keys[0].Value)
	if err != nil {
		return nil, fmt.Errorf("creating shared key credential: %w", err)
	}

	return azblob.NewClientWithSharedKeyCredential(cell.StorageAccountURL, sharedKey, nil)
}
