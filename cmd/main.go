package main

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"net/url"
	"os"
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
	"insights-logs-queryruntimestatistics",
}

type Config struct {
	StorageAccountURL    string
	StorageAccountKey    string
	SubscriptionID       string
	StorageResourceGroup string
	BlobPathPrefix       string
	Cluster              string
	PollInterval         time.Duration
	CheckpointFile       string
	MaxAge               time.Duration
	OTLPEndpoint         string
	OTLPUsername         string
	OTLPPassword         string
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func (c *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&c.StorageAccountURL, "storage-account-url", "", "Azure Blob Storage account URL (e.g. https://myaccount.blob.core.windows.net).")
	f.StringVar(&c.StorageAccountKey, "storage-account-key", envOrDefault("AZURE_STORAGE_KEY", ""), "Azure storage account key. If set, skips ARM key lookup (env: AZURE_STORAGE_KEY).")
	f.StringVar(&c.SubscriptionID, "subscription-id", "", "Azure subscription ID containing the storage account.")
	f.StringVar(&c.StorageResourceGroup, "storage-resource-group", "", "Resource group of the storage account.")
	f.StringVar(&c.BlobPathPrefix, "blob-path-prefix", "", "Optional prefix to scope blob listing (e.g. resourceId=SUBSCRIPTIONS/...).")
	f.StringVar(&c.Cluster, "cluster", "", "Cluster label added to all exported metrics.")
	f.DurationVar(&c.PollInterval, "poll-interval", time.Minute, "How frequently to poll for new blobs.")
	f.StringVar(&c.CheckpointFile, "checkpoint-file", "./checkpoint.json", "Path to the checkpoint file for tracking processed blobs.")
	f.DurationVar(&c.MaxAge, "max-age", 90*time.Minute, "Maximum age of blobs to process. Blobs older than this are skipped on startup or when the checkpoint is stale.")
	f.StringVar(&c.OTLPEndpoint, "otlp-endpoint", "", "OTLP HTTP endpoint URL (e.g. http://localhost:4318). If empty, uses OTEL_EXPORTER_OTLP_ENDPOINT env var.")
	f.StringVar(&c.OTLPUsername, "otlp-username", envOrDefault("OTLP_USERNAME", ""), "Username for OTLP basic auth (env: OTLP_USERNAME).")
	f.StringVar(&c.OTLPPassword, "otlp-password", envOrDefault("OTLP_PASSWORD", ""), "Password for OTLP basic auth (env: OTLP_PASSWORD).")
}

func main() {
	ctx := context.Background()

	// Init logger.
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stdout))
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)

	// Register CLI flags.
	appCfg := &Config{}
	appCfg.RegisterFlags(flag.CommandLine)
	if err := flag.CommandLine.Parse(os.Args[1:]); err != nil {
		level.Error(logger).Log("msg", "failed to parse flags", "err", err)
		os.Exit(1)
	}

	if appCfg.StorageAccountURL == "" {
		level.Error(logger).Log("msg", "-storage-account-url is required")
		os.Exit(1)
	}
	if appCfg.StorageAccountKey == "" {
		if appCfg.SubscriptionID == "" {
			level.Error(logger).Log("msg", "-subscription-id is required when -storage-account-key is not set")
			os.Exit(1)
		}
		if appCfg.StorageResourceGroup == "" {
			level.Error(logger).Log("msg", "-storage-resource-group is required when -storage-account-key is not set")
			os.Exit(1)
		}
	}
	if appCfg.Cluster == "" {
		level.Error(logger).Log("msg", "-cluster is required")
		os.Exit(1)
	}

	// Configure Azure blob client.
	client, err := newBlobClient(ctx, appCfg, logger)
	if err != nil {
		level.Error(logger).Log("msg", "failed to create blob client", "err", err)
		os.Exit(1)
	}

	// Configure OTLP exporter.
	var opts []otlpmetrichttp.Option
	if appCfg.OTLPEndpoint != "" {
		opts = append(opts, otlpmetrichttp.WithEndpointURL(appCfg.OTLPEndpoint))
	}
	if appCfg.OTLPUsername != "" || appCfg.OTLPPassword != "" {
		encoded := base64.StdEncoding.EncodeToString([]byte(appCfg.OTLPUsername + ":" + appCfg.OTLPPassword))
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

	tracker := newTopNTracker()

	level.Info(logger).Log("msg", "exporter started", "poll_interval", appCfg.PollInterval, "max_age", appCfg.MaxAge)

	// Run an initial poll at startup.
	poll(ctx, client, appCfg, exporter, res, tracker, logger)

	// Periodically poll for new blobs.
	ticker := time.NewTicker(appCfg.PollInterval)
	for range ticker.C {
		poll(ctx, client, appCfg, exporter, res, tracker, logger)
	}
}

func poll(ctx context.Context, client *azblob.Client, cfg *Config, exporter sdkmetric.Exporter, res *resource.Resource, tracker *topNTracker, logger log.Logger) {
	checkpoint, err := loadCheckpoint(cfg.CheckpointFile)
	if err != nil {
		level.Error(logger).Log("msg", "failed to load checkpoint", "err", err)
		return
	}

	if err := processMinutes(ctx, client, cfg.BlobPathPrefix, cfg.MaxAge, checkpoint, cfg.CheckpointFile, exporter, res, tracker, cfg.Cluster, cfg, logger); err != nil {
		level.Error(logger).Log("msg", "failed to process minutes", "err", err)
	}
}

// newBlobClient returns a blob client using Shared Key authentication.
// If a storage account key is provided directly, it uses that. Otherwise, it
// fetches the key via the ARM API using DefaultAzureCredential.
func newBlobClient(ctx context.Context, cfg *Config, logger log.Logger) (*azblob.Client, error) {
	// Extract account name from URL (e.g. "myaccount" from "https://myaccount.blob.core.windows.net").
	parsed, err := url.Parse(cfg.StorageAccountURL)
	if err != nil {
		return nil, fmt.Errorf("parsing storage account URL: %w", err)
	}
	accountName := strings.Split(parsed.Hostname(), ".")[0]

	var key string
	if cfg.StorageAccountKey != "" {
		key = cfg.StorageAccountKey
		level.Info(logger).Log("msg", "using provided storage account key", "account", accountName)
	} else {
		var err error
		key, err = fetchStorageKeyViaARM(ctx, cfg, accountName)
		if err != nil {
			return nil, err
		}
		level.Info(logger).Log("msg", "fetched storage account key via ARM", "account", accountName)
	}

	sharedKey, err := azblob.NewSharedKeyCredential(accountName, key)
	if err != nil {
		return nil, fmt.Errorf("creating shared key credential: %w", err)
	}

	return azblob.NewClientWithSharedKeyCredential(cfg.StorageAccountURL, sharedKey, nil)
}

func fetchStorageKeyViaARM(ctx context.Context, cfg *Config, accountName string) (string, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return "", fmt.Errorf("creating Azure credential: %w", err)
	}

	accountsClient, err := armstorage.NewAccountsClient(cfg.SubscriptionID, cred, nil)
	if err != nil {
		return "", fmt.Errorf("creating ARM storage client: %w", err)
	}

	keys, err := accountsClient.ListKeys(ctx, cfg.StorageResourceGroup, accountName, nil)
	if err != nil {
		return "", fmt.Errorf("listing storage account keys: %w", err)
	}
	if len(keys.Keys) == 0 || keys.Keys[0].Value == nil {
		return "", fmt.Errorf("no storage account keys found")
	}

	return *keys.Keys[0].Value, nil
}
