package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/storage/armstorage"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Azure diagnostic log container names (fixed by Azure).
var containerNames = []string{
	"insights-logs-dataplanerequests",
	"insights-logs-partitionkeyruconsumption",
	"insights-logs-partitionkeystatistics",
	"insights-logs-queryruntimestatistics",
}

type Config struct {
	ServerAddress      string
	ServerPort         int
	StorageAccountURL  string
	SubscriptionID     string
	StorageResourceGroup string
	BlobPathPrefix     string
	PollInterval       time.Duration
	CheckpointFile     string
	MaxAge             time.Duration
}

func (c *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&c.ServerAddress, "server-address", "0.0.0.0", "The network address listening for HTTP requests.")
	f.IntVar(&c.ServerPort, "server-port", 8080, "The network port listening for HTTP requests.")
	f.StringVar(&c.StorageAccountURL, "storage-account-url", "", "Azure Blob Storage account URL (e.g. https://myaccount.blob.core.windows.net).")
	f.StringVar(&c.SubscriptionID, "subscription-id", "", "Azure subscription ID containing the storage account.")
	f.StringVar(&c.StorageResourceGroup, "storage-resource-group", "", "Resource group of the storage account.")
	f.StringVar(&c.BlobPathPrefix, "blob-path-prefix", "", "Optional prefix to scope blob listing (e.g. resourceId=SUBSCRIPTIONS/...).")
	f.DurationVar(&c.PollInterval, "poll-interval", time.Minute, "How frequently to poll for new blobs.")
	f.StringVar(&c.CheckpointFile, "checkpoint-file", "./checkpoint.json", "Path to the checkpoint file for tracking processed blobs.")
	f.DurationVar(&c.MaxAge, "max-age", 5*time.Minute, "Maximum age of blobs to process. Blobs older than this are skipped on startup or when the checkpoint is stale.")
}

func main() {
	var (
		ctx     = context.Background()
		reg     = prometheus.NewRegistry()
		metrics = newMetrics(reg)
	)

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
	if appCfg.SubscriptionID == "" {
		level.Error(logger).Log("msg", "-subscription-id is required")
		os.Exit(1)
	}
	if appCfg.StorageResourceGroup == "" {
		level.Error(logger).Log("msg", "-storage-resource-group is required")
		os.Exit(1)
	}

	// Configure Azure client. Fetch storage account keys via ARM (same as az CLI),
	// then use Shared Key auth for blob access.
	client, err := newBlobClient(ctx, appCfg, logger)
	if err != nil {
		level.Error(logger).Log("msg", "failed to create blob client", "err", err)
		os.Exit(1)
	}

	// Register golang instrumentation.
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	// Expose metrics over HTTP.
	go func() {
		level.Info(logger).Log("msg", fmt.Sprintf("exporter listening on %s:%d", appCfg.ServerAddress, appCfg.ServerPort))

		http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{
			ErrorLog: &promhttpErrorLogger{logger: logger},
			Registry: reg,
			Timeout:  10 * time.Second,
		}))

		server := &http.Server{
			Addr:         fmt.Sprintf("%s:%d", appCfg.ServerAddress, appCfg.ServerPort),
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
		}
		if err := server.ListenAndServe(); err != nil {
			level.Error(logger).Log("msg", "failed to run HTTP server")
			os.Exit(1)
		}
	}()

	// Run an initial poll at startup.
	pollContainers(ctx, client, appCfg, metrics, logger)

	// Periodically poll for new blobs.
	interval := time.NewTicker(appCfg.PollInterval)

	for {
		<-interval.C
		pollContainers(ctx, client, appCfg, metrics, logger)
	}
}

func pollContainers(ctx context.Context, client *azblob.Client, cfg *Config, metrics *Metrics, logger log.Logger) {
	checkpoint, err := loadCheckpoint(cfg.CheckpointFile)
	if err != nil {
		level.Error(logger).Log("msg", "failed to load checkpoint", "err", err)
		metrics.processingErrorsTotal.Inc()
		return
	}

	for _, containerName := range containerNames {
		if err := processContainer(ctx, client, containerName, cfg.BlobPathPrefix, cfg.MaxAge, checkpoint, cfg.CheckpointFile, metrics, logger); err != nil {
			level.Error(logger).Log("msg", "failed to process container", "container", containerName, "err", err)
		}
	}
}

// newBlobClient fetches the storage account key via the ARM API (like az CLI does)
// and returns a blob client using Shared Key authentication.
func newBlobClient(ctx context.Context, cfg *Config, logger log.Logger) (*azblob.Client, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("creating Azure credential: %w", err)
	}

	// Extract account name from URL (e.g. "myaccount" from "https://myaccount.blob.core.windows.net").
	parsed, err := url.Parse(cfg.StorageAccountURL)
	if err != nil {
		return nil, fmt.Errorf("parsing storage account URL: %w", err)
	}
	accountName := strings.Split(parsed.Hostname(), ".")[0]

	// Fetch storage account keys via ARM.
	accountsClient, err := armstorage.NewAccountsClient(cfg.SubscriptionID, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("creating ARM storage client: %w", err)
	}

	keys, err := accountsClient.ListKeys(ctx, cfg.StorageResourceGroup, accountName, nil)
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

	return azblob.NewClientWithSharedKeyCredential(cfg.StorageAccountURL, sharedKey, nil)
}

type promhttpErrorLogger struct {
	logger log.Logger
}

func (l *promhttpErrorLogger) Println(v ...interface{}) {
	level.Error(l.logger).Log(v...)
}
