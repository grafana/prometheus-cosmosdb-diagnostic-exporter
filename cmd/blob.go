package main

import (
	"bufio"
	"context"
	"encoding/json"
	"regexp"
	"slices"
	"strconv"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
)

// blobPathTimeRe extracts y=YYYY/m=MM/d=DD/h=HH/m=MM from a blob path.
var blobPathTimeRe = regexp.MustCompile(`y=(\d{4})/m=(\d{2})/d=(\d{2})/h=(\d{2})/m=(\d{2})`)

// parseBlobPathTime extracts the timestamp encoded in the blob path.
// Returns zero time if the path doesn't match the expected pattern.
func parseBlobPathTime(blobPath string) time.Time {
	m := blobPathTimeRe.FindStringSubmatch(blobPath)
	if m == nil {
		return time.Time{}
	}

	year, _ := strconv.Atoi(m[1])
	month, _ := strconv.Atoi(m[2])
	day, _ := strconv.Atoi(m[3])
	hour, _ := strconv.Atoi(m[4])
	min, _ := strconv.Atoi(m[5])

	return time.Date(year, time.Month(month), day, hour, min, 0, 0, time.UTC)
}

// listNewBlobs returns blob names in the container that are lexicographically
// after lastBlobPath, filtered by the given prefix. Results are returned in
// lexicographic (chronological) order.
func listNewBlobs(ctx context.Context, client *azblob.Client, containerName, prefix, lastBlobPath string) ([]string, error) {
	var blobs []string

	pager := client.NewListBlobsFlatPager(containerName, &azblob.ListBlobsFlatOptions{
		Prefix: &prefix,
	})

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, item := range page.Segment.BlobItems {
			name := *item.Name
			if name > lastBlobPath {
				blobs = append(blobs, name)
			}
		}
	}

	return blobs, nil
}

// filterBlobsByAge filters out blobs older than maxAge.
func filterBlobsByAge(blobs []string, maxAge time.Duration, now time.Time) []string {
	cutoff := now.Add(-maxAge)
	filtered := make([]string, 0, len(blobs))
	for _, b := range blobs {
		if t := parseBlobPathTime(b); t.IsZero() || !t.Before(cutoff) {
			filtered = append(filtered, b)
		}
	}
	return filtered
}

// minuteBlobs groups blob paths by minute across all containers.
type minuteBlobs struct {
	Minute time.Time
	Blobs  map[string][]string // containerName → []blobPaths
}

// groupByMinute groups blob paths from all containers by their minute timestamp
// and excludes the global latest minute (which may still be receiving writes).
func groupByMinute(containerBlobs map[string][]string) []minuteBlobs {
	// Group by minute.
	byMinute := map[time.Time]map[string][]string{}
	for container, blobs := range containerBlobs {
		for _, b := range blobs {
			t := parseBlobPathTime(b)
			if t.IsZero() {
				continue
			}
			if byMinute[t] == nil {
				byMinute[t] = map[string][]string{}
			}
			byMinute[t][container] = append(byMinute[t][container], b)
		}
	}

	// Find the global latest minute across all containers.
	var latestMinute time.Time
	for t := range byMinute {
		if t.After(latestMinute) {
			latestMinute = t
		}
	}

	// Exclude the latest minute and sort the rest.
	var minutes []minuteBlobs
	for t, blobs := range byMinute {
		if t.Equal(latestMinute) {
			continue
		}
		minutes = append(minutes, minuteBlobs{Minute: t, Blobs: blobs})
	}
	slices.SortFunc(minutes, func(a, b minuteBlobs) int {
		return a.Minute.Compare(b.Minute)
	})
	return minutes
}

// downloadBlob downloads a blob and parses each JSON line as a diagnostic record.
func downloadBlob(ctx context.Context, client *azblob.Client, containerName, blobName string, logger log.Logger) ([]DiagnosticRecord, error) {
	resp, err := downloadBlobWithRetry(ctx, client, containerName, blobName)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024) // 1MB max line size

	var records []DiagnosticRecord
	var lineNum int
	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var record DiagnosticRecord
		if err := json.Unmarshal(line, &record); err != nil {
			level.Warn(logger).Log("msg", "failed to parse log record", "blob", blobName, "line", lineNum, "err", err)
			continue
		}
		records = append(records, record)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return records, nil
}

func downloadBlobWithRetry(ctx context.Context, client *azblob.Client, containerName, blobName string) (azblob.DownloadStreamResponse, error) {
	var lastErr error

	retries := backoff.New(ctx, backoff.Config{
		MinBackoff: time.Second,
		MaxBackoff: 10 * time.Second,
		MaxRetries: 3,
	})

	for retries.Ongoing() {
		resp, err := client.DownloadStream(ctx, containerName, blobName, nil)
		if err != nil {
			lastErr = err
			retries.Wait()
			continue
		}
		return resp, nil
	}

	return azblob.DownloadStreamResponse{}, lastErr
}

// processMinutes lists new blobs across all containers, groups them by minute,
// and processes each minute: builds the partition mapping from PartitionKeyRUConsumption,
// then aggregates all records into OTLP metrics and exports them.
func processMinutes(
	ctx context.Context,
	client *azblob.Client,
	prefix string,
	maxAge time.Duration,
	checkpoint *Checkpoint,
	checkpointFile string,
	exporter sdkmetric.Exporter,
	res *resource.Resource,
	mapping *partitionMapping,
	cluster string,
	logger log.Logger,
) error {
	now := time.Now().UTC()

	// Phase 1: List new blobs for all containers, filtered by age.
	containerBlobs := map[string][]string{}
	for _, containerName := range containerNames {
		cp := checkpoint.Containers[containerName]
		blobs, err := listNewBlobs(ctx, client, containerName, prefix, cp.LastBlobPath)
		if err != nil {
			level.Error(logger).Log("msg", "failed to list blobs", "container", containerName, "err", err)
			continue
		}
		blobs = filterBlobsByAge(blobs, maxAge, now)
		if len(blobs) > 0 {
			containerBlobs[containerName] = blobs
		}
	}

	// Phase 2: Group by minute, excluding the global latest minute.
	minutes := groupByMinute(containerBlobs)
	if len(minutes) == 0 {
		return nil
	}

	level.Info(logger).Log("msg", "processing minutes", "count", len(minutes),
		"from", minutes[0].Minute, "to", minutes[len(minutes)-1].Minute)

	// Phase 3: Process each minute.
	for _, mb := range minutes {
		var allRecords []DiagnosticRecord

		// Download PartitionKeyRUConsumption first to build the mapping.
		pkruContainer := "insights-logs-partitionkeyruconsumption"
		for _, blobName := range mb.Blobs[pkruContainer] {
			records, err := downloadBlob(ctx, client, pkruContainer, blobName, logger)
			if err != nil {
				level.Error(logger).Log("msg", "failed to download blob", "container", pkruContainer, "blob", blobName, "err", err)
				return err
			}
			mapping.update(records)
			allRecords = append(allRecords, records...)
		}

		// Download remaining containers.
		for containerName, blobs := range mb.Blobs {
			if containerName == pkruContainer {
				continue // Already processed above.
			}
			for _, blobName := range blobs {
				records, err := downloadBlob(ctx, client, containerName, blobName, logger)
				if err != nil {
					level.Error(logger).Log("msg", "failed to download blob", "container", containerName, "blob", blobName, "err", err)
					return err
				}
				allRecords = append(allRecords, records...)
			}
		}

		// Build and export OTLP metrics.
		level.Info(logger).Log("msg", "processing minute", "ts", mb.Minute, "records", len(allRecords))
		metrics := buildMinuteMetrics(allRecords, mapping, cluster, mb.Minute)
		if len(metrics) > 0 {
			rm := &metricdata.ResourceMetrics{
				Resource: res,
				ScopeMetrics: []metricdata.ScopeMetrics{
					{Metrics: metrics},
				},
			}
			level.Info(logger).Log("msg", "pushing metrics", "ts", mb.Minute, "series", countSeries(metrics))
			if err := exportMinuteMetrics(ctx, exporter, rm, logger); err != nil {
				return err
			}

			// Push NaN points 30s after the real data to terminate gauge lines.
			nullTS := mb.Minute.Add(30 * time.Second)
			nullMetrics := buildNullMetrics(metrics, nullTS)
			if len(nullMetrics) > 0 {
				nullRM := &metricdata.ResourceMetrics{
					Resource: res,
					ScopeMetrics: []metricdata.ScopeMetrics{
						{Metrics: nullMetrics},
					},
				}
				level.Info(logger).Log("msg", "pushing null metrics to terminate gauge lines", "ts", nullTS, "series", countSeries(nullMetrics))
				if err := exportMinuteMetrics(ctx, exporter, nullRM, logger); err != nil {
					return err
				}
			}
		}

		// Update checkpoints for all blobs processed in this minute.
		for containerName, blobs := range mb.Blobs {
			cp := checkpoint.Containers[containerName]
			for _, b := range blobs {
				if b > cp.LastBlobPath {
					cp.LastBlobPath = b
				}
			}
			cp.LastProcessedTime = time.Now()
			checkpoint.Containers[containerName] = cp
		}
		if err := saveCheckpoint(checkpointFile, checkpoint); err != nil {
			level.Error(logger).Log("msg", "failed to save checkpoint", "err", err)
		}
	}

	return nil
}
