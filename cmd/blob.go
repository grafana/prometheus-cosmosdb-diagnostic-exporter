package main

import (
	"bufio"
	"context"
	"encoding/json"
	"regexp"
	"strconv"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
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

// downloadAndProcessBlob downloads a blob and processes each JSON line as a diagnostic record.
func downloadAndProcessBlob(ctx context.Context, client *azblob.Client, containerName, blobName string, metrics *Metrics, logger log.Logger) error {
	resp, err := downloadBlobWithRetry(ctx, client, containerName, blobName)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024) // 1MB max line size

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
			metrics.processingErrorsTotal.Inc()
			continue
		}

		processRecord(&record, metrics)
		metrics.recordsProcessedTotal.WithLabelValues(record.Category).Inc()
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
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

// processContainer lists and processes new blobs in a single container,
// updating the checkpoint after each blob. Blobs older than maxAge are skipped.
func processContainer(
	ctx context.Context,
	client *azblob.Client,
	containerName string,
	prefix string,
	maxAge time.Duration,
	checkpoint *Checkpoint,
	checkpointFile string,
	metrics *Metrics,
	logger log.Logger,
) error {
	containerCp := checkpoint.Containers[containerName]

	blobs, err := listNewBlobs(ctx, client, containerName, prefix, containerCp.LastBlobPath)
	if err != nil {
		return err
	}

	// Filter out blobs older than maxAge.
	cutoff := time.Now().UTC().Add(-maxAge)
	filtered := blobs[:0]
	for _, b := range blobs {
		if t := parseBlobPathTime(b); t.IsZero() || !t.Before(cutoff) {
			filtered = append(filtered, b)
		}
	}
	blobs = filtered

	if len(blobs) == 0 {
		return nil
	}

	level.Info(logger).Log("msg", "processing new blobs", "container", containerName, "count", len(blobs))

	for _, blobName := range blobs {
		if err := downloadAndProcessBlob(ctx, client, containerName, blobName, metrics, logger); err != nil {
			level.Error(logger).Log("msg", "failed to process blob", "container", containerName, "blob", blobName, "err", err)
			metrics.processingErrorsTotal.Inc()
			return err
		}

		metrics.blobsProcessedTotal.Inc()

		containerCp.LastBlobPath = blobName
		containerCp.LastProcessedTime = time.Now()
		checkpoint.Containers[containerName] = containerCp

		if err := saveCheckpoint(checkpointFile, checkpoint); err != nil {
			level.Error(logger).Log("msg", "failed to save checkpoint", "err", err)
			metrics.processingErrorsTotal.Inc()
		}

		metrics.lastProcessedTimestampSeconds.SetToCurrentTime()
	}

	return nil
}
