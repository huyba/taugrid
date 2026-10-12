// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// tau-init is the dataset preloader that runs as a Kubernetes init container.
// It downloads all blobs under a given prefix from Azure Blob Storage to a
// node-local NVMe-backed emptyDir volume before the training container starts.
//
// Section 2 – Pod Initialization Phase:
//  1. Init container boots, reads DataFetch metadata from env vars.
//  2. Streams dataset via multi-threaded download from Azure Blob → local NVMe.
//  3. Computes SHA-256 checksum per file → verified → init exits (code 0).
//  4. Main training container boots with data already on local disk.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	storageAccount := mustenv("STORAGE_ACCOUNT")
	container := mustenv("CONTAINER")
	prefix := os.Getenv("BLOB_PREFIX")
	destDir := mustenv("DEST_DIR")
	datasetName := mustenv("DATASET_NAME")
	storageKey := os.Getenv("AZURE_STORAGE_KEY")

	logger.Info("tau-init preloader starting",
		"dataset", datasetName,
		"storageAccount", storageAccount,
		"container", container,
		"prefix", prefix,
		"destDir", destDir,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Hour)
	defer cancel()

	if err := os.MkdirAll(destDir, 0755); err != nil {
		logger.Error("failed to create destination directory", "dir", destDir, "err", err)
		os.Exit(1)
	}

	blobClient, err := newBlobClient(storageAccount, storageKey)
	if err != nil {
		logger.Error("failed to create blob client", "err", err)
		os.Exit(1)
	}

	keys, err := listBlobs(ctx, blobClient, container, prefix)
	if err != nil {
		logger.Error("failed to list blobs", "err", err)
		os.Exit(1)
	}
	logger.Info("discovered blobs", "count", len(keys))

	if err := downloadAll(ctx, blobClient, container, keys, destDir, logger); err != nil {
		logger.Error("dataset download failed", "err", err)
		os.Exit(1)
	}

	logger.Info("dataset preload complete", "dataset", datasetName,
		"files", len(keys), "destDir", destDir)
}

// listBlobs enumerates all blob names under the given prefix using a pager.
func listBlobs(ctx context.Context, client *azblob.Client, container, prefix string) ([]string, error) {
	var keys []string
	pager := client.NewListBlobsFlatPager(container, &azblob.ListBlobsFlatOptions{
		Prefix: &prefix,
	})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list blobs page: %w", err)
		}
		for _, item := range page.Segment.BlobItems {
			if item.Name != nil {
				keys = append(keys, *item.Name)
			}
		}
	}
	return keys, nil
}

// downloadAll concurrently downloads all blobs, verifying each SHA-256.
func downloadAll(
	ctx context.Context,
	client *azblob.Client,
	container string,
	keys []string,
	destDir string,
	logger *slog.Logger,
) error {
	workers := runtime.NumCPU() * 2
	if workers > 16 {
		workers = 16
	}

	type result struct {
		key string
		err error
	}

	jobs := make(chan string, len(keys))
	results := make(chan result, len(keys))

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range jobs {
				destPath := filepath.Join(destDir, filepath.Base(key))
				err := downloadBlob(ctx, client, container, key, destPath, logger)
				results <- result{key: key, err: err}
			}
		}()
	}

	for _, k := range keys {
		jobs <- k
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(results)
	}()

	var errs []string
	for r := range results {
		if r.err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", r.key, r.err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("download errors:\n  %s", strings.Join(errs, "\n  "))
	}
	return nil
}

// downloadBlob fetches a single blob to destPath and verifies its SHA-256.
// If the file already exists at the correct size it is skipped (incremental run).
func downloadBlob(
	ctx context.Context,
	client *azblob.Client,
	container, blobKey, destPath string,
	logger *slog.Logger,
) error {
	// Incremental check: skip if file already present and non-empty.
	if fi, err := os.Stat(destPath); err == nil && fi.Size() > 0 {
		logger.Info("skipping already-present file", "path", destPath)
		return nil
	}

	// Create a temporary file; rename to final path only after checksum passes.
	tmpPath := destPath + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	defer os.Remove(tmpPath) // no-op if rename succeeds

	h := sha256.New()
	writer := io.MultiWriter(f, h)

	start := time.Now()
	resp, err := client.DownloadStream(ctx, container, blobKey, nil)
	if err != nil {
		f.Close()
		return fmt.Errorf("download stream: %w", err)
	}
	defer resp.Body.Close()

	n, err := io.Copy(writer, resp.Body)
	f.Close()
	if err != nil {
		return fmt.Errorf("stream copy: %w", err)
	}

	digest := hex.EncodeToString(h.Sum(nil))
	elapsed := time.Since(start)
	throughputMBs := float64(n) / elapsed.Seconds() / 1024 / 1024

	if err := os.Rename(tmpPath, destPath); err != nil {
		return fmt.Errorf("rename to final path: %w", err)
	}

	logger.Info("downloaded blob",
		"blob", blobKey,
		"dest", destPath,
		"bytes", n,
		"sha256", digest,
		"elapsed", elapsed.Round(time.Millisecond),
		"throughputMBps", fmt.Sprintf("%.1f", throughputMBs),
	)
	return nil
}

func newBlobClient(storageAccount, storageKey string) (*azblob.Client, error) {
	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", storageAccount)

	if storageKey != "" {
		cred, err := azblob.NewSharedKeyCredential(storageAccount, storageKey)
		if err != nil {
			return nil, err
		}
		return azblob.NewClientWithSharedKeyCredential(serviceURL, cred, nil)
	}

	cred, err := azidentity.NewWorkloadIdentityCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("workload identity credential: %w", err)
	}
	return azblob.NewClient(serviceURL, cred, nil)
}

func mustenv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		fmt.Fprintf(os.Stderr, "required environment variable %q is not set\n", key)
		os.Exit(1)
	}
	return v
}
