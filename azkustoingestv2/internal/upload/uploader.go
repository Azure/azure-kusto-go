// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package upload

import (
	"compress/gzip"
	"context"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/config"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/resources"
)

// Uploader defines the interface for uploading data sources to blob storage.
type Uploader interface {
	io.Closer

	// Upload uploads a single local source to blob storage and returns a BlobSource.
	Upload(ctx context.Context, source ingestoptions.LocalSource) (*ingestoptions.BlobSource, error)

	// UploadMany uploads multiple local sources to blob storage.
	UploadMany(ctx context.Context, sources []ingestoptions.LocalSource) (*ingestoptions.UploadResults, error)

	// SetIgnoreSizeLimit sets whether to ignore the max data size during upload.
	SetIgnoreSizeLimit(ignore bool)
}

// selectContainersFunc is the function type for selecting containers.
type selectContainersFunc func(ctx context.Context, method ingestoptions.UploadMethod) (*resources.RoundRobinContainerList, error)

// ContainerUploaderBase provides common upload functionality with retry and container cycling.
type ContainerUploaderBase struct {
	retryPolicy      ingestoptions.IngestRetryPolicy
	maxConcurrency   int
	maxDataSize      int64
	configCache      config.ConfigurationCache
	uploadMethod     ingestoptions.UploadMethod
	ignoreSizeLimit  bool
	selectContainers selectContainersFunc

	logger *slog.Logger
}

// NewContainerUploaderBase creates a new ContainerUploaderBase.
func NewContainerUploaderBase(
	retryPolicy ingestoptions.IngestRetryPolicy,
	maxConcurrency int,
	maxDataSize int64,
	configCache config.ConfigurationCache,
	uploadMethod ingestoptions.UploadMethod,
) *ContainerUploaderBase {
	return &ContainerUploaderBase{
		retryPolicy:    retryPolicy,
		maxConcurrency: maxConcurrency,
		maxDataSize:    maxDataSize,
		configCache:    configCache,
		uploadMethod:   uploadMethod,
		logger:         slog.Default(),
	}
}

func (u *ContainerUploaderBase) SetIgnoreSizeLimit(ignore bool) {
	u.ignoreSizeLimit = ignore
}

// Upload uploads a single local source with retry and container cycling.
func (u *ContainerUploaderBase) Upload(ctx context.Context, source ingestoptions.LocalSource) (*ingestoptions.BlobSource, error) {
	stream, err := source.Data()
	if err != nil {
		return nil, ingestoptions.NewIngestClientError("failed to get source data", err, true)
	}

	name := source.GenerateBlobName()

	// Check size limit if not ignored
	if !u.ignoreSizeLimit {
		if sizer, ok := stream.(interface{ Len() int }); ok {
			size := int64(sizer.Len())
			if size > u.maxDataSize {
				return nil, ingestoptions.NewIngestSizeLimitExceededError(size, u.maxDataSize)
			}
		}
	}

	// Get containers
	containers, err := u.selectContainers(ctx, u.uploadMethod)
	if err != nil {
		return nil, err
	}
	if containers.Len() == 0 {
		return nil, ingestoptions.NewIngestClientError("no upload containers available", nil, true)
	}

	// Compress stream if needed
	preparedStream := stream
	effectiveCompression := source.CompressionType()
	if source.ShouldCompress() {
		u.logger.Debug("auto-compressing stream", "name", name)
		pr, pw := io.Pipe()
		go func() {
			gw := gzip.NewWriter(pw)
			_, copyErr := io.Copy(gw, stream)
			if closeErr := gw.Close(); closeErr != nil && copyErr == nil {
				copyErr = closeErr
			}
			pw.CloseWithError(copyErr)
		}()
		preparedStream = pr
		effectiveCompression = ingestoptions.CompressionGZip
	}

	// Upload with retries and container cycling
	blobSource, err := u.uploadWithRetries(ctx, source, name, preparedStream, containers, effectiveCompression, source.Format())
	if err != nil {
		return nil, err
	}

	return blobSource, nil
}

// UploadMany uploads multiple sources concurrently.
func (u *ContainerUploaderBase) UploadMany(ctx context.Context, sources []ingestoptions.LocalSource) (*ingestoptions.UploadResults, error) {
	results := &ingestoptions.UploadResults{
		Results: make([]ingestoptions.UploadResult, len(sources)),
	}

	concurrency := u.maxConcurrency
	if concurrency <= 0 {
		concurrency = 1
	}
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	for i, src := range sources {
		wg.Add(1)
		go func(idx int, s ingestoptions.LocalSource) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			blobSrc, err := u.Upload(ctx, s)
			if err != nil {
				results.Results[idx] = ingestoptions.UploadResult{
					Error: err,
				}
			} else {
				results.Results[idx] = ingestoptions.UploadResult{
					BlobSource: blobSrc,
				}
			}
		}(i, src)
	}

	wg.Wait()
	return results, nil
}

// Close closes the base uploader.
func (u *ContainerUploaderBase) Close() error {
	return nil
}

// uploadWithRetries performs the actual upload with retry logic and container cycling.
func (u *ContainerUploaderBase) uploadWithRetries(
	ctx context.Context,
	source ingestoptions.LocalSource,
	name string,
	stream io.Reader,
	containers *resources.RoundRobinContainerList,
	compression ingestoptions.CompressionType,
	format ingestoptions.DataFormat,
) (*ingestoptions.BlobSource, error) {
	var lastErr error

	for attempt := 0; u.retryPolicy.ShouldRetry(attempt); attempt++ {
		container := containers.Next()
		if container == nil {
			lastErr = ingestoptions.NewIngestClientError("no containers available", nil, true)
			break
		}

		blobPath := container.BuildBlobPath(name, compression)
		u.logger.Debug("uploading to container",
			"attempt", attempt+1,
			"container", container.ContainerName,
			"blob", name,
		)

		// Here we would perform the actual blob upload using Azure SDK.
		// This is a placeholder for the actual upload implementation.
		err := u.uploadToBlob(ctx, blobPath, stream, container)
		if err != nil {
			lastErr = err
			u.logger.Warn("upload attempt failed",
				"attempt", attempt+1,
				"error", err,
			)

			if attempt+1 < u.retryPolicy.MaxRetries() {
				delay := u.retryPolicy.GetDelay(attempt)
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(delay):
				}
			}
			continue
		}

		blobSrc, err := ingestoptions.NewBlobSource(blobPath, format)
		if err != nil {
			return nil, err
		}
		return blobSrc, nil
	}

	if lastErr == nil {
		lastErr = ingestoptions.NewIngestClientError("upload failed after all retries", nil, false)
	}
	return nil, lastErr
}

// uploadToBlob performs the actual upload to Azure Blob Storage.
// This is a placeholder that should be implemented with the Azure SDK.
func (u *ContainerUploaderBase) uploadToBlob(ctx context.Context, blobPath string, stream io.Reader, container *resources.ExtendedContainerInfo) error {
	// TODO: Implement actual blob upload using Azure Blob SDK (azblob).
	// The implementation should:
	// 1. Create a BlobClient using the container's SAS URL
	// 2. Upload the stream using BlockBlobClient.Upload or StageBlock/CommitBlockList
	// 3. Handle the upload timeout (BLOB_UPLOAD_TIMEOUT_HOURS)
	return nil
}
