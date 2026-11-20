// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package upload

import (
	"bufio"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/config"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/httpclient"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/resources"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/validation"
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
	tokenCredential  azcore.TokenCredential
	clientOptions    policy.ClientOptions
	s2sTokenProvider httpclient.S2STokenProvider
	s2sAccessContext string

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
	// Validate source
	stream, err := source.Data()
	if err != nil {
		return nil, ingestoptions.NewUploadFailedError(
			ingestoptions.UploadErrorSourceNotReadable, source.Name(), err,
		)
	}
	if validation.IsNil(stream) {
		return nil, ingestoptions.NewUploadFailedError(
			ingestoptions.UploadErrorSourceIsNull, source.Name(), nil,
		)
	}

	name := source.GenerateBlobName()

	size, err := remainingUploadSize(stream)
	if err != nil {
		return nil, ingestoptions.NewUploadFailedError(ingestoptions.UploadErrorSourceNotReadable, source.Name(), err)
	}
	if size == 0 {
		return nil, ingestoptions.NewUploadFailedError(ingestoptions.UploadErrorSourceIsEmpty, source.Name(), nil)
	}
	if !u.ignoreSizeLimit && size >= 0 && size > u.maxDataSize {
		return nil, ingestoptions.NewUploadFailedError(
			ingestoptions.UploadErrorSourceSizeLimitExceed, source.Name(),
			fmt.Errorf("size %d exceeds max %d", size, u.maxDataSize),
		)
	}

	// Get containers
	containers, err := u.selectContainers(ctx, u.uploadMethod)
	if err != nil {
		return nil, err
	}
	if containers.Len() == 0 {
		return nil, ingestoptions.NewIngestClientError("no upload containers available", nil, true)
	}

	effectiveCompression := source.CompressionType()
	if source.ShouldCompress() {
		effectiveCompression = ingestoptions.CompressionGZip
	}

	// Upload with retries and container cycling
	blobSource, err := u.uploadWithRetries(ctx, source, name, stream, containers, effectiveCompression, source.Format())
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
	for _, result := range results.Results {
		if result.IsSuccess() {
			results.SuccessCount++
		} else {
			results.FailureCount++
		}
	}
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
	seeker, seekable := stream.(io.Seeker)
	var startOffset int64
	if seekable {
		var err error
		startOffset, err = seeker.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, fmt.Errorf("get upload stream position: %w", err)
		}
	}

	for attempt := 0; u.retryPolicy.ShouldRetry(attempt); attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if attempt > 0 {
			if _, err := seeker.Seek(startOffset, io.SeekStart); err != nil {
				return nil, fmt.Errorf("rewind upload stream: %w", errors.Join(lastErr, err))
			}
		}
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

		checked := &uploadSizeReader{
			reader: stream, name: source.Name(), max: u.maxDataSize, ignore: u.ignoreSizeLimit,
		}
		buffered := bufio.NewReader(checked)
		_, err := buffered.Peek(1)
		if err == nil {
			err = u.uploadToContainer(ctx, blobPath, buffered, container, source.ShouldCompress())
		}
		if checked.err != nil {
			err = checked.err
		}
		if err != nil {
			lastErr = err
			u.logger.Warn("upload attempt failed",
				"attempt", attempt+1,
				"error", err,
			)
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			var uploadError *ingestoptions.UploadFailedError
			if errors.As(err, &uploadError) && uploadError.IsPermanent {
				return nil, err
			}
			var responseError *azcore.ResponseError
			if errors.As(err, &responseError) && responseError.StatusCode >= 400 && responseError.StatusCode < 500 {
				return nil, ingestoptions.NewIngestError(fmt.Sprintf("storage rejected the upload: %v", err), err, true)
			}
			var clientError *ingestoptions.IngestClientError
			if errors.As(err, &clientError) && clientError.IsPermanent {
				return nil, err
			}
			if !u.retryPolicy.ShouldRetry(attempt + 1) {
				break
			}
			if !seekable {
				return nil, ingestoptions.NewIngestClientError("cannot retry upload of a non-seekable stream", err, true)
			}

			delay := u.retryPolicy.GetDelay(attempt)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
			continue
		}

		blobSrc, err := ingestoptions.NewBlobSource(blobPath, format,
			ingestoptions.WithBlobCompression(compression),
			ingestoptions.WithBlobSourceID(source.SourceID()),
		)
		if err != nil {
			return nil, err
		}
		blobSrc.BlobExactSize = max(0, source.Size())
		return blobSrc, nil
	}

	if lastErr == nil {
		lastErr = ingestoptions.NewIngestClientError("upload failed after all retries", nil, false)
	}
	return nil, lastErr
}

func (u *ContainerUploaderBase) uploadToContainer(ctx context.Context, blobPath string, stream io.Reader, container *resources.ExtendedContainerInfo, compress bool) error {
	ctx, cancel := context.WithTimeout(ctx, ingestoptions.BlobUploadTimeout)
	defer cancel()

	if compress {
		pr, pw := io.Pipe()
		done := make(chan struct{})
		defer func() {
			_ = pr.Close()
			<-done
		}()
		go func(input io.Reader) {
			defer close(done)
			gw := gzip.NewWriter(pw)
			_, copyErr := io.Copy(gw, input)
			_ = pw.CloseWithError(errors.Join(copyErr, gw.Close()))
		}(stream)
		stream = pr
	}

	switch container.UploadMethod {
	case ingestoptions.UploadMethodStorage:
		return u.uploadUsingBlobAPI(ctx, blobPath, stream)
	case ingestoptions.UploadMethodLake:
		return u.uploadUsingDataLakeAPI(ctx, blobPath, stream)
	default:
		return ingestoptions.NewIngestClientError(
			fmt.Sprintf("unsupported upload method: %v", container.UploadMethod), nil, true,
		)
	}
}
