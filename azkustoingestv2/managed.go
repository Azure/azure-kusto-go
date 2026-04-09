// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package azkustoingestv2

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/policy"
)

// ManagedStreamingIngestClient combines streaming and queued ingestion.
//
// It intelligently chooses between streaming and queued ingestion based on:
//   - Data size (falls back to queued for large data)
//   - Server response (falls back to queued on certain errors)
//   - Policy decisions (configured managed streaming policy)
//
// When streaming fails with transient errors, the client retries.
// When it fails with permanent errors (e.g., streaming disabled), it
// falls back to queued ingestion.
type ManagedStreamingIngestClient struct {
	streamingClient *StreamingIngestClient
	queuedClient    *QueuedIngestClient
	policy          policy.ManagedStreamingPolicy
	logger          *slog.Logger
}

// NewManagedStreamingIngestClient creates a new ManagedStreamingIngestClient.
func NewManagedStreamingIngestClient(
	streamingClient *StreamingIngestClient,
	queuedClient *QueuedIngestClient,
	p policy.ManagedStreamingPolicy,
) *ManagedStreamingIngestClient {
	if p == nil {
		p = policy.NewDefaultManagedStreamingPolicy()
	}
	return &ManagedStreamingIngestClient{
		streamingClient: streamingClient,
		queuedClient:    queuedClient,
		policy:          p,
		logger:          slog.Default(),
	}
}

// Ingest ingests data, choosing between streaming and queued based on policy.
func (c *ManagedStreamingIngestClient) Ingest(ctx context.Context, database, table string, source ingestoptions.IngestionSource, props *ingestoptions.IngestRequestProperties) (*ingestoptions.ExtendedIngestResponse, error) {
	if database == "" {
		return nil, ingestoptions.NewIngestRequestError("database cannot be empty", nil, true)
	}
	if table == "" {
		return nil, ingestoptions.NewIngestRequestError("table cannot be empty", nil, true)
	}

	switch s := source.(type) {
	case *ingestoptions.BlobSource:
		return c.ingestBlob(ctx, s, database, table, props)
	case ingestoptions.LocalSource:
		return c.ingestLocal(ctx, s, database, table, props)
	default:
		return nil, ingestoptions.NewIngestClientError(
			fmt.Sprintf("unsupported source type: %T", source), nil, true,
		)
	}
}

func (c *ManagedStreamingIngestClient) ingestBlob(ctx context.Context, source *ingestoptions.BlobSource, database, table string, props *ingestoptions.IngestRequestProperties) (*ingestoptions.ExtendedIngestResponse, error) {
	if c.shouldUseQueuedByPolicy(source, database, table) {
		return c.invokeQueued(ctx, database, table, source, props)
	}
	return c.invokeStreamingWithFallback(ctx, source, database, table, props)
}

func (c *ManagedStreamingIngestClient) ingestLocal(ctx context.Context, source ingestoptions.LocalSource, database, table string, props *ingestoptions.IngestRequestProperties) (*ingestoptions.ExtendedIngestResponse, error) {
	// Check stream validity
	stream, err := source.Data()
	if err != nil {
		return nil, ingestoptions.NewIngestClientError("failed to get source data", err, true)
	}

	// Check size
	var streamSize int64
	if sizer, ok := stream.(interface{ Len() int }); ok {
		streamSize = int64(sizer.Len())
	}

	if c.shouldUseQueuedBySize(streamSize) || c.shouldUseQueuedByPolicy(source, database, table) {
		return c.invokeQueued(ctx, database, table, source, props)
	}
	return c.invokeStreamingWithFallback(ctx, source, database, table, props)
}

func (c *ManagedStreamingIngestClient) shouldUseQueuedBySize(size int64) bool {
	threshold := int64(float64(ingestoptions.StreamingMaxRequestBodySize) * c.policy.DataSizeFactor())
	if size > threshold {
		c.logger.Info("data size too big for streaming, using queued",
			"size", size,
			"threshold", threshold,
		)
		return true
	}
	return false
}

func (c *ManagedStreamingIngestClient) shouldUseQueuedByPolicy(source ingestoptions.IngestionSource, database, table string) bool {
	return c.policy.ShouldDefaultToQueuedIngestion(source, database, table)
}

func (c *ManagedStreamingIngestClient) invokeStreamingWithFallback(ctx context.Context, source ingestoptions.IngestionSource, database, table string, props *ingestoptions.IngestRequestProperties) (*ingestoptions.ExtendedIngestResponse, error) {
	retryPolicy := c.policy.RetryPolicy()
	var lastErr error

	for attempt := 0; ; attempt++ {
		if !retryPolicy.ShouldRetry(attempt) && attempt > 0 {
			break
		}

		startTime := time.Now()

		result, err := c.streamingClient.Ingest(ctx, database, table, source, props)
		duration := time.Since(startTime)

		if err == nil {
			c.policy.StreamingSuccessCallback(source, database, table, policy.ManagedStreamingRequestSuccessDetails{
				Duration: duration,
			})
			return result, nil
		}

		lastErr = err
		isPermanent := isPermError(err)
		category := categorizeError(err)

		c.policy.StreamingErrorCallback(source, database, table, policy.ManagedStreamingRequestFailureDetails{
			Duration:      duration,
			IsPermanent:   isPermanent,
			ErrorCategory: category,
			Err:           err,
		})

		// If permanent error, decide whether to fall back
		if isPermanent {
			switch category {
			case policy.ErrorCategoryStreamingIngestionOff:
				if !c.policy.ContinueWhenStreamingIngestionUnavailable() {
					return nil, err
				}
			case policy.ErrorCategoryTableConfigurationPreventsStreaming:
				// Fall through to queued
			default:
				// For other permanent errors, fall back
			}
			break
		}

		// Reset stream for retry if possible
		c.resetSourceIfPossible(source)

		if attempt+1 < retryPolicy.MaxRetries() {
			delay := retryPolicy.GetDelay(attempt)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-waitDuration(delay):
			}
		}
	}

	// Streaming failed, fall back to queued
	c.logger.Warn("streaming ingestion failed, falling back to queued",
		"lastError", lastErr,
	)
	return c.invokeQueued(ctx, database, table, source, props)
}

func (c *ManagedStreamingIngestClient) resetSourceIfPossible(source ingestoptions.IngestionSource) {
	if ls, ok := source.(ingestoptions.LocalSource); ok {
		stream, err := ls.Data()
		if err != nil {
			return
		}
		if seeker, ok := stream.(io.Seeker); ok {
			_, _ = seeker.Seek(0, io.SeekStart)
		}
	}
}

func (c *ManagedStreamingIngestClient) invokeQueued(ctx context.Context, database, table string, source ingestoptions.IngestionSource, props *ingestoptions.IngestRequestProperties) (*ingestoptions.ExtendedIngestResponse, error) {
	return c.queuedClient.Ingest(ctx, database, table, source, props)
}

// GetOperationSummary delegates to the appropriate client based on IngestKind.
func (c *ManagedStreamingIngestClient) GetOperationSummary(ctx context.Context, op *IngestionOperation) (*OperationStatus, error) {
	if op.IngestKind == ingestoptions.IngestKindStreaming {
		c.logger.Warn("GetOperationSummary called for streaming operation; streaming operations are not tracked")
		return &OperationStatus{}, nil
	}
	return c.queuedClient.GetOperationSummary(ctx, op)
}

// GetOperationDetails delegates to the appropriate client based on IngestKind.
func (c *ManagedStreamingIngestClient) GetOperationDetails(ctx context.Context, op *IngestionOperation) (*StatusResponse, error) {
	if op.IngestKind == ingestoptions.IngestKindStreaming {
		c.logger.Warn("GetOperationDetails called for streaming operation; streaming operations are not tracked")
		return &StatusResponse{Status: &OperationStatus{}}, nil
	}
	return c.queuedClient.GetOperationDetails(ctx, op)
}

// Close closes both the streaming and queued clients.
func (c *ManagedStreamingIngestClient) Close() error {
	var firstErr error
	if err := c.streamingClient.Close(); err != nil {
		firstErr = err
	}
	if err := c.queuedClient.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// categorizeError maps an error to a ManagedStreamingErrorCategory.
func categorizeError(err error) policy.ManagedStreamingErrorCategory {
	if err == nil {
		return policy.ErrorCategoryUnknown
	}
	// TODO: Implement proper error categorization based on HTTP response codes
	return policy.ErrorCategoryOther
}

// isPermError checks if an error is permanent.
func isPermError(err error) bool {
	if ie, ok := err.(*ingestoptions.IngestError); ok {
		return ie.IsPermanent
	}
	return false
}

// waitDuration returns a channel that receives after the given duration.
func waitDuration(d time.Duration) <-chan time.Time {
	return time.After(d)
}
