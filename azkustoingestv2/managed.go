// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package azkustoingestv2

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/policy"
)

// ManagedStreamingIngestClient combines streaming and queued ingestion.
//
// It intelligently chooses between streaming and queued ingestion based on:
//   - Data size (falls back to queued for large data)
//   - Server response (falls back to queued after transient retries are exhausted)
//   - Policy decisions (configured managed streaming policy)
//
// When streaming fails with transient errors, the client retries before falling back.
// Permanent streaming errors are returned without queued fallback.
type ManagedStreamingIngestClient struct {
	streamingClient *StreamingIngestClient
	queuedClient    *QueuedIngestClient
	policy          policy.ManagedStreamingPolicy
	logger          *slog.Logger
}

// newManagedStreamingIngestClient creates a new ManagedStreamingIngestClient.
func newManagedStreamingIngestClient(
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
	defer closeIngestionSource(source, c.logger)
	if database == "" {
		return nil, ingestoptions.NewIngestRequestError("database cannot be empty", nil, true)
	}
	if table == "" {
		return nil, ingestoptions.NewIngestRequestError("table cannot be empty", nil, true)
	}
	if err := validateIngestionSource(source); err != nil {
		return nil, err
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
	if c.shouldUseQueuedBySize(source.Size()) || c.shouldUseQueuedByPolicy(source, database, table) {
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
	originalSource := source
	if local, ok := source.(ingestoptions.LocalSource); ok {
		props = normalizeIngestProperties(source, props)
		prepared, useQueued, err := c.prepareLocalSource(ctx, local)
		if err != nil {
			return nil, err
		}
		source = prepared
		if useQueued {
			return c.invokeQueued(ctx, database, table, source, props)
		}
	}

	retryPolicy := c.policy.RetryPolicy()
	var lastErr error

	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !retryPolicy.ShouldRetry(attempt) && attempt > 0 {
			break
		}

		startTime := time.Now()

		result, err := c.streamingClient.ingest(ctx, database, table, source, props)
		duration := time.Since(startTime)

		if err == nil {
			c.policy.StreamingSuccessCallback(originalSource, database, table, policy.ManagedStreamingRequestSuccessDetails{
				Duration: duration,
			})
			return result, nil
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		lastErr = err
		isPermanent := isPermError(err)
		category := categorizeError(err)

		c.policy.StreamingErrorCallback(originalSource, database, table, policy.ManagedStreamingRequestFailureDetails{
			Duration:      duration,
			IsPermanent:   isPermanent,
			ErrorCategory: category,
			Err:           err,
		})

		if isPermanent {
			return nil, err
		}

		if attempt+1 < retryPolicy.MaxRetries() {
			delay := retryPolicy.GetDelay(attempt)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-waitDuration(delay):
			}
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Streaming failed, fall back to queued
	c.logger.Warn("streaming ingestion failed, falling back to queued",
		"lastError", lastErr,
	)
	return c.invokeQueued(ctx, database, table, source, props)
}

func (c *ManagedStreamingIngestClient) invokeQueued(ctx context.Context, database, table string, source ingestoptions.IngestionSource, props *ingestoptions.IngestRequestProperties) (*ingestoptions.ExtendedIngestResponse, error) {
	return c.queuedClient.Ingest(ctx, database, table, source, props)
}

// GetOperationSummary delegates to the appropriate client based on IngestKind.
func (c *ManagedStreamingIngestClient) GetOperationSummary(ctx context.Context, op *IngestionOperation) (*OperationStatus, error) {
	if err := validateIngestionOperation(op); err != nil {
		return nil, err
	}
	if op.IngestKind == ingestoptions.IngestKindStreaming {
		c.logger.Warn("GetOperationSummary called for streaming operation; streaming operations are not tracked")
		return &OperationStatus{}, nil
	}
	return c.queuedClient.GetOperationSummary(ctx, op)
}

// GetOperationDetails delegates to the appropriate client based on IngestKind.
func (c *ManagedStreamingIngestClient) GetOperationDetails(ctx context.Context, op *IngestionOperation) (*StatusResponse, error) {
	if err := validateIngestionOperation(op); err != nil {
		return nil, err
	}
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

	statusCode := extractStatusCode(err)
	switch statusCode {
	case http.StatusRequestEntityTooLarge:
		return policy.ErrorCategoryRequestPropertiesPreventStreaming
	case http.StatusTooManyRequests:
		return policy.ErrorCategoryThrottled
	}

	msg := strings.ToLower(err.Error())

	// Check for streaming ingestion off
	if strings.Contains(msg, "streaming") &&
		(strings.Contains(msg, "disabled") || strings.Contains(msg, "not enabled") || strings.Contains(msg, "off")) {
		return policy.ErrorCategoryStreamingIngestionOff
	}

	// Check for table configuration issues
	if strings.Contains(msg, "update policy") || strings.Contains(msg, "schema") || strings.Contains(msg, "incompatible") {
		return policy.ErrorCategoryTableConfigurationPreventsStreaming
	}

	// Check for payload too large
	if strings.Contains(msg, "too large") || strings.Contains(msg, "exceeds") ||
		strings.Contains(msg, "maximum allowed size") ||
		strings.Contains(msg, "kustorequestpayloadtoolargeexception") {
		return policy.ErrorCategoryRequestPropertiesPreventStreaming
	}

	if strings.Contains(msg, "kustorequestthrottledexception") {
		return policy.ErrorCategoryThrottled
	}

	return policy.ErrorCategoryOther
}

// Prefer HTTP status while retaining failure-code support for caller-created errors.
func extractStatusCode(err error) int {
	var fallback int
	for err != nil {
		var ingestErr *ingestoptions.IngestError
		switch e := err.(type) {
		case *ingestoptions.IngestRequestError:
			ingestErr = &e.IngestError
		case *ingestoptions.IngestServiceError:
			ingestErr = &e.IngestError
		case *ingestoptions.IngestClientError:
			ingestErr = &e.IngestError
		case *ingestoptions.IngestError:
			ingestErr = e
		}
		if ingestErr != nil {
			if ingestErr.HTTPStatusCode != 0 {
				return ingestErr.HTTPStatusCode
			}
			if fallback == 0 {
				fallback = ingestErr.FailureCode
			}
		}
		err = errors.Unwrap(err)
	}
	return fallback
}

// isPermError checks if an error is permanent, unwrapping wrapped errors.
func isPermError(err error) bool {
	for err != nil {
		switch e := err.(type) {
		case *ingestoptions.IngestRequestError:
			return e.IsPermanent
		case *ingestoptions.IngestServiceError:
			return e.IsPermanent
		case *ingestoptions.IngestClientError:
			return e.IsPermanent
		case *ingestoptions.IngestError:
			return e.IsPermanent
		}
		err = errors.Unwrap(err)
	}
	return false
}

// waitDuration returns a channel that receives after the given duration.
func waitDuration(d time.Duration) <-chan time.Time {
	return time.After(d)
}
