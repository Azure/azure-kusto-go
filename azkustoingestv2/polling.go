// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package azkustoingestv2

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

const (
	// DefaultPollingInterval is the delay between operation status checks.
	DefaultPollingInterval = 30 * time.Second
	// DefaultPollingTimeout is the maximum duration of a polling call.
	DefaultPollingTimeout = 5 * time.Minute
)

// OperationPoller is implemented by queued and managed streaming clients.
// Polling requires EnableTracking on the original queued ingestion request.
type OperationPoller interface {
	PollForCompletion(ctx context.Context, op *IngestionOperation, options ...PollingOption) (*StatusResponse, error)
}

type pollingConfig struct {
	interval time.Duration
	timeout  time.Duration
}

// PollingOption configures an explicit PollForCompletion call.
type PollingOption func(*pollingConfig)

// WithPollingInterval sets the delay between checks. The interval must be positive.
func WithPollingInterval(interval time.Duration) PollingOption {
	return func(c *pollingConfig) { c.interval = interval }
}

// WithPollingTimeout sets the overall timeout. An earlier context deadline takes precedence.
// The timeout must be positive.
func WithPollingTimeout(timeout time.Duration) PollingOption {
	return func(c *pollingConfig) { c.timeout = timeout }
}

// PollForCompletion checks detailed status immediately and then at the configured interval.
// It waits for a nonempty set of blob details to reach Succeeded, Failed, or Canceled.
// Failed or canceled operations return their final status with a permanent IngestError.
// Status retrieval errors stop polling without retrying or resubmitting ingestion.
// On retrieval errors, timeout, or context cancellation, the last status is returned, if any.
func (c *QueuedIngestClient) PollForCompletion(ctx context.Context, op *IngestionOperation, options ...PollingOption) (*StatusResponse, error) {
	if op == nil || op.Database == "" || op.Table == "" || op.OperationID == "" {
		return nil, ingestoptions.NewIngestClientError("polling requires a database, table, and operation ID", nil, true)
	}
	if op.IngestKind != ingestoptions.IngestKindQueued {
		return nil, ingestoptions.NewIngestClientError("only queued ingestion operations support status polling", nil, true)
	}
	cfg := pollingConfig{interval: DefaultPollingInterval, timeout: DefaultPollingTimeout}
	for _, option := range options {
		if option == nil {
			return nil, ingestoptions.NewIngestClientError("polling option cannot be nil", nil, true)
		}
		option(&cfg)
	}
	if cfg.interval <= 0 || cfg.timeout <= 0 {
		return nil, ingestoptions.NewIngestClientError("polling interval and timeout must be positive", nil, true)
	}
	ctx, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()

	var last *StatusResponse
	for {
		if err := ctx.Err(); err != nil {
			return last, fmt.Errorf("poll ingestion operation %q: %w", op.OperationID, err)
		}
		result, err := c.GetOperationDetails(ctx, op)
		if err != nil {
			return last, fmt.Errorf("poll ingestion operation %q: %w", op.OperationID, err)
		}
		last = result
		complete, unsuccessful := operationCompletion(result)
		if complete {
			if unsuccessful {
				var message strings.Builder
				fmt.Fprintf(&message, "ingestion operation %q failed or was canceled", op.OperationID)
				if result.Status != nil {
					fmt.Fprintf(&message, " (failed=%d, canceled=%d)", result.Status.Failed, result.Status.Canceled)
				}
				for _, blob := range result.Details {
					if blob.Status == "Failed" || blob.Status == "Canceled" {
						fmt.Fprintf(&message, "; sourceId=%q status=%s errorCode=%q failureStatus=%q details=%q message=%q",
							blob.SourceID, blob.Status, blob.ErrorCode, blob.FailureStatus, blob.Details, blob.Message)
					}
				}
				return result, ingestoptions.NewIngestError(message.String(), nil, true)
			}
			return result, nil
		}

		timer := time.NewTimer(cfg.interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return last, fmt.Errorf("poll ingestion operation %q: %w", op.OperationID, ctx.Err())
		case <-timer.C:
		}
	}
}

// PollForCompletion waits for a managed request that used queued ingestion.
// Operations completed through direct streaming do not support status tracking.
func (c *ManagedStreamingIngestClient) PollForCompletion(ctx context.Context, op *IngestionOperation, options ...PollingOption) (*StatusResponse, error) {
	return c.queuedClient.PollForCompletion(ctx, op, options...)
}

func operationCompletion(result *StatusResponse) (complete, unsuccessful bool) {
	if result == nil || len(result.Details) == 0 {
		return false, false
	}
	if result.Status != nil {
		if result.Status.InProgress > 0 {
			return false, false
		}
		unsuccessful = result.Status.Failed > 0 || result.Status.Canceled > 0
	}
	for _, blob := range result.Details {
		switch blob.Status {
		case "Succeeded":
		case "Failed", "Canceled":
			unsuccessful = true
		default:
			return false, false
		}
	}
	return true, unsuccessful
}
