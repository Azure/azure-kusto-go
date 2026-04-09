// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package azkustoingestv2

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/google/uuid"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

// StreamingIngestClient handles direct streaming ingestion to the Kusto engine.
//
// Streaming ingestion provides low-latency data ingestion but does not support
// operation tracking. GetOperationSummary and GetOperationDetails return empty results.
type StreamingIngestClient struct {
	apiClient *APIClient
	logger    *slog.Logger
}

// NewStreamingIngestClient creates a new StreamingIngestClient.
func NewStreamingIngestClient(apiClient *APIClient) *StreamingIngestClient {
	return &StreamingIngestClient{
		apiClient: apiClient,
		logger:    slog.Default(),
	}
}

// Ingest ingests data from the specified source via streaming ingestion.
func (c *StreamingIngestClient) Ingest(ctx context.Context, database, table string, source ingestoptions.IngestionSource, props *ingestoptions.IngestRequestProperties) (*ingestoptions.ExtendedIngestResponse, error) {
	if database == "" {
		return nil, ingestoptions.NewIngestRequestError("database cannot be empty", nil, true)
	}
	if table == "" {
		return nil, ingestoptions.NewIngestRequestError("table cannot be empty", nil, true)
	}

	effectiveProps := props
	if effectiveProps == nil {
		effectiveProps = &ingestoptions.IngestRequestProperties{}
	}

	format := effectiveProps.Format.String()
	mappingName := effectiveProps.IngestionMappingRef
	operationID := uuid.New().String()

	switch s := source.(type) {
	case *ingestoptions.BlobSource:
		c.logger.Info("streaming ingestion from BlobSource",
			"blobPath", s.BlobPath(),
			"database", database,
			"table", table,
		)
		_, err := c.apiClient.PostStreamingIngest(
			ctx, database, table,
			nil, format, mappingName,
			s.BlobPath(), s.CompressionType(),
		)
		if err != nil {
			return nil, fmt.Errorf("streaming ingest from blob failed: %w", err)
		}

	case *ingestoptions.FileSource:
		c.logger.Info("streaming ingestion from FileSource",
			"name", s.Name(),
			"database", database,
			"table", table,
		)
		data, err := c.readSourceData(s)
		if err != nil {
			return nil, err
		}
		if err := c.checkStreamingSize(data); err != nil {
			return nil, err
		}
		_, err = c.apiClient.PostStreamingIngest(
			ctx, database, table,
			data, format, mappingName,
			"", s.CompressionType(),
		)
		if err != nil {
			return nil, fmt.Errorf("streaming ingest from file failed: %w", err)
		}

	case *ingestoptions.StreamSource:
		c.logger.Info("streaming ingestion from StreamSource",
			"name", s.Name(),
			"database", database,
			"table", table,
		)
		data, err := c.readSourceData(s)
		if err != nil {
			return nil, err
		}
		if err := c.checkStreamingSize(data); err != nil {
			return nil, err
		}
		_, err = c.apiClient.PostStreamingIngest(
			ctx, database, table,
			data, format, mappingName,
			"", s.CompressionType(),
		)
		if err != nil {
			return nil, fmt.Errorf("streaming ingest from stream failed: %w", err)
		}

	default:
		return nil, ingestoptions.NewIngestClientError(
			fmt.Sprintf("unsupported source type for streaming ingestion: %T", source),
			nil, true,
		)
	}

	return &ingestoptions.ExtendedIngestResponse{
		Response: ingestoptions.IngestResponse{OperationID: operationID},
		Kind:     ingestoptions.IngestKindStreaming,
	}, nil
}

// readSourceData reads the data from a local source and returns it as bytes.
func (c *StreamingIngestClient) readSourceData(source ingestoptions.LocalSource) ([]byte, error) {
	stream, err := source.Data()
	if err != nil {
		return nil, ingestoptions.NewIngestClientError("failed to get source data", err, true)
	}
	data, err := io.ReadAll(stream)
	if err != nil {
		return nil, ingestoptions.NewIngestClientError("failed to read source data", err, false)
	}
	return data, nil
}

// checkStreamingSize verifies the data doesn't exceed the streaming size limit.
func (c *StreamingIngestClient) checkStreamingSize(data []byte) error {
	if int64(len(data)) > ingestoptions.StreamingMaxRequestBodySize {
		return ingestoptions.NewIngestSizeLimitExceededError(int64(len(data)), ingestoptions.StreamingMaxRequestBodySize)
	}
	return nil
}

// GetOperationSummary returns an empty status — streaming doesn't support tracking.
func (c *StreamingIngestClient) GetOperationSummary(ctx context.Context, op *IngestionOperation) (*OperationStatus, error) {
	c.logger.Warn("GetOperationSummary called for streaming ingestion; streaming operations are not tracked")
	return &OperationStatus{}, nil
}

// GetOperationDetails returns an empty response — streaming doesn't support tracking.
func (c *StreamingIngestClient) GetOperationDetails(ctx context.Context, op *IngestionOperation) (*StatusResponse, error) {
	c.logger.Warn("GetOperationDetails called for streaming ingestion; streaming operations are not tracked")
	return &StatusResponse{Status: &OperationStatus{}}, nil
}

// Close closes the streaming client.
func (c *StreamingIngestClient) Close() error {
	return nil
}
