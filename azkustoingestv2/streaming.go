// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package azkustoingestv2

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"

	"github.com/google/uuid"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/validation"
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
// Inline ingestion mappings are not supported by direct streaming.
func (c *StreamingIngestClient) Ingest(ctx context.Context, database, table string, source ingestoptions.IngestionSource, props *ingestoptions.IngestRequestProperties) (*ingestoptions.ExtendedIngestResponse, error) {
	defer closeIngestionSource(source, c.logger)
	if props != nil && len(props.IngestionMapping) > 0 {
		return nil, ingestoptions.NewIngestClientError("inline ingestion mappings are not supported by the streaming client", nil, true)
	}
	return c.ingest(ctx, database, table, source, props)
}

// ingest is shared with managed ingestion, which retains inline mappings for queued fallback.
func (c *StreamingIngestClient) ingest(ctx context.Context, database, table string, source ingestoptions.IngestionSource, props *ingestoptions.IngestRequestProperties) (*ingestoptions.ExtendedIngestResponse, error) {
	if database == "" {
		return nil, ingestoptions.NewIngestRequestError("database cannot be empty", nil, true)
	}
	if table == "" {
		return nil, ingestoptions.NewIngestRequestError("table cannot be empty", nil, true)
	}
	if err := validateIngestionSource(source); err != nil {
		return nil, err
	}

	effectiveProps := normalizeIngestProperties(source, props)

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
		blobBody, _ := json.Marshal(map[string]string{"SourceUri": s.BlobPath()})
		_, err := c.apiClient.PostStreamingIngest(
			ctx, database, table,
			bytes.NewReader(blobBody), "application/json",
			format, mappingName,
			s.BlobPath(), s.CompressionType(),
		)
		if err != nil {
			return nil, fmt.Errorf("streaming ingest from blob failed: %w", err)
		}

	case ingestoptions.LocalSource:
		c.logger.Info("streaming ingestion from local source",
			"name", s.Name(),
			"database", database,
			"table", table,
		)
		data, compression, err := c.prepareSourceStream(s, effectiveProps.Format)
		if err != nil {
			return nil, err
		}
		defer data.Close()
		_, err = c.apiClient.PostStreamingIngest(
			ctx, database, table,
			data, "application/octet-stream",
			format, mappingName,
			"", compression,
		)
		if err != nil {
			return nil, fmt.Errorf("streaming ingest from local source failed: %w", err)
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

// prepareSourceStream borrows local data and compresses it incrementally when eligible.
func (c *StreamingIngestClient) prepareSourceStream(source ingestoptions.LocalSource, format ingestoptions.DataFormat) (io.ReadCloser, ingestoptions.CompressionType, error) {
	compression := source.CompressionType()
	stream, err := source.Data()
	if err != nil {
		return nil, compression, ingestoptions.NewIngestClientError("failed to get source data", err, true)
	}
	if validation.IsNil(stream) {
		return nil, compression, ingestoptions.NewIngestClientError("source data cannot be nil", nil, true)
	}
	if !source.ShouldCompress() || format.IsBinaryFormat() {
		return &streamingSourceReader{source: stream}, compression, nil
	}
	return newStreamingGzipReader(stream), ingestoptions.CompressionGZip, nil
}

// GetOperationSummary returns an empty status — streaming doesn't support tracking.
func (c *StreamingIngestClient) GetOperationSummary(ctx context.Context, op *IngestionOperation) (*OperationStatus, error) {
	if err := validateIngestionOperation(op); err != nil {
		return nil, err
	}
	c.logger.Warn("GetOperationSummary called for streaming ingestion; streaming operations are not tracked")
	return &OperationStatus{}, nil
}

// GetOperationDetails returns an empty response — streaming doesn't support tracking.
func (c *StreamingIngestClient) GetOperationDetails(ctx context.Context, op *IngestionOperation) (*StatusResponse, error) {
	if err := validateIngestionOperation(op); err != nil {
		return nil, err
	}
	c.logger.Warn("GetOperationDetails called for streaming ingestion; streaming operations are not tracked")
	return &StatusResponse{Status: &OperationStatus{}}, nil
}

// Close closes the streaming client.
func (c *StreamingIngestClient) Close() error {
	return nil
}
