// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package azkustoingestv2

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"time"

	"github.com/google/uuid"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/config"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/upload"
)

// QueuedIngestClient handles ingestion through the queued ingestion path.
// It uploads local sources to blob storage, then submits them to the DM service.
type QueuedIngestClient struct {
	apiClient      *APIClient
	configCache    config.ConfigurationCache
	uploader       upload.Uploader
	ownsUploader   bool
	logger         *slog.Logger
}

// NewQueuedIngestClient creates a new QueuedIngestClient.
func NewQueuedIngestClient(apiClient *APIClient, configCache config.ConfigurationCache, uploader upload.Uploader, ownsUploader bool) *QueuedIngestClient {
	return &QueuedIngestClient{
		apiClient:    apiClient,
		configCache:  configCache,
		uploader:     uploader,
		ownsUploader: ownsUploader,
		logger:       slog.Default(),
	}
}

// Ingest ingests data from a single source.
func (c *QueuedIngestClient) Ingest(ctx context.Context, database, table string, source ingestoptions.IngestionSource, props *ingestoptions.IngestRequestProperties) (*ingestoptions.ExtendedIngestResponse, error) {
	switch s := source.(type) {
	case *ingestoptions.BlobSource:
		return c.IngestBlobs(ctx, database, table, []*ingestoptions.BlobSource{s}, props)
	case ingestoptions.LocalSource:
		blobSource, err := c.uploader.Upload(ctx, s)
		if err != nil {
			return nil, fmt.Errorf("failed to upload source: %w", err)
		}
		return c.IngestBlobs(ctx, database, table, []*ingestoptions.BlobSource{blobSource}, props)
	default:
		return nil, ingestoptions.NewIngestClientError(
			fmt.Sprintf("unsupported ingestion source type: %T", source), nil, true,
		)
	}
}

// IngestBlobs ingests data from multiple blob sources.
func (c *QueuedIngestClient) IngestBlobs(ctx context.Context, database, table string, sources []*ingestoptions.BlobSource, props *ingestoptions.IngestRequestProperties) (*ingestoptions.ExtendedIngestResponse, error) {
	if len(sources) == 0 {
		return nil, ingestoptions.NewIngestClientError("sources list cannot be empty", nil, true)
	}

	maxBlobs, err := c.MaxSourcesPerMultiIngest(ctx)
	if err != nil {
		return nil, err
	}
	if len(sources) > maxBlobs {
		return nil, ingestoptions.NewIngestSizeLimitExceededError(int64(len(sources)), int64(maxBlobs))
	}

	// Validate all blobs have same format
	if len(sources) > 1 {
		firstFormat := sources[0].Format()
		for _, s := range sources[1:] {
			if s.Format() != firstFormat {
				return nil, ingestoptions.NewIngestClientError(
					"all blobs in the request must have the same format", nil, true,
				)
			}
		}
	}

	// Check for duplicate blob URLs
	seen := make(map[string]bool)
	for _, s := range sources {
		sanitized := sanitizeBlobURL(s.BlobPath())
		if seen[sanitized] {
			return nil, ingestoptions.NewIngestClientError(
				fmt.Sprintf("duplicate blob source detected: %s", sanitized), nil, true,
			)
		}
		seen[sanitized] = true
	}

	// Build ingestion request
	blobs := make([]ingestBlob, 0, len(sources))
	for _, s := range sources {
		blobs = append(blobs, ingestBlob{
			BlobPath: s.BlobPath(),
			SourceID: s.SourceID().String(),
			RawSize:  s.BlobExactSize,
		})
	}

	request := &ingestRequest{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Blobs:     blobs,
		Properties: ingestRequestPayload{
			Format:                props.Format.String(),
			IngestionMappingRef:   props.IngestionMappingRef,
			FlushImmediately:      props.FlushImmediately,
			IgnoreFirstRecord:     props.IgnoreFirstRecord,
			IngestIfNotExists:     props.IngestIfNotExists,
			Tags:                  props.Tags,
			DropByTags:            props.DropByTags,
			IngestByTags:          props.IngestByTags,
			CreationTime:          props.CreationTime,
		},
	}

	// Submit to DM
	response, err := c.apiClient.PostQueuedIngest(ctx, database, table, request)
	if err != nil {
		return nil, fmt.Errorf("queued ingest failed: %w", err)
	}

	return &ingestoptions.ExtendedIngestResponse{
		Response: *response,
		Kind:     ingestoptions.IngestKindQueued,
	}, nil
}

// MaxSourcesPerMultiIngest returns the maximum number of sources per multi-ingest call.
func (c *QueuedIngestClient) MaxSourcesPerMultiIngest(ctx context.Context) (int, error) {
	cfg, err := c.configCache.GetConfiguration(ctx)
	if err != nil {
		c.logger.Warn("failed to get max sources from configuration, using default", "error", err)
		return ingestoptions.MaxBlobsPerBatch, nil
	}
	if cfg.IngestionSettings() != nil && cfg.IngestionSettings().MaxBlobsPerBatch > 0 {
		return cfg.IngestionSettings().MaxBlobsPerBatch, nil
	}
	return ingestoptions.MaxBlobsPerBatch, nil
}

// GetOperationSummary returns a summary of the ingestion operation.
func (c *QueuedIngestClient) GetOperationSummary(ctx context.Context, op *IngestionOperation) (*OperationStatus, error) {
	resp, err := c.GetOperationDetails(ctx, op)
	if err != nil {
		return nil, err
	}
	if resp.Status != nil {
		return resp.Status, nil
	}
	return &OperationStatus{}, nil
}

// GetOperationDetails returns detailed status of the ingestion operation.
func (c *QueuedIngestClient) GetOperationDetails(ctx context.Context, op *IngestionOperation) (*StatusResponse, error) {
	return c.apiClient.GetIngestStatus(ctx, op.Database, op.Table, op.OperationID, true)
}

// Close closes the client and optionally the owned uploader.
func (c *QueuedIngestClient) Close() error {
	if c.ownsUploader && c.uploader != nil {
		return c.uploader.Close()
	}
	return nil
}

func sanitizeBlobURL(blobPath string) string {
	u, err := url.Parse(blobPath)
	if err != nil {
		return blobPath
	}
	u.RawQuery = ""
	return u.String()
}

// ingestBlob is the blob payload for the ingest request.
type ingestBlob struct {
	BlobPath string `json:"blobPath"`
	SourceID string `json:"sourceId,omitempty"`
	RawSize  int64  `json:"rawSize,omitempty"`
}

// ingestRequestPayload holds the properties portion of the ingest request.
type ingestRequestPayload struct {
	Format              string   `json:"format,omitempty"`
	IngestionMappingRef string   `json:"ingestionMappingReference,omitempty"`
	FlushImmediately    bool     `json:"flushImmediately,omitempty"`
	IgnoreFirstRecord   bool     `json:"ignoreFirstRecord,omitempty"`
	IngestIfNotExists   string   `json:"ingestIfNotExists,omitempty"`
	Tags                []string `json:"tags,omitempty"`
	DropByTags          []string `json:"dropByTags,omitempty"`
	IngestByTags        []string `json:"ingestByTags,omitempty"`
	CreationTime        string   `json:"creationTime,omitempty"`
}

// ingestRequest is the full queued ingest request payload.
type ingestRequest struct {
	Timestamp  string               `json:"timestamp"`
	Blobs      []ingestBlob         `json:"blobs"`
	Properties ingestRequestPayload `json:"properties"`
}

// APIClient provides HTTP methods against the Kusto DM and Engine endpoints.
// This is a thin wrapper intended to be implemented with actual HTTP calls.
type APIClient struct {
	DMURL     string
	EngineURL string
	// httpClient would be configured with auth, tracing headers, etc.
}

// NewAPIClient creates a new APIClient.
func NewAPIClient(dmURL, engineURL string) *APIClient {
	return &APIClient{DMURL: dmURL, EngineURL: engineURL}
}

// PostQueuedIngest submits a queued ingestion request.
func (a *APIClient) PostQueuedIngest(ctx context.Context, database, table string, request *ingestRequest) (*ingestoptions.IngestResponse, error) {
	// TODO: Implement actual HTTP POST to DM endpoint
	// POST {dmURL}/v2/rest/ingest/{database}/{table}
	_, _ = json.Marshal(request)
	return &ingestoptions.IngestResponse{
		OperationID: uuid.New().String(),
	}, nil
}

// PostStreamingIngest submits a streaming ingestion request.
func (a *APIClient) PostStreamingIngest(ctx context.Context, database, table string, data []byte, format string, mappingName string, blobURL string, compression ingestoptions.CompressionType) (*StreamingIngestResponse, error) {
	// TODO: Implement actual HTTP POST to Engine endpoint
	// POST {engineURL}/v1/rest/ingest/{database}/{table}?streamFormat={format}&mappingName={mappingName}
	return &StreamingIngestResponse{}, nil
}

// GetIngestStatus retrieves the status of an ingestion operation.
func (a *APIClient) GetIngestStatus(ctx context.Context, database, table, operationID string, details bool) (*StatusResponse, error) {
	// TODO: Implement actual HTTP GET from DM endpoint
	// GET {dmURL}/v2/rest/ingest/{database}/{table}/operations/{operationID}?details={details}
	return &StatusResponse{
		Status: &OperationStatus{},
	}, nil
}

// StreamingIngestResponse is the response from a streaming ingestion request.
type StreamingIngestResponse struct {
	// Streaming doesn't return much — mainly used for error detection
}
