// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package azkustoingestv2

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"time"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/config"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/httpclient"
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

	if props != nil {
		if err := props.Validate(); err != nil {
			return nil, err
		}
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
			URL:      s.BlobPath(),
			SourceID: s.SourceID().String(),
			RawSize:  s.BlobExactSize,
		})
	}

	// Build inline mapping JSON if present
	var inlineMappingJSON string
	if len(props.IngestionMapping) > 0 {
		var err error
		inlineMappingJSON, err = ingestoptions.SerializeColumnMappings(props.IngestionMapping)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize inline mapping: %w", err)
		}
	}

	request := &ingestRequest{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Blobs:     blobs,
		Properties: ingestRequestPayload{
			Format:                    props.Format.String(),
			IngestionMappingRef:       props.IngestionMappingRef,
			IngestionMapping:          inlineMappingJSON,
			EnableTracking:            props.EnableTracking,
			FlushImmediately:          props.FlushImmediately,
			IgnoreFirstRecord:         props.IgnoreFirstRecord,
			IgnoreLastRecordIfInvalid: props.IgnoreLastRecordIfInvalid,
			IngestIfNotExists:         props.IngestIfNotExists,
			Tags:                      props.SynthesizeTags(),
			SkipBatching:              props.SkipBatching,
			DeleteAfterDownload:       props.DeleteAfterDownload,
			IgnoreSizeLimit:           props.IgnoreSizeLimit,
			CreationTime:              props.CreationTime,
			ZipPattern:                props.ZipPattern,
			ExtendSchema:              props.ExtendSchema,
			RecreateSchema:            props.RecreateSchema,
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
	URL      string `json:"url"`
	SourceID string `json:"sourceId,omitempty"`
	RawSize  int64  `json:"rawSize,omitempty"`
}

// ingestRequestPayload holds the properties portion of the ingest request.
type ingestRequestPayload struct {
	Format                    string   `json:"format,omitempty"`
	IngestionMappingRef       string   `json:"ingestionMappingReference,omitempty"`
	IngestionMapping          string   `json:"ingestionMapping,omitempty"`
	EnableTracking            bool     `json:"enableTracking,omitempty"`
	FlushImmediately          bool     `json:"flushImmediately,omitempty"`
	IgnoreFirstRecord         bool     `json:"ignoreFirstRecord,omitempty"`
	IgnoreLastRecordIfInvalid bool     `json:"ignoreLastRecordIfInvalid,omitempty"`
	IngestIfNotExists         string   `json:"ingestIfNotExists,omitempty"`
	Tags                      []string `json:"tags,omitempty"`
	SkipBatching              bool     `json:"skipBatching,omitempty"`
	DeleteAfterDownload       bool     `json:"deleteAfterDownload,omitempty"`
	IgnoreSizeLimit           bool     `json:"ignoreSizeLimit,omitempty"`
	CreationTime              string   `json:"creationTime,omitempty"`
	ZipPattern                string   `json:"zipPattern,omitempty"`
	ExtendSchema              bool     `json:"extend_schema,omitempty"`
	RecreateSchema            bool     `json:"recreate_schema,omitempty"`
}

// ingestRequest is the full queued ingest request payload.
type ingestRequest struct {
	Timestamp  string               `json:"timestamp"`
	Blobs      []ingestBlob         `json:"blobs"`
	Properties ingestRequestPayload `json:"properties"`
}

// APIClient provides HTTP methods against the Kusto DM and Engine endpoints.
type APIClient struct {
	DMURL      string
	EngineURL  string
	baseClient *httpclient.BaseClient
}

// NewAPIClient creates a new APIClient.
func NewAPIClient(dmURL, engineURL string, baseClient *httpclient.BaseClient) *APIClient {
	return &APIClient{DMURL: dmURL, EngineURL: engineURL, baseClient: baseClient}
}

// PostQueuedIngest submits a queued ingestion request.
func (a *APIClient) PostQueuedIngest(ctx context.Context, database, table string, request *ingestRequest) (*ingestoptions.IngestResponse, error) {
	apiURL := fmt.Sprintf("%s/v1/rest/ingestion/queued/%s/%s", a.DMURL, database, table)

	var result struct {
		IngestionOperationID string `json:"ingestionOperationId"`
	}
	resp, err := a.baseClient.DoJSON(ctx, "POST", apiURL, request, &result)
	if err != nil {
		return nil, fmt.Errorf("queued ingest POST failed: %w", err)
	}

	if resp.StatusCode != 200 {
		return nil, httpclient.ParseErrorResponse(resp)
	}

	return &ingestoptions.IngestResponse{
		OperationID: result.IngestionOperationID,
	}, nil
}

// PostStreamingIngest submits a streaming ingestion request.
func (a *APIClient) PostStreamingIngest(ctx context.Context, database, table string, body io.Reader, contentType string, format string, mappingName string, blobURL string, compression ingestoptions.CompressionType) (*StreamingIngestResponse, error) {
	apiURL := fmt.Sprintf("%s/v1/rest/ingest/%s/%s?streamFormat=%s", a.EngineURL, database, table, format)
	if mappingName != "" {
		apiURL += "&mappingName=" + url.QueryEscape(mappingName)
	}
	if blobURL != "" {
		apiURL += "&sourceKind=uri"
	}

	resp, err := a.baseClient.Do(ctx, "POST", apiURL, body, contentType)
	if err != nil {
		return nil, fmt.Errorf("streaming ingest POST failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, httpclient.ParseErrorResponse(resp)
	}

	return &StreamingIngestResponse{}, nil
}

// GetIngestStatus retrieves the status of an ingestion operation.
func (a *APIClient) GetIngestStatus(ctx context.Context, database, table, operationID string, details bool) (*StatusResponse, error) {
	apiURL := fmt.Sprintf("%s/v1/rest/ingestion/queued/%s/%s/%s?details=%t", a.DMURL, database, table, operationID, details)

	var result StatusResponse
	resp, err := a.baseClient.DoJSON(ctx, "GET", apiURL, nil, &result)
	if err != nil {
		return nil, fmt.Errorf("get ingest status failed: %w", err)
	}

	if resp.StatusCode != 200 {
		return nil, httpclient.ParseErrorResponse(resp)
	}

	return &result, nil
}

// StreamingIngestResponse is the response from a streaming ingestion request.
type StreamingIngestResponse struct {
	// Streaming doesn't return much — mainly used for error detection
}
