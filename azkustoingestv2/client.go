// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package azkustoingestv2

import (
	"context"
	"io"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

// IngestClient defines the interface for ingesting data into Kusto.
//
// Ingestion can be done from:
//   - A local file (FileSource)
//   - A stream (StreamSource)
//   - A blob (BlobSource)
//
// To track the result, set IngestRequestProperties.EnableTracking to true,
// then use GetOperationSummary and GetOperationDetails.
type IngestClient interface {
	io.Closer

	// Ingest ingests data from the specified source into the given database and table.
	Ingest(ctx context.Context, database, table string, source ingestoptions.IngestionSource, props *ingestoptions.IngestRequestProperties) (*ingestoptions.ExtendedIngestResponse, error)

	// GetOperationSummary returns a summary of the ingestion operation.
	// Requires EnableTracking to be true in IngestRequestProperties.
	GetOperationSummary(ctx context.Context, op *IngestionOperation) (*OperationStatus, error)

	// GetOperationDetails returns detailed status of the ingestion operation.
	// Requires EnableTracking to be true in IngestRequestProperties.
	GetOperationDetails(ctx context.Context, op *IngestionOperation) (*StatusResponse, error)
}

// MultiIngestClient extends IngestClient with multi-source ingestion.
type MultiIngestClient interface {
	IngestClient

	// IngestBlobs ingests data from multiple blob sources.
	IngestBlobs(ctx context.Context, database, table string, sources []*ingestoptions.BlobSource, props *ingestoptions.IngestRequestProperties) (*ingestoptions.ExtendedIngestResponse, error)

	// MaxSourcesPerMultiIngest returns the maximum number of sources per multi-ingest call.
	MaxSourcesPerMultiIngest(ctx context.Context) (int, error)
}

// IngestionOperation represents a tracked ingestion operation.
type IngestionOperation struct {
	Database    string
	Table       string
	OperationID string
	IngestKind  ingestoptions.IngestKind
}

// OperationStatus contains summary status counts for an ingestion operation.
type OperationStatus struct {
	Succeeded  int64 `json:"succeeded"`
	Failed     int64 `json:"failed"`
	InProgress int64 `json:"inProgress"`
	Canceled   int64 `json:"canceled"`
}

// StatusResponse contains detailed status information for an ingestion operation.
type StatusResponse struct {
	Status  *OperationStatus `json:"status,omitempty"`
	Details []BlobStatus     `json:"details,omitempty"`
}

// BlobStatus contains the status of a single blob in an ingestion operation.
type BlobStatus struct {
	BlobPath  string `json:"blobPath,omitempty"`
	Status    string `json:"status,omitempty"`
	SourceID  string `json:"sourceId,omitempty"`
	ErrorCode string `json:"errorCode,omitempty"`
	Message   string `json:"message,omitempty"`
}
