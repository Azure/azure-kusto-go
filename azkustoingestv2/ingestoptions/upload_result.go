// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package ingestoptions

import "fmt"

// UploadResult represents the result of an upload operation.
type UploadResult struct {
	// BlobSource is the blob source that was created by the upload.
	BlobSource *BlobSource
	// Error is the error that occurred during the upload, if any.
	Error error
}

// IsSuccess returns true if the upload was successful.
func (r *UploadResult) IsSuccess() bool {
	return r.Error == nil && r.BlobSource != nil
}

// UploadResults represents the results of a batch upload operation.
type UploadResults struct {
	// Results are the individual upload results.
	Results []UploadResult
	// SuccessCount is the number of successful uploads.
	SuccessCount int
	// FailureCount is the number of failed uploads.
	FailureCount int
}

// BatchOperationResult represents the result of a batch ingestion operation.
type BatchOperationResult struct {
	// OperationID is the unique identifier for the operation.
	OperationID string
	// SuccessCount is the number of successful operations.
	SuccessCount int
	// FailureCount is the number of failed operations.
	FailureCount int
	// Errors contains any errors that occurred.
	Errors []error
}

// IsSuccess returns true if all operations were successful.
func (r *BatchOperationResult) IsSuccess() bool {
	return r.FailureCount == 0
}

func (r *BatchOperationResult) String() string {
	return fmt.Sprintf("BatchOperationResult{operationID=%s, success=%d, failure=%d}",
		r.OperationID, r.SuccessCount, r.FailureCount)
}

// IngestKind indicates the type of ingestion that was performed.
type IngestKind int

const (
	// IngestKindStreaming indicates streaming ingestion.
	IngestKindStreaming IngestKind = iota
	// IngestKindQueued indicates queued ingestion.
	IngestKindQueued
)

// String returns the string representation.
func (k IngestKind) String() string {
	switch k {
	case IngestKindStreaming:
		return "Streaming"
	case IngestKindQueued:
		return "Queued"
	default:
		return "Unknown"
	}
}

// IngestResponse represents the response from an ingestion operation.
type IngestResponse struct {
	// OperationID is the unique identifier for the ingestion operation.
	OperationID string
}

// ExtendedIngestResponse wraps an IngestResponse with additional metadata.
type ExtendedIngestResponse struct {
	// Response is the underlying ingestion response.
	Response IngestResponse
	// Kind indicates whether streaming or queued ingestion was used.
	Kind IngestKind
}
