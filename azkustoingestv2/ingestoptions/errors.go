// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package ingestoptions

import "fmt"

// IngestError is the base error type for ingest operations.
type IngestError struct {
	Message        string
	Cause          error
	FailureCode    int
	FailureSubCode string
	IsPermanent    bool
}

func (e *IngestError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return "something went wrong calling Kusto client library"
}

func (e *IngestError) Unwrap() error {
	return e.Cause
}

// IngestRequestError represents an error from a Kusto ingestion request.
type IngestRequestError struct {
	IngestError
	ErrorCode       string
	ErrorReason     string
	ErrorMessage    string
	DataSource      string
	DatabaseName    string
	ClientRequestID string
	ActivityID      string
}

func (e *IngestRequestError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return fmt.Sprintf("%s (%s): %s. This normally represents a permanent error, and retrying is unlikely to help.",
		e.ErrorReason, e.ErrorCode, e.ErrorMessage)
}

// IngestClientError represents an error from the ingest client itself.
type IngestClientError struct {
	IngestError
}

// IngestSizeLimitExceededError indicates that the ingestion size limit was exceeded.
type IngestSizeLimitExceededError struct {
	IngestError
	MaxAllowedSize int64
	ActualSize     int64
}

func (e *IngestSizeLimitExceededError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return fmt.Sprintf("ingestion size limit exceeded: max %d bytes, actual %d bytes",
		e.MaxAllowedSize, e.ActualSize)
}

// InvalidUploadStreamError indicates that the upload stream is invalid.
type InvalidUploadStreamError struct {
	IngestError
	FileName string
	BlobName string
}

func (e *InvalidUploadStreamError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return fmt.Sprintf("invalid upload stream for file %q", e.FileName)
}

// NewIngestError creates a new IngestError.
func NewIngestError(message string, cause error, isPermanent bool) *IngestError {
	return &IngestError{
		Message:     message,
		Cause:       cause,
		IsPermanent: isPermanent,
	}
}

// NewIngestClientError creates a new IngestClientError.
func NewIngestClientError(message string, cause error, isPermanent bool) *IngestClientError {
	return &IngestClientError{
		IngestError: IngestError{
			Message:     message,
			Cause:       cause,
			IsPermanent: isPermanent,
		},
	}
}

// NewIngestRequestError creates a new IngestRequestError.
func NewIngestRequestError(message string, cause error, isPermanent bool) *IngestRequestError {
	return &IngestRequestError{
		IngestError: IngestError{
			Message:     message,
			Cause:       cause,
			IsPermanent: isPermanent,
		},
	}
}

// NewIngestSizeLimitExceededError creates a new IngestSizeLimitExceededError.
func NewIngestSizeLimitExceededError(actualSize, maxAllowedSize int64) *IngestSizeLimitExceededError {
	return &IngestSizeLimitExceededError{
		IngestError: IngestError{
			Message:     fmt.Sprintf("ingestion size limit exceeded: max %d bytes, actual %d bytes", maxAllowedSize, actualSize),
			IsPermanent: true,
		},
		MaxAllowedSize: maxAllowedSize,
		ActualSize:     actualSize,
	}
}

// IngestServiceError represents a non-permanent service error from Kusto.
// These are transient errors that may succeed on retry.
type IngestServiceError struct {
	IngestError
}

// NewIngestServiceError creates a new IngestServiceError.
func NewIngestServiceError(message string, cause error, failureCode int, failureSubCode string) *IngestServiceError {
	return &IngestServiceError{
		IngestError: IngestError{
			Message:        message,
			Cause:          cause,
			FailureCode:    failureCode,
			FailureSubCode: failureSubCode,
			IsPermanent:    false,
		},
	}
}

// UploadErrorCode categorizes upload failure reasons.
type UploadErrorCode string

const (
	UploadErrorSourceIsNull          UploadErrorCode = "SOURCE_IS_NULL"
	UploadErrorSourceNotFound        UploadErrorCode = "SOURCE_NOT_FOUND"
	UploadErrorSourceNotReadable     UploadErrorCode = "SOURCE_NOT_READABLE"
	UploadErrorSourceIsEmpty         UploadErrorCode = "SOURCE_IS_EMPTY"
	UploadErrorSourceSizeLimitExceed UploadErrorCode = "SOURCE_SIZE_LIMIT_EXCEEDED"
	UploadErrorNoContainers          UploadErrorCode = "NO_CONTAINERS_AVAILABLE"
	UploadErrorUploadFailed          UploadErrorCode = "UPLOAD_FAILED"
	UploadErrorContainerNotFound     UploadErrorCode = "CONTAINER_NOT_FOUND"
	UploadErrorCompressionFailed     UploadErrorCode = "COMPRESSION_FAILED"
	UploadErrorUnknown               UploadErrorCode = "UNKNOWN"
)

// UploadFailedError represents a failed upload operation.
type UploadFailedError struct {
	IngestError
	ErrorCode UploadErrorCode
	FileName  string
}

func (e *UploadFailedError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return fmt.Sprintf("upload failed [%s]: %s", e.ErrorCode, e.FileName)
}

// NewUploadFailedError creates a new UploadFailedError.
func NewUploadFailedError(code UploadErrorCode, fileName string, cause error) *UploadFailedError {
	return &UploadFailedError{
		IngestError: IngestError{
			Message:     fmt.Sprintf("upload failed [%s]: %s", code, fileName),
			Cause:       cause,
			IsPermanent: true,
		},
		ErrorCode: code,
		FileName:  fileName,
	}
}
