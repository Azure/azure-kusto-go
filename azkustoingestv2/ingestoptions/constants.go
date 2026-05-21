// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package ingestoptions

import "time"

// Blob upload sizing constants.
const (
	// UploadBlockSizeBytes is the size of each block to upload to Azure Blob Storage (4 MB).
	UploadBlockSizeBytes int64 = 4 * 1024 * 1024

	// UploadMaxSingleSizeBytes is the maximum size for a single upload operation to Azure Blob Storage (256 MB).
	UploadMaxSingleSizeBytes int64 = 256 * 1024 * 1024

	// StreamingMaxRequestBodySize is the maximum request body size for streaming ingestion (10 MB).
	StreamingMaxRequestBodySize int64 = 10 * 1024 * 1024

	// UploadContainerMaxDataSizeBytes is the default maximum data size for blob upload operations (4 GB).
	UploadContainerMaxDataSizeBytes int64 = 4 * 1024 * 1024 * 1024

	// UploadContainerMaxConcurrency is the default maximum concurrency for blob upload operations.
	UploadContainerMaxConcurrency = 4

	// MaxBlobsPerBatch is the number of blobs to upload in a single batch.
	MaxBlobsPerBatch = 70
)

// Kusto API constants.
const (
	// KustoAPIVersion is the Kusto API version used in HTTP requests.
	KustoAPIVersion = "2024-12-12"

	// KustoAPIRequestTimeout is the request timeout for Kusto API HTTP requests.
	KustoAPIRequestTimeout = 60 * time.Second

	// KustoAPIConnectTimeout is the connection timeout for Kusto API HTTP requests.
	KustoAPIConnectTimeout = 60 * time.Second
)

// Configuration cache defaults.
const (
	// ConfigCacheDefaultRefreshInterval is the default refresh interval for configuration cache.
	ConfigCacheDefaultRefreshInterval = 1 * time.Hour

	// DefaultConfigurationRefreshInterval is an alias for ConfigCacheDefaultRefreshInterval.
	DefaultConfigurationRefreshInterval = ConfigCacheDefaultRefreshInterval

	// ConfigCacheDefaultSkipSecurityChecks is the default value for skipSecurityChecks.
	ConfigCacheDefaultSkipSecurityChecks = false
)

// Retry policy defaults.
const (
	// IngestRetryPolicyDefaultInterval is the default interval between retries.
	IngestRetryPolicyDefaultInterval = 10 * time.Second

	// IngestRetryPolicyDefaultTotalRetries is the default total number of retries.
	IngestRetryPolicyDefaultTotalRetries = 3

	// BlobUploadTimeout is the default timeout for blob upload operations.
	BlobUploadTimeout = 1 * time.Hour
)

// IngestRetryPolicyCustomIntervals are the default retry intervals for CustomRetryPolicy.
var IngestRetryPolicyCustomIntervals = []time.Duration{1 * time.Second, 3 * time.Second, 7 * time.Second}

// Managed streaming policy defaults.
const (
	// ManagedStreamingContinueWhenUnavailableDefault controls whether the client falls back
	// to queued ingestion when streaming is unavailable. When false, the client will fail.
	ManagedStreamingContinueWhenUnavailableDefault = false

	// ManagedStreamingDataSizeFactorDefault is the factor used to determine the size threshold
	// for queued ingestion (1.0 = no adjustment).
	ManagedStreamingDataSizeFactorDefault = 1.0

	// ManagedStreamingThrottleBackoff is how long to use queued ingestion after streaming is throttled.
	ManagedStreamingThrottleBackoff = 10 * time.Second

	// ManagedStreamingResumeTime is how long to use queued ingestion after streaming becomes unavailable.
	ManagedStreamingResumeTime = 15 * time.Minute

	// ManagedStreamingRetryJitter is the maximum jitter to add to retry delays.
	ManagedStreamingRetryJitter = 1 * time.Second
)

// ManagedStreamingRetryDelays are the default retry delays for managed streaming policy.
var ManagedStreamingRetryDelays = []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second}

// Stream buffer sizes.
const (
	// StreamCompressionBufferSize is the buffer size for stream compression.
	StreamCompressionBufferSize = 64 * 1024

	// StreamPipeBufferSize is the buffer size for stream pipe operations.
	StreamPipeBufferSize = 1024 * 1024
)

// HTTP header names used in Kusto API requests.
const (
	HeaderContentType                = "Content-Type"
	HeaderMSApp                      = "x-ms-app"
	HeaderMSUser                     = "x-ms-user"
	HeaderMSClientVersion            = "x-ms-client-version"
	HeaderMSClientRequestID          = "x-ms-client-request-id"
	HeaderMSVersion                  = "x-ms-version"
	HeaderConnection                 = "Connection"
	HeaderAccept                     = "Accept"
	HeaderMSS2SActorAuthorization    = "x-ms-s2s-actor-authorization"
	HeaderMSFabricS2SAccessContext   = "x-ms-fabric-s2s-access-context"
)
