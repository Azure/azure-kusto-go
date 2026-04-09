// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package azkustoingestv2

import (
	"net/http"
	"time"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/config"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/policy"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/upload"
)

// ClientOption configures an ingest client.
type ClientOption func(*clientConfig)

type clientConfig struct {
	// Common options
	DMURL     string
	EngineURL string

	// HTTP and identity
	HTTPClient    *http.Client
	ClientDetails *ingestoptions.ClientDetails

	// Uploader options
	UploadMethod   ingestoptions.UploadMethod
	MaxConcurrency int
	MaxDataSize    int64
	RetryPolicy    ingestoptions.IngestRetryPolicy
	IgnoreSizeLimit bool

	// Managed streaming options
	ManagedStreamingPolicy policy.ManagedStreamingPolicy

	// Config cache
	ConfigRefreshInterval time.Duration
}

// WithDMURL sets the Data Management endpoint URL.
func WithDMURL(url string) ClientOption {
	return func(c *clientConfig) { c.DMURL = url }
}

// WithEngineURL sets the Engine endpoint URL.
func WithEngineURL(url string) ClientOption {
	return func(c *clientConfig) { c.EngineURL = url }
}

// WithUploadMethod sets the upload method (Storage, Lake, or Default).
func WithUploadMethod(m ingestoptions.UploadMethod) ClientOption {
	return func(c *clientConfig) { c.UploadMethod = m }
}

// WithMaxConcurrency sets the maximum concurrency for parallel uploads.
func WithMaxConcurrency(n int) ClientOption {
	return func(c *clientConfig) { c.MaxConcurrency = n }
}

// WithMaxDataSize sets the maximum data size for uploads.
func WithMaxDataSize(n int64) ClientOption {
	return func(c *clientConfig) { c.MaxDataSize = n }
}

// WithRetryPolicy sets the retry policy for ingestion operations.
func WithRetryPolicy(p ingestoptions.IngestRetryPolicy) ClientOption {
	return func(c *clientConfig) { c.RetryPolicy = p }
}

// WithIgnoreSizeLimit sets whether to ignore the size limit for uploads.
func WithIgnoreSizeLimit(ignore bool) ClientOption {
	return func(c *clientConfig) { c.IgnoreSizeLimit = ignore }
}

// WithManagedStreamingPolicy sets the managed streaming policy.
func WithManagedStreamingPolicy(p policy.ManagedStreamingPolicy) ClientOption {
	return func(c *clientConfig) { c.ManagedStreamingPolicy = p }
}

// WithConfigRefreshInterval sets the configuration cache refresh interval.
func WithConfigRefreshInterval(d time.Duration) ClientOption {
	return func(c *clientConfig) { c.ConfigRefreshInterval = d }
}

// WithHTTPClient sets a custom HTTP client for API calls.
func WithHTTPClient(client *http.Client) ClientOption {
	return func(c *clientConfig) { c.HTTPClient = client }
}

// WithClientDetails sets the client identity details for tracing.
func WithClientDetails(details *ingestoptions.ClientDetails) ClientOption {
	return func(c *clientConfig) { c.ClientDetails = details }
}

func defaultClientConfig() *clientConfig {
	return &clientConfig{
		UploadMethod:          ingestoptions.UploadMethodDefault,
		MaxConcurrency:        ingestoptions.UploadContainerMaxConcurrency,
		MaxDataSize:           ingestoptions.UploadContainerMaxDataSizeBytes,
		RetryPolicy:           ingestoptions.DefaultSimpleRetryPolicy(),
		ConfigRefreshInterval: ingestoptions.DefaultConfigurationRefreshInterval,
	}
}

// NewQueuedClient creates a new QueuedIngestClient with the given options.
// The dmURL is required: it's the Data Management endpoint.
func NewQueuedClient(dmURL string, opts ...ClientOption) (*QueuedIngestClient, error) {
	cfg := defaultClientConfig()
	cfg.DMURL = dmURL
	for _, opt := range opts {
		opt(cfg)
	}

	apiClient := NewAPIClient(cfg.DMURL, cfg.EngineURL)

	configClient := config.NewConfigurationClient(cfg.DMURL, cfg.HTTPClient, cfg.ClientDetails)
	configCache := config.NewDefaultConfigurationCache(configClient,
		config.WithCacheRefreshInterval(cfg.ConfigRefreshInterval),
	)

	uploader, err := upload.NewManagedUploader(
		upload.WithUploaderConfigCache(configCache),
		upload.WithUploaderMaxConcurrency(cfg.MaxConcurrency),
		upload.WithUploaderMaxDataSize(cfg.MaxDataSize),
		upload.WithUploaderUploadMethod(cfg.UploadMethod),
		upload.WithUploaderRetryPolicy(cfg.RetryPolicy),
		upload.WithUploaderIgnoreSizeLimit(cfg.IgnoreSizeLimit),
	)
	if err != nil {
		return nil, err
	}

	return NewQueuedIngestClient(apiClient, configCache, uploader, true), nil
}

// NewStreamingClient creates a new StreamingIngestClient with the given options.
// The engineURL is required: it's the Kusto Engine endpoint.
func NewStreamingClient(engineURL string, opts ...ClientOption) (*StreamingIngestClient, error) {
	cfg := defaultClientConfig()
	cfg.EngineURL = engineURL
	for _, opt := range opts {
		opt(cfg)
	}

	apiClient := NewAPIClient(cfg.DMURL, cfg.EngineURL)
	return NewStreamingIngestClient(apiClient), nil
}

// NewManagedStreamingClient creates a new ManagedStreamingIngestClient with the given options.
// Both dmURL (for queued fallback) and engineURL (for streaming) are required.
func NewManagedStreamingClient(dmURL, engineURL string, opts ...ClientOption) (*ManagedStreamingIngestClient, error) {
	cfg := defaultClientConfig()
	cfg.DMURL = dmURL
	cfg.EngineURL = engineURL
	for _, opt := range opts {
		opt(cfg)
	}

	apiClient := NewAPIClient(cfg.DMURL, cfg.EngineURL)

	configClient := config.NewConfigurationClient(cfg.DMURL, cfg.HTTPClient, cfg.ClientDetails)
	configCache := config.NewDefaultConfigurationCache(configClient,
		config.WithCacheRefreshInterval(cfg.ConfigRefreshInterval),
	)

	uploader, err := upload.NewManagedUploader(
		upload.WithUploaderConfigCache(configCache),
		upload.WithUploaderMaxConcurrency(cfg.MaxConcurrency),
		upload.WithUploaderMaxDataSize(cfg.MaxDataSize),
		upload.WithUploaderUploadMethod(cfg.UploadMethod),
		upload.WithUploaderRetryPolicy(cfg.RetryPolicy),
		upload.WithUploaderIgnoreSizeLimit(cfg.IgnoreSizeLimit),
	)
	if err != nil {
		return nil, err
	}

	queuedClient := NewQueuedIngestClient(apiClient, configCache, uploader, true)
	streamingClient := NewStreamingIngestClient(apiClient)

	p := cfg.ManagedStreamingPolicy
	if p == nil {
		p = policy.NewDefaultManagedStreamingPolicy()
	}

	return NewManagedStreamingIngestClient(streamingClient, queuedClient, p), nil
}
