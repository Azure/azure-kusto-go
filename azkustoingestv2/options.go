// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package azkustoingestv2

import (
	"net/http"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/config"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/endpoints"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/httpclient"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/policy"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/upload"
)

// ClientOption configures an ingest client.
type ClientOption func(*clientConfig)

type clientConfig struct {
	// Common options
	DMURL                     string
	EngineURL                 string
	WithoutEndpointCorrection bool

	// HTTP and identity
	HTTPClient            *http.Client
	TokenProvider         httpclient.TokenProvider
	ClientDetails         *ingestoptions.ClientDetails
	UploadTokenCredential azcore.TokenCredential
	SkipSecurityChecks    bool

	// S2S / Fabric Private Link
	S2STokenProvider         httpclient.S2STokenProvider
	S2SFabricPLAccessContext string

	// Uploader options
	UploadMethod    ingestoptions.UploadMethod
	MaxConcurrency  int
	MaxDataSize     int64
	RetryPolicy     ingestoptions.IngestRetryPolicy
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

// WithoutEndpointCorrection uses the supplied Kusto URLs without adding or removing "ingest-".
// Trusted-endpoint validation remains enabled.
func WithoutEndpointCorrection() ClientOption {
	return func(c *clientConfig) { c.WithoutEndpointCorrection = true }
}

// WithTokenProvider sets the token provider for authentication.
func WithTokenProvider(p httpclient.TokenProvider) ClientOption {
	return func(c *clientConfig) { c.TokenProvider = p }
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

// WithConfigRefreshInterval sets the configuration cache refresh interval.
func WithConfigRefreshInterval(d time.Duration) ClientOption {
	return func(c *clientConfig) { c.ConfigRefreshInterval = d }
}

// WithHTTPClient sets a custom HTTP client for API and upload calls.
func WithHTTPClient(client *http.Client) ClientOption {
	return func(c *clientConfig) { c.HTTPClient = client }
}

// WithSkipSecurityChecks disables trusted-endpoint validation for custom endpoints.
// Only enable this for endpoints you trust.
func WithSkipSecurityChecks(skip bool) ClientOption {
	return func(c *clientConfig) { c.SkipSecurityChecks = skip }
}

// WithUploadTokenCredential sets the storage-scoped credential for OneLake uploads.
// It is separate from WithTokenProvider, which authenticates Kusto API calls.
func WithUploadTokenCredential(credential azcore.TokenCredential) ClientOption {
	return func(c *clientConfig) { c.UploadTokenCredential = credential }
}

// WithClientDetails sets the client identity details for tracing.
func WithClientDetails(details *ingestoptions.ClientDetails) ClientOption {
	return func(c *clientConfig) { c.ClientDetails = details }
}

// WithS2STokenProvider sets the S2S token provider for Fabric Private Link.
func WithS2STokenProvider(p httpclient.S2STokenProvider) ClientOption {
	return func(c *clientConfig) { c.S2STokenProvider = p }
}

// WithS2SFabricAccessContext sets the Fabric Private Link access context.
func WithS2SFabricAccessContext(ctx string) ClientOption {
	return func(c *clientConfig) { c.S2SFabricPLAccessContext = ctx }
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

// buildBaseClient creates a shared BaseClient from the config.
func buildBaseClient(cfg *clientConfig) (*httpclient.BaseClient, error) {
	if !cfg.WithoutEndpointCorrection {
		cfg.DMURL = endpoints.GetIngestionEndpoint(cfg.DMURL)
		cfg.EngineURL = endpoints.GetQueryEndpoint(cfg.EngineURL)
	}
	trusted := endpoints.DefaultKustoTrustedEndpoints()
	for _, endpoint := range []string{cfg.DMURL, cfg.EngineURL} {
		if endpoint != "" {
			if err := trusted.ValidateEndpointURL(endpoint, cfg.SkipSecurityChecks); err != nil {
				return nil, err
			}
		}
	}
	opts := []httpclient.Option{httpclient.WithSkipSecurityChecks(cfg.SkipSecurityChecks)}
	if cfg.HTTPClient != nil {
		opts = append(opts, httpclient.WithHTTPClient(cfg.HTTPClient))
	}
	if cfg.S2STokenProvider != nil {
		opts = append(opts, httpclient.WithS2STokenProvider(cfg.S2STokenProvider))
	}
	if cfg.S2SFabricPLAccessContext != "" {
		opts = append(opts, httpclient.WithS2SFabricAccessContext(cfg.S2SFabricPLAccessContext))
	}
	return httpclient.NewBaseClient(cfg.TokenProvider, cfg.ClientDetails, opts...), nil
}

// NewQueuedClient creates a new QueuedIngestClient with the given options.
// The dmURL is required: it's the Data Management endpoint.
// An Engine URL is corrected to its DM endpoint unless WithoutEndpointCorrection is set.
func NewQueuedClient(dmURL string, opts ...ClientOption) (*QueuedIngestClient, error) {
	cfg := defaultClientConfig()
	cfg.DMURL = dmURL
	for _, opt := range opts {
		opt(cfg)
	}

	baseClient, err := buildBaseClient(cfg)
	if err != nil {
		return nil, err
	}
	apiClient := NewAPIClient(cfg.DMURL, cfg.EngineURL, baseClient)

	configClient := config.NewConfigurationClient(cfg.DMURL, baseClient)
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
		upload.WithUploaderTokenCredential(cfg.UploadTokenCredential),
		upload.WithUploaderHTTPClient(cfg.HTTPClient),
		upload.WithUploaderFabricPrivateLink(cfg.S2STokenProvider, cfg.S2SFabricPLAccessContext),
	)
	if err != nil {
		configCache.Close()
		return nil, err
	}

	client := newQueuedIngestClient(apiClient, configCache, uploader, true)
	client.ownsConfigCache = true
	return client, nil
}

// NewStreamingClient creates a new StreamingIngestClient with the given options.
// The engineURL is required: it's the Kusto Engine endpoint.
// A DM URL is corrected to its Engine endpoint unless WithoutEndpointCorrection is set.
func NewStreamingClient(engineURL string, opts ...ClientOption) (*StreamingIngestClient, error) {
	cfg := defaultClientConfig()
	cfg.EngineURL = engineURL
	for _, opt := range opts {
		opt(cfg)
	}

	baseClient, err := buildBaseClient(cfg)
	if err != nil {
		return nil, err
	}
	apiClient := NewAPIClient(cfg.DMURL, cfg.EngineURL, baseClient)
	return NewStreamingIngestClient(apiClient), nil
}

// NewManagedStreamingClient creates a new ManagedStreamingIngestClient with the given options.
// Both dmURL (for queued fallback) and engineURL (for streaming) are required.
// Each URL is corrected to its respective endpoint role unless WithoutEndpointCorrection is set.
func NewManagedStreamingClient(dmURL, engineURL string, opts ...ClientOption) (*ManagedStreamingIngestClient, error) {
	cfg := defaultClientConfig()
	cfg.DMURL = dmURL
	cfg.EngineURL = engineURL
	for _, opt := range opts {
		opt(cfg)
	}

	baseClient, err := buildBaseClient(cfg)
	if err != nil {
		return nil, err
	}
	apiClient := NewAPIClient(cfg.DMURL, cfg.EngineURL, baseClient)

	configClient := config.NewConfigurationClient(cfg.DMURL, baseClient)
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
		upload.WithUploaderTokenCredential(cfg.UploadTokenCredential),
		upload.WithUploaderHTTPClient(cfg.HTTPClient),
		upload.WithUploaderFabricPrivateLink(cfg.S2STokenProvider, cfg.S2SFabricPLAccessContext),
	)
	if err != nil {
		configCache.Close()
		return nil, err
	}

	queuedClient := newQueuedIngestClient(apiClient, configCache, uploader, true)
	queuedClient.ownsConfigCache = true
	streamingClient := NewStreamingIngestClient(apiClient)

	return newManagedStreamingIngestClient(streamingClient, queuedClient, cfg.ManagedStreamingPolicy), nil
}
