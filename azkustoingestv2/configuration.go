// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package azkustoingestv2

import (
	"context"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/config"
)

// ConfigurationClient retrieves ingestion configuration from the DM service.
// Configuration caching is separate from this client.
type ConfigurationClient struct {
	client *config.ConfigurationClient
}

// NewConfigurationClient creates a standalone configuration client.
// An Engine URL is corrected to its DM endpoint unless WithoutEndpointCorrection is set.
// It accepts the same authentication, HTTP, tracing, and security options as ingest clients.
func NewConfigurationClient(dmURL string, opts ...ClientOption) (*ConfigurationClient, error) {
	cfg := defaultClientConfig()
	cfg.DMURL = dmURL
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.DMURL == "" {
		return nil, ingestoptions.NewIngestClientError("DM endpoint cannot be empty", nil, true)
	}
	baseClient, err := buildBaseClient(cfg)
	if err != nil {
		return nil, err
	}
	return &ConfigurationClient{client: config.NewConfigurationClient(cfg.DMURL, baseClient)}, nil
}

// GetConfigurationDetails fetches a fresh configuration response from the DM service.
// Upload target paths may contain SAS tokens and should not be logged.
func (c *ConfigurationClient) GetConfigurationDetails(ctx context.Context) (*ingestoptions.ConfigurationResponse, error) {
	return c.client.FetchConfiguration(ctx)
}
