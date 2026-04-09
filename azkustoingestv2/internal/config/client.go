// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package config

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

// ConfigurationClient fetches ingestion configuration from the DM endpoint.
type ConfigurationClient struct {
	dmURL             string
	httpClient        *http.Client
	clientDetails     *ingestoptions.ClientDetails
	skipSecurityChecks bool
	s2sTokenProvider  func(ctx context.Context) (*ingestoptions.S2SToken, error)
	s2sFabricPLAccessContext string
}

// ConfigurationClientOption configures a ConfigurationClient.
type ConfigurationClientOption func(*ConfigurationClient)

// WithConfigSkipSecurityChecks sets whether to skip security checks.
func WithConfigSkipSecurityChecks(skip bool) ConfigurationClientOption {
	return func(c *ConfigurationClient) { c.skipSecurityChecks = skip }
}

// WithConfigS2STokenProvider sets the S2S token provider for Fabric Private Link.
func WithConfigS2STokenProvider(provider func(ctx context.Context) (*ingestoptions.S2SToken, error)) ConfigurationClientOption {
	return func(c *ConfigurationClient) { c.s2sTokenProvider = provider }
}

// WithConfigS2SFabricAccessContext sets the Fabric Private Link access context.
func WithConfigS2SFabricAccessContext(accessContext string) ConfigurationClientOption {
	return func(c *ConfigurationClient) { c.s2sFabricPLAccessContext = accessContext }
}

// NewConfigurationClient creates a new ConfigurationClient.
func NewConfigurationClient(
	dmURL string,
	httpClient *http.Client,
	clientDetails *ingestoptions.ClientDetails,
	opts ...ConfigurationClientOption,
) *ConfigurationClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: ingestoptions.KustoAPIRequestTimeout}
	}
	c := &ConfigurationClient{
		dmURL:         dmURL,
		httpClient:    httpClient,
		clientDetails: clientDetails,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// FetchConfiguration fetches the ingestion configuration from the DM endpoint.
func (c *ConfigurationClient) FetchConfiguration(ctx context.Context, authToken string) (*ConfigurationResponse, error) {
	url := fmt.Sprintf("%s/v2/rest/configuration", c.dmURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create configuration request: %w", err)
	}

	// Set headers
	req.Header.Set(ingestoptions.HeaderAccept, "application/json")
	req.Header.Set(ingestoptions.HeaderMSVersion, ingestoptions.KustoAPIVersion)
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", authToken))

	if c.clientDetails != nil {
		req.Header.Set(ingestoptions.HeaderMSApp, c.clientDetails.EffectiveApplicationForTracing())
		req.Header.Set(ingestoptions.HeaderMSUser, c.clientDetails.EffectiveUserNameForTracing())
		req.Header.Set(ingestoptions.HeaderMSClientVersion, c.clientDetails.ClientHeader())
	}

	// Set S2S headers if configured
	if c.s2sTokenProvider != nil {
		token, err := c.s2sTokenProvider(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get S2S token: %w", err)
		}
		if token != nil {
			req.Header.Set(ingestoptions.HeaderMSS2SActorAuthorization, token.ToHeaderValue())
		}
	}
	if c.s2sFabricPLAccessContext != "" {
		req.Header.Set(ingestoptions.HeaderMSFabricS2SAccessContext, c.s2sFabricPLAccessContext)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch configuration: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ingestoptions.NewIngestError("configuration endpoint not found (404): DM may not support v2 configuration", nil, true)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("configuration request failed with status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read configuration response: %w", err)
	}

	return ParseConfigurationResponse(body)
}
