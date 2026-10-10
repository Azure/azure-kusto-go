// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package config

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/httpclient"
)

// ConfigurationClient fetches ingestion configuration from the DM endpoint.
type ConfigurationClient struct {
	dmURL      string
	baseClient *httpclient.BaseClient
}

// NewConfigurationClient creates a new ConfigurationClient.
func NewConfigurationClient(dmURL string, baseClient *httpclient.BaseClient) *ConfigurationClient {
	return &ConfigurationClient{
		dmURL:      dmURL,
		baseClient: baseClient,
	}
}

// FetchConfiguration fetches the ingestion configuration from the DM endpoint.
func (c *ConfigurationClient) FetchConfiguration(ctx context.Context) (*ConfigurationResponse, error) {
	url := fmt.Sprintf("%s/v1/rest/ingestion/configuration", c.dmURL)

	resp, err := c.baseClient.Do(ctx, http.MethodGet, url, nil, "application/json")
	if err != nil {
		return nil, fmt.Errorf("failed to fetch configuration: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusNotFound {
			return nil, fmt.Errorf("configuration endpoint not found (404): DM may not support v2 configuration")
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("configuration request failed with status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read configuration response: %w", err)
	}

	return ParseConfigurationResponse(body)
}
