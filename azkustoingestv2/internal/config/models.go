// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package config

import (
	"encoding/json"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

type ContainerInfo = ingestoptions.ContainerInfo
type ContainerSettings = ingestoptions.ContainerSettings
type IngestionSettings = ingestoptions.IngestionSettings
type ConfigurationResponse = ingestoptions.ConfigurationResponse

// ParseConfigurationResponse parses a JSON configuration response.
func ParseConfigurationResponse(data []byte) (*ConfigurationResponse, error) {
	var resp ConfigurationResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}
