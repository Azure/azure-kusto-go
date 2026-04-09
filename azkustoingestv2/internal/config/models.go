// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package config

import "encoding/json"

// ContainerInfo represents information about an ingestion container.
type ContainerInfo struct {
	// Path is the container URI (with SAS token for storage, or OneLake path for lake).
	Path string `json:"path,omitempty"`
}

// ContainerSettings holds the container configuration returned by the DM endpoint.
type ContainerSettings struct {
	// Containers are Azure Storage blob containers for ingestion.
	Containers []ContainerInfo `json:"containers,omitempty"`
	// LakeFolders are OneLake folders for ingestion.
	LakeFolders []ContainerInfo `json:"lakeFolders,omitempty"`
	// RefreshInterval is the recommended refresh interval (e.g., "01:00:00").
	RefreshInterval string `json:"refreshInterval,omitempty"`
	// PreferredUploadMethod is the server-preferred upload method ("Storage" or "Lake").
	PreferredUploadMethod string `json:"preferredUploadMethod,omitempty"`
}

// IngestionSettings holds ingestion settings returned by the DM endpoint.
type IngestionSettings struct {
	// MaxBlobsPerBatch is the maximum number of blobs per batch.
	MaxBlobsPerBatch int `json:"maxBlobsPerBatch,omitempty"`
	// MaxDataSize is the maximum data size in bytes.
	MaxDataSize int64 `json:"maxDataSize,omitempty"`
}

// ConfigurationResponse represents the full configuration response from the DM endpoint.
type ConfigurationResponse struct {
	// ContainerSettings contains container and lake folder information.
	ContainerSettings *ContainerSettings `json:"containerSettings,omitempty"`
	// IngestionSettings contains ingestion limits and preferences.
	IngestionSettings *IngestionSettings `json:"ingestionSettings,omitempty"`
}

// ParseConfigurationResponse parses a JSON configuration response.
func ParseConfigurationResponse(data []byte) (*ConfigurationResponse, error) {
	var resp ConfigurationResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}
