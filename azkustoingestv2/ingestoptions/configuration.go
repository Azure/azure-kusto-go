// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package ingestoptions

// ContainerInfo identifies an ingestion upload target.
// Path may contain a SAS token.
type ContainerInfo struct {
	Path string `json:"path,omitempty"`
}

// ContainerSettings describes upload targets and the service's upload preference.
type ContainerSettings struct {
	Containers  []ContainerInfo `json:"containers,omitempty"`
	LakeFolders []ContainerInfo `json:"lakeFolders,omitempty"`
	// RefreshInterval is the service's recommended refresh interval, as a .NET TimeSpan.
	RefreshInterval       string `json:"refreshInterval,omitempty"`
	PreferredUploadMethod string `json:"preferredUploadMethod,omitempty"`
}

// IngestionSettings describes ingestion limits and the service's ingestion preference.
type IngestionSettings struct {
	MaxBlobsPerBatch int `json:"maxBlobsPerBatch,omitempty"`
	// MaxDataSize is the maximum data size in bytes.
	MaxDataSize              int64  `json:"maxDataSize,omitempty"`
	PreferredIngestionMethod string `json:"preferredIngestionMethod,omitempty"`
}

// ConfigurationResponse contains the configuration returned by the DM service.
type ConfigurationResponse struct {
	ContainerSettings *ContainerSettings `json:"containerSettings,omitempty"`
	IngestionSettings *IngestionSettings `json:"ingestionSettings,omitempty"`
}
