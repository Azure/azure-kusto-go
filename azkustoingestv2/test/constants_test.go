// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package test

import (
	"testing"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

func TestConstants(t *testing.T) {
	// Verify key constant values are reasonable
	if ingestoptions.StreamingMaxRequestBodySize <= 0 {
		t.Error("StreamingMaxRequestBodySize should be positive")
	}
	if ingestoptions.UploadContainerMaxDataSizeBytes <= 0 {
		t.Error("UploadContainerMaxDataSizeBytes should be positive")
	}
	if ingestoptions.UploadContainerMaxConcurrency <= 0 {
		t.Error("UploadContainerMaxConcurrency should be positive")
	}
	if ingestoptions.MaxBlobsPerBatch <= 0 {
		t.Error("MaxBlobsPerBatch should be positive")
	}
	if ingestoptions.IngestRetryPolicyDefaultTotalRetries < 0 {
		t.Error("IngestRetryPolicyDefaultTotalRetries should be non-negative")
	}
	if ingestoptions.DefaultConfigurationRefreshInterval <= 0 {
		t.Error("DefaultConfigurationRefreshInterval should be positive")
	}
}
