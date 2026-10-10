// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/config"
)

// createTempCSVFile creates a temp CSV file and returns its path.
// The file is automatically cleaned up when the test completes.
func createTempCSVFile(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "test-*.csv")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	if _, err := f.WriteString(content); err != nil {
		f.Close()
		t.Fatalf("failed to write temp file: %v", err)
	}
	f.Close()
	return f.Name()
}

// mustBlobSource creates a BlobSource and fails the test if creation returns an error.
func mustBlobSource(t *testing.T, blobPath string, format ingestoptions.DataFormat, opts ...ingestoptions.BlobSourceOption) *ingestoptions.BlobSource {
	t.Helper()
	bs, err := ingestoptions.NewBlobSource(blobPath, format, opts...)
	if err != nil {
		t.Fatalf("NewBlobSource(%q) failed: %v", blobPath, err)
	}
	return bs
}

// fakeCache implements config.ConfigurationCache for tests.
type fakeCache struct {
	maxBlobs int
}

func (f *fakeCache) GetConfiguration(ctx context.Context) (*config.CachedConfigurationData, error) {
	resp := &config.ConfigurationResponse{
		IngestionSettings: &config.IngestionSettings{
			MaxBlobsPerBatch: f.maxBlobs,
		},
	}
	return config.NewCachedConfigurationData(resp), nil
}
func (f *fakeCache) RefreshInterval() time.Duration { return time.Hour }
func (f *fakeCache) Close()                         {}

// newDefaultFakeCache returns a fakeCache with default max blobs.
func newDefaultFakeCache() *fakeCache {
	return &fakeCache{maxBlobs: ingestoptions.MaxBlobsPerBatch}
}
