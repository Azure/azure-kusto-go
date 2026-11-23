// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/Azure/azure-kusto-go/azkustoingestv2"
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

func newQueuedTestServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/v1/rest/ingestion/configuration" {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(&config.ConfigurationResponse{
				IngestionSettings: &config.IngestionSettings{
					MaxBlobsPerBatch: ingestoptions.MaxBlobsPerBatch,
				},
			}); err != nil {
				t.Errorf("writing configuration response: %v", err)
			}
			return
		}
		handler(w, r)
	}))
}

func newTestQueuedClient(t *testing.T, dmURL, token string) *azkustoingestv2.QueuedIngestClient {
	t.Helper()
	client, err := azkustoingestv2.NewQueuedClient(dmURL,
		azkustoingestv2.WithTokenProvider(func(context.Context) (string, error) { return token, nil }),
		azkustoingestv2.WithClientDetails(ingestoptions.NewClientDetails("test-app", "test-user", "1.0.0")),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("closing queued client: %v", err)
		}
	})
	return client
}
