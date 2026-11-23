// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package config

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/httpclient"
)

func TestParseTimeSpan_HHmmss(t *testing.T) {
	d, err := parseTimeSpan("01:00:00")
	if err != nil {
		t.Fatal(err)
	}
	if d != 1*time.Hour {
		t.Errorf("expected 1h, got %v", d)
	}
}

func TestParseTimeSpan_DayHHmmss(t *testing.T) {
	d, err := parseTimeSpan("1.02:30:45")
	if err != nil {
		t.Fatal(err)
	}
	expected := 24*time.Hour + 2*time.Hour + 30*time.Minute + 45*time.Second
	if d != expected {
		t.Errorf("expected %v, got %v", expected, d)
	}
}

func TestParseTimeSpan_Fractional(t *testing.T) {
	d, err := parseTimeSpan("01:00:00.5000000")
	if err != nil {
		t.Fatal(err)
	}
	expected := 1*time.Hour + 500*time.Millisecond
	if d != expected {
		t.Errorf("expected %v, got %v", expected, d)
	}
}

func TestParseTimeSpan_Invalid(t *testing.T) {
	_, err := parseTimeSpan("invalid")
	if err == nil {
		t.Error("expected error for invalid format")
	}
}

func TestConfigurationClient_FetchConfiguration(t *testing.T) {
	resp := &ConfigurationResponse{
		ContainerSettings: &ContainerSettings{
			Containers: []ContainerInfo{
				{Path: "https://storage.blob.core.windows.net/container1?sas=token"},
			},
			LakeFolders: []ContainerInfo{
				{Path: "https://onelake.dfs.core.windows.net/folder1?sas=token"},
			},
			RefreshInterval:       "01:00:00",
			PreferredUploadMethod: "Storage",
		},
		IngestionSettings: &IngestionSettings{
			MaxBlobsPerBatch: 50,
			MaxDataSize:      1024 * 1024 * 1024,
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/rest/ingestion/configuration" {
			t.Errorf("unexpected path: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Method != http.MethodGet {
			t.Errorf("unexpected method: %s", r.Method)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	baseClient := httpclient.NewBaseClient(nil, nil, httpclient.WithHTTPClient(server.Client()))
	configClient := NewConfigurationClient(server.URL, baseClient)

	result, err := configClient.FetchConfiguration(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if result.ContainerSettings == nil {
		t.Fatal("expected container settings")
	}
	if len(result.ContainerSettings.Containers) != 1 {
		t.Errorf("expected 1 container, got %d", len(result.ContainerSettings.Containers))
	}
	if result.IngestionSettings.MaxBlobsPerBatch != 50 {
		t.Errorf("expected maxBlobsPerBatch=50, got %d", result.IngestionSettings.MaxBlobsPerBatch)
	}
}

func TestConfigurationClient_404(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	baseClient := httpclient.NewBaseClient(nil, nil, httpclient.WithHTTPClient(server.Client()))
	configClient := NewConfigurationClient(server.URL, baseClient)

	_, err := configClient.FetchConfiguration(context.Background())
	if err == nil {
		t.Error("expected error for 404")
	}
}

func TestDefaultConfigurationCache_RefreshUsesMin(t *testing.T) {
	// Server returns 30-minute refresh interval; default is 1h.
	// Cache should use min(30m, 1h) = 30m
	resp := &ConfigurationResponse{
		ContainerSettings: &ContainerSettings{
			RefreshInterval: "00:30:00",
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	baseClient := httpclient.NewBaseClient(nil, nil, httpclient.WithHTTPClient(server.Client()))
	configClient := NewConfigurationClient(server.URL, baseClient)
	cache := NewDefaultConfigurationCache(configClient)
	defer cache.Close()

	_, err := cache.GetConfiguration(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// Access internal cache to verify refresh interval
	cache.mu.RLock()
	refreshAfter := cache.cache.refreshAfter
	cache.mu.RUnlock()

	if refreshAfter != 30*time.Minute {
		t.Errorf("expected refresh interval 30m, got %v", refreshAfter)
	}
}
