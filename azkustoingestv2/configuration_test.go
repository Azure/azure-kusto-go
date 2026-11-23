// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package azkustoingestv2_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	v2 "github.com/Azure/azure-kusto-go/azkustoingestv2"
)

func TestConfigurationClientFreshDetails(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method := "Rest"
		if requests.Add(1) > 1 {
			method = "Queued"
		}
		if r.Method != http.MethodGet || r.URL.Path != "/v1/rest/ingestion/configuration" ||
			r.Header.Get("Authorization") != "Bearer test-token" {
			t.Errorf("unexpected configuration request: %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{
			"ingestionSettings":{"preferredIngestionMethod":"%s","maxBlobsPerBatch":20,"maxDataSize":1024},
			"containerSettings":{"preferredUploadMethod":"Lake","refreshInterval":"01:00:00",
				"containers":[{"path":"https://storage.example/container"}],
				"lakeFolders":[{"path":"https://lake.example/folder"}]}
		}`, method)
	}))
	defer server.Close()
	client, err := v2.NewConfigurationClient(server.URL+"/",
		v2.WithHTTPClient(server.Client()),
		v2.WithTokenProvider(func(context.Context) (string, error) { return "test-token", nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 0 {
		t.Fatal("constructing the configuration client made an HTTP request")
	}
	first, err := client.GetConfigurationDetails(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if first.IngestionSettings == nil || first.ContainerSettings == nil ||
		first.IngestionSettings.PreferredIngestionMethod != "Rest" ||
		first.ContainerSettings.PreferredUploadMethod != "Lake" ||
		first.IngestionSettings.MaxDataSize != 1024 ||
		first.ContainerSettings.RefreshInterval != "01:00:00" ||
		len(first.ContainerSettings.Containers) != 1 ||
		len(first.ContainerSettings.LakeFolders) != 1 {
		t.Fatal("configuration details were not exposed")
	}
	first.IngestionSettings.PreferredIngestionMethod = "changed"
	first.IngestionSettings.MaxBlobsPerBatch = 1
	first.ContainerSettings.PreferredUploadMethod = "changed"
	first.ContainerSettings.Containers[0].Path = "changed"
	first.ContainerSettings.LakeFolders[0].Path = "changed"
	second, err := client.GetConfigurationDetails(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if second.IngestionSettings.PreferredIngestionMethod != "Queued" ||
		second.IngestionSettings.MaxBlobsPerBatch != 20 ||
		second.ContainerSettings.PreferredUploadMethod != "Lake" ||
		second.ContainerSettings.Containers[0].Path != "https://storage.example/container" ||
		second.ContainerSettings.LakeFolders[0].Path != "https://lake.example/folder" {
		t.Fatal("configuration responses were not independent")
	}
	if requests.Load() != 2 {
		t.Fatalf("expected one HTTP request per getter call, got %d", requests.Load())
	}
}

func TestConfigurationClientErrorAndCancellation(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusNotFound, http.StatusOK} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
				_, _ = io.WriteString(w, "invalid configuration")
			}))
			defer server.Close()
			client, err := v2.NewConfigurationClient(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			if response, err := client.GetConfigurationDetails(t.Context()); err == nil || response != nil {
				t.Fatalf("configuration error returned success: response=%v err=%v", response, err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if response, err := client.GetConfigurationDetails(ctx); !errors.Is(err, context.Canceled) || response != nil {
				t.Fatalf("canceled request returned success: response=%v err=%v", response, err)
			}
		})
	}
}

func TestConfigurationClientRejectsUntrustedEndpoint(t *testing.T) {
	for _, endpoint := range []string{"", "https://untrusted.example", "not-a-url"} {
		for _, opts := range [][]v2.ClientOption{nil, {v2.WithoutEndpointCorrection()}} {
			if client, err := v2.NewConfigurationClient(endpoint, opts...); err == nil || client != nil {
				t.Fatalf("invalid endpoint %q returned success: client=%v err=%v", endpoint, client, err)
			}
		}
	}
}

func TestConfigurationClientTokenFailureAndRecovery(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("configuration request was not authenticated")
		}
		_, _ = io.WriteString(w, `{"ingestionSettings":{"preferredIngestionMethod":"Rest"}}`)
	}))
	defer server.Close()
	tokenErr := errors.New("token unavailable")
	var available atomic.Bool
	client, err := v2.NewConfigurationClient(server.URL,
		v2.WithTokenProvider(func(context.Context) (string, error) {
			if !available.Load() {
				return "", tokenErr
			}
			return "test-token", nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if response, err := client.GetConfigurationDetails(t.Context()); response != nil || !errors.Is(err, tokenErr) {
		t.Fatalf("token failure was not propagated: response=%v err=%v", response, err)
	}
	if requests.Load() != 0 {
		t.Fatal("token failure caused an unauthenticated HTTP request")
	}
	available.Store(true)
	response, err := client.GetConfigurationDetails(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if response.IngestionSettings == nil || response.IngestionSettings.PreferredIngestionMethod != "Rest" ||
		response.ContainerSettings != nil || requests.Load() != 1 {
		t.Fatal("configuration read did not recover after authentication became available")
	}
}
