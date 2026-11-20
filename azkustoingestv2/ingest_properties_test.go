// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package azkustoingestv2

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/policy"
)

func TestNormalizeIngestProperties(t *testing.T) {
	cases := []struct {
		name     string
		path     string
		file     bool
		source   ingestoptions.DataFormat
		request  ingestoptions.DataFormat
		want     ingestoptions.DataFormat
		nilProps bool
	}{
		{"request-overrides-source", "data.csv", true, ingestoptions.FormatCSV, ingestoptions.FormatJSON, ingestoptions.FormatJSON, false},
		{"source-overrides-extension", "https://storage/data.csv", false, ingestoptions.FormatJSON, ingestoptions.FormatUnknown, ingestoptions.FormatJSON, true},
		{"windows-compressed-file", `C:\data\DATA.JSON.GZ`, true, ingestoptions.FormatUnknown, ingestoptions.FormatUnknown, ingestoptions.FormatJSON, true},
		{"binary-file", "data.parquet", true, ingestoptions.FormatUnknown, ingestoptions.FormatUnknown, ingestoptions.FormatParquet, true},
		{"gzip-extension", "data.json.gzip", true, ingestoptions.FormatUnknown, ingestoptions.FormatUnknown, ingestoptions.FormatJSON, false},
		{"blob-url-query", "https://storage/DATA.JSON.zip?sig=token&filename=data.csv", false, ingestoptions.FormatUnknown, ingestoptions.FormatUnknown, ingestoptions.FormatJSON, false},
		{"unknown-extension", "https://storage/data.unknown", false, ingestoptions.FormatUnknown, ingestoptions.FormatUnknown, ingestoptions.FormatCSV, true},
		{"stream-source-format", "", false, ingestoptions.FormatJSON, ingestoptions.FormatUnknown, ingestoptions.FormatJSON, true},
		{"stream-default", "", false, ingestoptions.FormatUnknown, ingestoptions.FormatUnknown, ingestoptions.FormatCSV, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var source ingestoptions.IngestionSource
			switch {
			case tc.file:
				source = ingestoptions.NewFileSource(tc.path, tc.source)
			case tc.path != "":
				var err error
				source, err = ingestoptions.NewBlobSource(tc.path, tc.source)
				if err != nil {
					t.Fatal(err)
				}
			default:
				source = ingestoptions.NewStreamSource(nil, tc.source)
			}
			var props *ingestoptions.IngestRequestProperties
			if !tc.nilProps {
				props = &ingestoptions.IngestRequestProperties{
					Format: tc.request, IngestionMappingRef: "mapping",
				}
			}
			got := normalizeIngestProperties(source, props)
			if got.Format != tc.want {
				t.Fatalf("format = %q, want %q", got.Format, tc.want)
			}
			if props != nil && (got == props || props.Format != tc.request || got.IngestionMappingRef != "mapping") {
				t.Fatal("request properties were mutated or not preserved")
			}
			if source.Format() != tc.source {
				t.Fatal("source format was mutated")
			}
		})
	}
}

func TestStreamingUsesSourceFormatWithNilProperties(t *testing.T) {
	formats := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		formats <- r.URL.Query().Get("streamFormat")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	client, err := NewStreamingClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	source := ingestoptions.NewStreamSource(io.NopCloser(strings.NewReader(`{"value":1}`)), ingestoptions.FormatJSON)
	defer source.Close()
	if _, err := client.Ingest(context.Background(), "db", "table", source, nil); err != nil {
		t.Fatal(err)
	}
	if got := <-formats; got != "json" {
		t.Fatalf("streamFormat = %q, want json", got)
	}
}

func TestQueuedBatchResolvesUnknownFormats(t *testing.T) {
	for _, tc := range []struct {
		name    string
		second  string
		request ingestoptions.DataFormat
		wantErr bool
	}{
		{"same-inferred-format", "b.json.gz", ingestoptions.FormatUnknown, false},
		{"different-inferred-formats", "b.csv", ingestoptions.FormatUnknown, true},
		{"explicit-request-format", "b.csv", ingestoptions.FormatJSON, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			formats := make(chan string, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodGet {
					_, _ = io.WriteString(w, `{"ingestionSettings":{"maxBlobsPerBatch":70}}`)
					return
				}
				var request ingestRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
				}
				formats <- request.Properties.Format
				_, _ = io.WriteString(w, `{"ingestionOperationId":"operation"}`)
			}))
			defer server.Close()
			client, err := NewQueuedClient(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			t.Cleanup(client.configCache.Close)
			var sources []*ingestoptions.BlobSource
			for _, name := range []string{"a.json", tc.second} {
				source, err := ingestoptions.NewBlobSource("https://storage/"+name, ingestoptions.FormatUnknown)
				if err != nil {
					t.Fatal(err)
				}
				sources = append(sources, source)
			}
			var props *ingestoptions.IngestRequestProperties
			if tc.request != ingestoptions.FormatUnknown {
				props = &ingestoptions.IngestRequestProperties{Format: tc.request}
			}
			_, err = client.IngestBlobs(context.Background(), "db", "table", sources, props)
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "same format") {
					t.Fatalf("expected format mismatch, got %v", err)
				}
				if len(formats) != 0 {
					t.Fatal("submitted mismatched formats")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if got := <-formats; got != "json" {
					t.Fatalf("format = %q, want json", got)
				}
			}

		})
	}
}

func TestManagedFallbackResolvesFormatWithNilProperties(t *testing.T) {
	formats := make(chan string, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet:
			_, _ = io.WriteString(w, `{"ingestionSettings":{"maxBlobsPerBatch":70}}`)
		case strings.HasPrefix(r.URL.Path, "/v1/rest/ingest/"):
			formats <- r.URL.Query().Get("streamFormat")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `{"error":{"code":"ServiceUnavailable","message":"temporary streaming failure","@permanent":false}}`)
		default:
			var request ingestRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			formats <- request.Properties.Format
			_, _ = io.WriteString(w, `{"ingestionOperationId":"operation"}`)
		}
	}))
	defer server.Close()
	client, err := NewManagedStreamingClient(server.URL, server.URL,
		withManagedStreamingPolicy(policy.NewDefaultManagedStreamingPolicy(
			policy.WithPolicyRetryPolicy(ingestoptions.NewSimpleRetryPolicy(0, 1)),
		)))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	t.Cleanup(client.queuedClient.configCache.Close)
	source, err := ingestoptions.NewBlobSource("https://storage/data.json.gz?sig=token", ingestoptions.FormatUnknown)
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.Ingest(context.Background(), "db", "table", source, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Kind != ingestoptions.IngestKindQueued || len(formats) != 2 {
		t.Fatalf("expected streaming followed by queued ingestion, got %+v and %d requests", result, len(formats))
	}
	for range 2 {
		if got := <-formats; got != "json" {
			t.Errorf("format = %q, want json", got)
		}
	}
}
