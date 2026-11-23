// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package test

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Azure/azure-kusto-go/azkustoingestv2"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/httpclient"
)

// TestStreamingIngest_BlobSource_SendsJSON verifies that BlobSource results in a JSON body
// with {"SourceUri": "..."} and sourceKind=uri query parameter.
func TestStreamingIngest_BlobSource_SendsJSON(t *testing.T) {
	t.Parallel()

	var capturedBody []byte
	var capturedContentType string
	var capturedURL string
	var capturedContentEncoding string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedBody, _ = io.ReadAll(r.Body)
		capturedContentType = r.Header.Get("Content-Type")
		capturedURL = r.URL.RequestURI()
		capturedContentEncoding = r.Header.Get("Content-Encoding")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	baseClient := httpclient.NewBaseClient(
		func(ctx context.Context) (string, error) { return "token", nil },
		ingestoptions.NewClientDetails("app", "user", "1.0"),
	)
	apiClient := azkustoingestv2.NewAPIClient(srv.URL, srv.URL, baseClient)
	client := azkustoingestv2.NewStreamingIngestClient(apiClient)

	blob := mustBlobSource(t,
		"https://storage.blob.core.windows.net/container/data.csv.gz?sas=token",
		ingestoptions.FormatCSV,
		ingestoptions.WithBlobCompression(ingestoptions.CompressionGZip),
	)

	resp, err := client.Ingest(context.Background(), "testdb", "testtable", blob,
		&ingestoptions.IngestRequestProperties{
			Format:              ingestoptions.FormatCSV,
			IngestionMappingRef: "csvMapping",
		})
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	if resp.Kind != ingestoptions.IngestKindStreaming {
		t.Errorf("expected kind Streaming, got %v", resp.Kind)
	}

	// Verify content type is JSON
	if capturedContentType != "application/json" {
		t.Errorf("expected Content-Type 'application/json', got %q", capturedContentType)
	}
	if capturedContentEncoding != "" {
		t.Errorf("blob URI JSON must not be marked as gzip, got %q", capturedContentEncoding)
	}

	// Verify body has SourceUri
	var body map[string]string
	if err := json.Unmarshal(capturedBody, &body); err != nil {
		t.Fatalf("failed to parse body as JSON: %v (body: %s)", err, string(capturedBody))
	}
	if body["SourceUri"] != blob.BlobPath() {
		t.Errorf("expected SourceUri %q, got %q", blob.BlobPath(), body["SourceUri"])
	}

	// Verify URL has sourceKind=uri
	if !strings.Contains(capturedURL, "sourceKind=uri") {
		t.Errorf("expected sourceKind=uri in URL, got %q", capturedURL)
	}

	// Verify URL has mappingName
	if !strings.Contains(capturedURL, "mappingName=csvMapping") {
		t.Errorf("expected mappingName=csvMapping in URL, got %q", capturedURL)
	}

	// Verify URL has streamFormat=csv
	if !strings.Contains(capturedURL, "streamFormat=csv") {
		t.Errorf("expected streamFormat=csv in URL, got %q", capturedURL)
	}
}

// TestStreamingIngest_StreamSource_SendsOctetStream verifies local source sends binary body.
func TestStreamingIngest_StreamSource_SendsOctetStream(t *testing.T) {
	t.Parallel()

	var capturedBody []byte
	var capturedContentType string
	var capturedURL string
	var capturedContentEncoding string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedBody, _ = io.ReadAll(r.Body)
		capturedContentType = r.Header.Get("Content-Type")
		capturedURL = r.URL.RequestURI()
		capturedContentEncoding = r.Header.Get("Content-Encoding")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	baseClient := httpclient.NewBaseClient(
		func(ctx context.Context) (string, error) { return "token", nil },
		ingestoptions.NewClientDetails("app", "user", "1.0"),
	)
	apiClient := azkustoingestv2.NewAPIClient(srv.URL, srv.URL, baseClient)
	client := azkustoingestv2.NewStreamingIngestClient(apiClient)

	csvData := "name,age\nAlice,30\nBob,25\n"
	source := ingestoptions.NewStreamSource(
		io.NopCloser(bytes.NewReader([]byte(csvData))),
		ingestoptions.FormatCSV,
	)

	_, err := client.Ingest(context.Background(), "db", "tbl", source,
		&ingestoptions.IngestRequestProperties{Format: ingestoptions.FormatCSV})
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// Verify content type is octet-stream
	if capturedContentType != "application/octet-stream" {
		t.Errorf("expected Content-Type 'application/octet-stream', got %q", capturedContentType)
	}
	if capturedContentEncoding != "gzip" {
		t.Errorf("expected Content-Encoding gzip, got %q", capturedContentEncoding)
	}

	// Verify body matches the CSV data
	if got := readStreamingGzip(t, capturedBody); string(got) != csvData {
		t.Errorf("expected body %q, got %q", csvData, string(got))
	}

	// Verify no sourceKind in URL (not a blob)
	if strings.Contains(capturedURL, "sourceKind") {
		t.Errorf("expected no sourceKind in URL for stream source, got %q", capturedURL)
	}
}

func TestStreamingIngest_Compression(t *testing.T) {
	dontCompress := ingestoptions.DontCompress()
	cases := []struct {
		name          string
		format        ingestoptions.DataFormat
		requestFormat ingestoptions.DataFormat
		compression   ingestoptions.CompressionType
		dontCompress  bool
		encoding      string
	}{
		{"automatic-gzip", ingestoptions.FormatCSV, ingestoptions.FormatCSV, ingestoptions.CompressionNone, false, "gzip"},
		{"already-gzip", ingestoptions.FormatCSV, ingestoptions.FormatCSV, ingestoptions.CompressionGZip, false, "gzip"},
		{"already-zip", ingestoptions.FormatCSV, ingestoptions.FormatCSV, ingestoptions.CompressionZip, false, ""},
		{"binary", ingestoptions.FormatParquet, ingestoptions.FormatParquet, ingestoptions.CompressionNone, false, ""},
		{"binary-sstream", ingestoptions.FormatSStream, ingestoptions.FormatSStream, ingestoptions.CompressionNone, false, ""},
		{"binary-request-format", ingestoptions.FormatUnknown, ingestoptions.FormatParquet, ingestoptions.CompressionNone, false, ""},
		{"dont-compress", ingestoptions.FormatCSV, ingestoptions.FormatCSV, ingestoptions.CompressionNone, true, ""},
		{"dont-compress-already-gzip", ingestoptions.FormatCSV, ingestoptions.FormatCSV, ingestoptions.CompressionGZip, true, "gzip"},
	}
	for _, kind := range []string{"file", "stream", "managed-stream"} {
		for _, tc := range cases {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				payload := []byte("name,age\nAlice,30\n")
				switch tc.compression {
				case ingestoptions.CompressionGZip:
					var compressed bytes.Buffer
					writer := gzip.NewWriter(&compressed)
					if _, err := writer.Write(payload); err != nil {
						t.Fatal(err)
					}
					if err := writer.Close(); err != nil {
						t.Fatal(err)
					}
					payload = compressed.Bytes()
				case ingestoptions.CompressionZip:
					var compressed bytes.Buffer
					writer := zip.NewWriter(&compressed)
					entry, err := writer.Create("data.csv")
					if err != nil {
						t.Fatal(err)
					}
					if _, err := entry.Write(payload); err != nil {
						t.Fatal(err)
					}
					if err := writer.Close(); err != nil {
						t.Fatal(err)
					}
					payload = compressed.Bytes()
				}

				type request struct {
					headers http.Header
					body    []byte
				}
				requests := make(chan request, 1)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					body, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
					}
					requests <- request{headers: r.Header.Clone(), body: body}
					w.WriteHeader(http.StatusOK)
				}))
				defer server.Close()
				baseClient := httpclient.NewBaseClient(
					func(context.Context) (string, error) { return "token", nil }, nil,
				)
				apiClient := azkustoingestv2.NewAPIClient(server.URL, server.URL, baseClient)
				streamingClient := azkustoingestv2.NewStreamingIngestClient(apiClient)
				var client azkustoingestv2.IngestClient = streamingClient
				if kind == "managed-stream" {
					managed, err := azkustoingestv2.NewManagedStreamingClient(
						server.URL, server.URL,
						azkustoingestv2.WithTokenProvider(func(context.Context) (string, error) { return "token", nil }),
					)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = managed.Close() })
					client = managed
				}
				var source ingestoptions.IngestionSource
				if kind == "file" {
					path := filepath.Join(t.TempDir(), "data."+tc.format.String()+tc.compression.Extension())
					if err := os.WriteFile(path, payload, 0600); err != nil {
						t.Fatal(err)
					}
					var opts []ingestoptions.FileSourceOption
					if tc.dontCompress {
						opts = append(opts, dontCompress)
					}
					source = ingestoptions.NewFileSource(path, tc.format, opts...)
				} else {
					opts := []ingestoptions.StreamSourceOption{ingestoptions.WithStreamCompression(tc.compression)}
					if tc.dontCompress {
						opts = append(opts, dontCompress)
					}
					source = ingestoptions.NewStreamSource(
						io.NopCloser(bytes.NewReader(payload)), tc.format, opts...,
					)
				}
				t.Cleanup(func() {
					if err := source.Close(); err != nil {
						t.Error(err)
					}
				})
				response, err := client.Ingest(context.Background(), "db", "table", source,
					&ingestoptions.IngestRequestProperties{Format: tc.requestFormat})
				if err != nil {
					t.Fatal(err)
				}
				if response.Kind != ingestoptions.IngestKindStreaming {
					t.Fatalf("unexpected ingest kind: %v", response.Kind)
				}
				got := <-requests
				if got.headers.Get("Content-Encoding") != tc.encoding {
					t.Errorf("expected Content-Encoding %q, got %q", tc.encoding, got.headers.Get("Content-Encoding"))
				}
				if got.headers.Get("Content-Type") != "application/octet-stream" ||
					got.headers.Get("Authorization") != "Bearer token" {
					t.Errorf("standard headers were not preserved: %v", got.headers)
				}
				if tc.encoding == "gzip" && tc.compression == ingestoptions.CompressionNone {
					got.body = readStreamingGzip(t, got.body)
				}
				if !bytes.Equal(got.body, payload) {
					t.Error("payload did not match the source, or already-compressed data was compressed again")
				}
				if source.CompressionType() != tc.compression {
					t.Error("streaming changed the source compression metadata")
				}
			})
		}
	}
}

func readStreamingGzip(t *testing.T, data []byte) []byte {
	t.Helper()
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	data, err = io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestStreamingIngest_CompressedSizeLimitFromService(t *testing.T) {
	t.Parallel()
	payload := make([]byte, ingestoptions.StreamingMaxRequestBodySize)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	source := ingestoptions.NewStreamSource(io.NopCloser(bytes.NewReader(payload)), ingestoptions.FormatCSV)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		size, err := io.Copy(io.Discard, r.Body)
		if err != nil {
			t.Error(err)
		}
		if size <= ingestoptions.StreamingMaxRequestBodySize {
			t.Errorf("expected compressed data beyond the managed streaming bound, got %d bytes", size)
		}
		w.WriteHeader(http.StatusRequestEntityTooLarge)
	}))
	defer server.Close()
	client, err := azkustoingestv2.NewStreamingClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	_, err = client.Ingest(context.Background(), "db", "table", source,
		&ingestoptions.IngestRequestProperties{Format: ingestoptions.FormatCSV})
	var serviceErr *ingestoptions.IngestError
	if !errors.As(err, &serviceErr) || serviceErr.HTTPStatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected size rejection from the service, got %v", err)
	}
}

func TestStreamingIngest_LargePayloadStreams(t *testing.T) {
	t.Parallel()

	var received int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		received, err = io.Copy(io.Discard, r.Body)
		if err != nil {
			t.Error(err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	baseClient := httpclient.NewBaseClient(
		func(ctx context.Context) (string, error) { return "token", nil },
		ingestoptions.NewClientDetails("app", "user", "1.0"),
	)
	apiClient := azkustoingestv2.NewAPIClient(srv.URL, srv.URL, baseClient)
	client := azkustoingestv2.NewStreamingIngestClient(apiClient)

	bigData := make([]byte, ingestoptions.StreamingMaxRequestBodySize+1)
	source := ingestoptions.NewStreamSource(
		io.NopCloser(bytes.NewReader(bigData)),
		ingestoptions.FormatParquet,
	)

	result, err := client.Ingest(context.Background(), "db", "tbl", source,
		&ingestoptions.IngestRequestProperties{Format: ingestoptions.FormatParquet})
	if err != nil || result == nil || result.Kind != ingestoptions.IngestKindStreaming {
		t.Fatalf("direct streaming applied a local size limit: result=%v err=%v", result, err)
	}
	if received != int64(len(bigData)) {
		t.Errorf("received %d bytes, want %d", received, len(bigData))
	}
}

// TestStreamingIngest_EmptyDatabase verifies that empty database is rejected.
func TestStreamingIngest_EmptyDatabase(t *testing.T) {
	t.Parallel()
	client := azkustoingestv2.NewStreamingIngestClient(nil)
	_, err := client.Ingest(context.Background(), "", "table", nil, nil)
	if err == nil {
		t.Fatal("expected error for empty database")
	}
	if !strings.Contains(err.Error(), "database") {
		t.Errorf("expected 'database' in error, got %q", err.Error())
	}
}

// TestStreamingIngest_EmptyTable verifies that empty table is rejected.
func TestStreamingIngest_EmptyTable(t *testing.T) {
	t.Parallel()
	client := azkustoingestv2.NewStreamingIngestClient(nil)
	_, err := client.Ingest(context.Background(), "db", "", nil, nil)
	if err == nil {
		t.Fatal("expected error for empty table")
	}
	if !strings.Contains(err.Error(), "table") {
		t.Errorf("expected 'table' in error, got %q", err.Error())
	}
}

// TestStreamingIngest_ServerError_Propagated verifies server errors are properly propagated.
func TestStreamingIngest_ServerError_Propagated(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": map[string]interface{}{
				"code":       "InternalError",
				"message":    "Streaming ingestion is disabled for this cluster",
				"@permanent": true,
			},
		})
	}))
	defer srv.Close()

	baseClient := httpclient.NewBaseClient(
		func(ctx context.Context) (string, error) { return "token", nil },
		ingestoptions.NewClientDetails("app", "user", "1.0"),
	)
	apiClient := azkustoingestv2.NewAPIClient(srv.URL, srv.URL, baseClient)
	client := azkustoingestv2.NewStreamingIngestClient(apiClient)

	csvData := "a,b\n1,2\n"
	source := ingestoptions.NewStreamSource(
		io.NopCloser(bytes.NewReader([]byte(csvData))),
		ingestoptions.FormatCSV,
	)

	_, err := client.Ingest(context.Background(), "db", "tbl", source,
		&ingestoptions.IngestRequestProperties{Format: ingestoptions.FormatCSV})
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
	if !strings.Contains(err.Error(), "disabled") {
		t.Errorf("expected error to mention 'disabled', got %q", err.Error())
	}
}

// TestStreamingIngest_GetOperationSummary_ReturnsEmpty verifies streaming doesn't support tracking.
func TestStreamingIngest_GetOperationSummary_ReturnsEmpty(t *testing.T) {
	t.Parallel()
	client := azkustoingestv2.NewStreamingIngestClient(nil)
	status, err := client.GetOperationSummary(context.Background(), &azkustoingestv2.IngestionOperation{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status.Succeeded != 0 || status.Failed != 0 {
		t.Errorf("expected empty status, got %+v", status)
	}
}

// TestStreamingIngest_FileSource_SendsOctetStream verifies FileSource sends binary body.
func TestStreamingIngest_FileSource_SendsOctetStream(t *testing.T) {
	t.Parallel()

	var capturedContentType string
	var capturedBody []byte
	var capturedContentEncoding string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedContentType = r.Header.Get("Content-Type")
		capturedContentEncoding = r.Header.Get("Content-Encoding")
		capturedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	baseClient := httpclient.NewBaseClient(
		func(ctx context.Context) (string, error) { return "token", nil },
		ingestoptions.NewClientDetails("app", "user", "1.0"),
	)
	apiClient := azkustoingestv2.NewAPIClient(srv.URL, srv.URL, baseClient)
	client := azkustoingestv2.NewStreamingIngestClient(apiClient)

	// Create a temp file for FileSource
	tmpFile := createTempCSVFile(t, "name,age\nAlice,30\n")

	source := ingestoptions.NewFileSource(tmpFile, ingestoptions.FormatCSV)
	t.Cleanup(func() {
		if err := source.Close(); err != nil {
			t.Error(err)
		}
	})

	_, err := client.Ingest(context.Background(), "db", "tbl", source,
		&ingestoptions.IngestRequestProperties{Format: ingestoptions.FormatCSV})
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	if capturedContentType != "application/octet-stream" {
		t.Errorf("expected Content-Type 'application/octet-stream', got %q", capturedContentType)
	}
	if capturedContentEncoding != "gzip" {
		t.Errorf("expected Content-Encoding gzip, got %q", capturedContentEncoding)
	}
	if got := readStreamingGzip(t, capturedBody); string(got) != "name,age\nAlice,30\n" {
		t.Errorf("unexpected file payload: %q", got)
	}
}
