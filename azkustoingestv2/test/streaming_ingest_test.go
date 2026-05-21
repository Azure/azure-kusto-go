// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedBody, _ = io.ReadAll(r.Body)
		capturedContentType = r.Header.Get("Content-Type")
		capturedURL = r.URL.RequestURI()
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
			Format:             ingestoptions.FormatCSV,
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

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedBody, _ = io.ReadAll(r.Body)
		capturedContentType = r.Header.Get("Content-Type")
		capturedURL = r.URL.RequestURI()
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

	// Verify body matches the CSV data
	if string(capturedBody) != csvData {
		t.Errorf("expected body %q, got %q", csvData, string(capturedBody))
	}

	// Verify no sourceKind in URL (not a blob)
	if strings.Contains(capturedURL, "sourceKind") {
		t.Errorf("expected no sourceKind in URL for stream source, got %q", capturedURL)
	}
}

// TestStreamingIngest_SizeLimit verifies that data exceeding StreamingMaxRequestBodySize is rejected.
func TestStreamingIngest_SizeLimit(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("server should not be called when size limit exceeded")
	}))
	defer srv.Close()

	baseClient := httpclient.NewBaseClient(
		func(ctx context.Context) (string, error) { return "token", nil },
		ingestoptions.NewClientDetails("app", "user", "1.0"),
	)
	apiClient := azkustoingestv2.NewAPIClient(srv.URL, srv.URL, baseClient)
	client := azkustoingestv2.NewStreamingIngestClient(apiClient)

	// Create data slightly over 10 MiB
	bigData := make([]byte, ingestoptions.StreamingMaxRequestBodySize+1)
	source := ingestoptions.NewStreamSource(
		io.NopCloser(bytes.NewReader(bigData)),
		ingestoptions.FormatCSV,
	)

	_, err := client.Ingest(context.Background(), "db", "tbl", source,
		&ingestoptions.IngestRequestProperties{Format: ingestoptions.FormatCSV})
	if err == nil {
		t.Fatal("expected error for oversized data")
	}

	var sizeErr *ingestoptions.IngestSizeLimitExceededError
	if ok := containsError(err, &sizeErr); !ok {
		if !strings.Contains(strings.ToLower(err.Error()), "size") {
			t.Errorf("expected size-related error, got %q", err.Error())
		}
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
				"code":    "InternalError",
				"message": "Streaming ingestion is disabled for this cluster",
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

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedContentType = r.Header.Get("Content-Type")
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

	_, err := client.Ingest(context.Background(), "db", "tbl", source,
		&ingestoptions.IngestRequestProperties{Format: ingestoptions.FormatCSV})
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	if capturedContentType != "application/octet-stream" {
		t.Errorf("expected Content-Type 'application/octet-stream', got %q", capturedContentType)
	}
	if len(capturedBody) == 0 {
		t.Error("expected non-empty body")
	}
}
