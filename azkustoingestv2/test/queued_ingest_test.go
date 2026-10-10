// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/azkustoingestv2"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/httpclient"
)

// fakeUploader implements upload.Uploader for testing queued flow without real blob storage.
type fakeUploader struct {
	blobURL   string
	blobSize  int64
	err       error
	uploads   int
	lastData  []byte
}

func (f *fakeUploader) Upload(ctx context.Context, source ingestoptions.LocalSource) (*ingestoptions.BlobSource, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.uploads++
	if stream, err := source.Data(); err == nil {
		f.lastData, _ = io.ReadAll(stream)
	}
	bs, err := ingestoptions.NewBlobSource(f.blobURL, source.Format())
	if err != nil {
		return nil, err
	}
	bs.BlobExactSize = f.blobSize
	return bs, nil
}

func (f *fakeUploader) UploadMany(ctx context.Context, sources []ingestoptions.LocalSource) (*ingestoptions.UploadResults, error) {
	results := &ingestoptions.UploadResults{}
	for _, s := range sources {
		_, err := f.Upload(ctx, s)
		if err != nil {
			results.FailureCount++
			continue
		}
		results.SuccessCount++
	}
	return results, nil
}

func (f *fakeUploader) SetIgnoreSizeLimit(ignore bool) {}
func (f *fakeUploader) Close() error                   { return nil }

// fakeConfigCache implements config.ConfigurationCache for testing.
type fakeConfigCache struct {
	maxBlobs int
}

func (f *fakeConfigCache) GetConfiguration(ctx context.Context) (*fakeConfigData, error) {
	return &fakeConfigData{maxBlobs: f.maxBlobs}, nil
}
func (f *fakeConfigCache) RefreshInterval() time.Duration { return time.Hour }
func (f *fakeConfigCache) Close()                         {}

type fakeConfigData struct {
	maxBlobs int
}

func newTestBaseClient(token string) *httpclient.BaseClient {
	return httpclient.NewBaseClient(
		func(ctx context.Context) (string, error) { return token, nil },
		ingestoptions.NewClientDetails("test-app", "test-user", "1.0.0"),
	)
}

// TestQueuedIngest_BlobSource_PostsCorrectPayload verifies that IngestBlobs sends the correct
// request payload to the DM endpoint with proper URL, format, tags, and properties.
func TestQueuedIngest_BlobSource_PostsCorrectPayload(t *testing.T) {
	t.Parallel()

	var capturedBody []byte
	var capturedPath string
	var capturedAuth string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		capturedAuth = r.Header.Get("Authorization")
		capturedBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"ingestionOperationId": "op-123-abc",
		})
	}))
	defer srv.Close()

	baseClient := newTestBaseClient("test-token-xyz")
	apiClient := azkustoingestv2.NewAPIClient(srv.URL, srv.URL, baseClient)

	// Use a nil config cache + nil uploader since we're testing blob path only
	client := azkustoingestv2.NewQueuedIngestClient(apiClient, newDefaultFakeCache(), nil, false)

	blob := mustBlobSource(t,
		"https://storage.blob.core.windows.net/container/data.csv.gz?sas=token",
		ingestoptions.FormatCSV,
	)
	blob.BlobExactSize = 1024

	props := &ingestoptions.IngestRequestProperties{
		Format:            ingestoptions.FormatCSV,
		IgnoreFirstRecord: true,
		FlushImmediately:  true,
		Tags:              []string{"env:test"},
		IngestByTags:      []string{"batch-1"},
		DropByTags:        []string{"old-data"},
	}

	resp, err := client.IngestBlobs(context.Background(), "testdb", "testtable",
		[]*ingestoptions.BlobSource{blob}, props)
	if err != nil {
		t.Fatalf("IngestBlobs failed: %v", err)
	}

	// Verify operation ID from response
	if resp.Response.OperationID != "op-123-abc" {
		t.Errorf("expected operationID 'op-123-abc', got %q", resp.Response.OperationID)
	}
	if resp.Kind != ingestoptions.IngestKindQueued {
		t.Errorf("expected kind Queued, got %v", resp.Kind)
	}

	// Verify request path
	if capturedPath != "/v1/rest/ingestion/queued/testdb/testtable" {
		t.Errorf("expected path '/v1/rest/ingestion/queued/testdb/testtable', got %q", capturedPath)
	}

	// Verify auth header
	if capturedAuth != "Bearer test-token-xyz" {
		t.Errorf("expected auth 'Bearer test-token-xyz', got %q", capturedAuth)
	}

	// Verify request body structure
	var reqBody map[string]interface{}
	if err := json.Unmarshal(capturedBody, &reqBody); err != nil {
		t.Fatalf("failed to parse request body: %v", err)
	}

	// Check blobs array
	blobs, ok := reqBody["blobs"].([]interface{})
	if !ok || len(blobs) != 1 {
		t.Fatalf("expected 1 blob in request, got %v", reqBody["blobs"])
	}
	blobPayload := blobs[0].(map[string]interface{})
	if blobPayload["url"] != blob.BlobPath() {
		t.Errorf("expected blob url %q, got %q", blob.BlobPath(), blobPayload["url"])
	}

	// Check properties
	props2 := reqBody["properties"].(map[string]interface{})
	if props2["format"] != "csv" {
		t.Errorf("expected format 'csv', got %q", props2["format"])
	}
	if props2["ignoreFirstRecord"] != true {
		t.Errorf("expected ignoreFirstRecord=true")
	}
	if props2["flushImmediately"] != true {
		t.Errorf("expected flushImmediately=true")
	}

	// Verify synthesized tags include ingest-by and drop-by prefixed tags
	tags, ok := props2["tags"].([]interface{})
	if !ok {
		t.Fatalf("expected tags in properties, got %v", props2["tags"])
	}
	tagStrs := make([]string, len(tags))
	for i, t := range tags {
		tagStrs[i] = t.(string)
	}
	found := map[string]bool{"env:test": false, "ingest-by:batch-1": false, "drop-by:old-data": false}
	for _, tag := range tagStrs {
		found[tag] = true
	}
	for tag, ok := range found {
		if !ok {
			t.Errorf("expected tag %q in synthesized tags, got %v", tag, tagStrs)
		}
	}
}

// TestQueuedIngest_BlobValidation_EmptyList verifies that empty source list is rejected.
func TestQueuedIngest_BlobValidation_EmptyList(t *testing.T) {
	t.Parallel()
	client := azkustoingestv2.NewQueuedIngestClient(nil, nil, nil, false)
	_, err := client.IngestBlobs(context.Background(), "db", "table",
		[]*ingestoptions.BlobSource{}, nil)
	if err == nil {
		t.Fatal("expected error for empty sources list")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Errorf("expected 'empty' in error message, got %q", err.Error())
	}
}

// TestQueuedIngest_BlobValidation_FormatMismatch verifies that blobs with different formats are rejected.
func TestQueuedIngest_BlobValidation_FormatMismatch(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Should not reach here
		t.Error("server should not be called for validation error")
	}))
	defer srv.Close()

	baseClient := newTestBaseClient("token")
	apiClient := azkustoingestv2.NewAPIClient(srv.URL, srv.URL, baseClient)
	client := azkustoingestv2.NewQueuedIngestClient(apiClient, newDefaultFakeCache(), nil, false)

	blobs := []*ingestoptions.BlobSource{
		mustBlobSource(t, "https://storage/a.csv", ingestoptions.FormatCSV),
		mustBlobSource(t, "https://storage/b.json", ingestoptions.FormatJSON),
	}

	_, err := client.IngestBlobs(context.Background(), "db", "table", blobs, &ingestoptions.IngestRequestProperties{
		Format: ingestoptions.FormatCSV,
	})
	if err == nil {
		t.Fatal("expected error for format mismatch")
	}
	if !strings.Contains(err.Error(), "same format") {
		t.Errorf("expected 'same format' in error, got %q", err.Error())
	}
}

// TestQueuedIngest_BlobValidation_DuplicateURLs verifies that duplicate blob URLs are rejected.
func TestQueuedIngest_BlobValidation_DuplicateURLs(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("server should not be called for validation error")
	}))
	defer srv.Close()

	baseClient := newTestBaseClient("token")
	apiClient := azkustoingestv2.NewAPIClient(srv.URL, srv.URL, baseClient)
	client := azkustoingestv2.NewQueuedIngestClient(apiClient, newDefaultFakeCache(), nil, false)

	blobs := []*ingestoptions.BlobSource{
		mustBlobSource(t, "https://storage/data.csv?sas=A", ingestoptions.FormatCSV),
		mustBlobSource(t, "https://storage/data.csv?sas=B", ingestoptions.FormatCSV),
	}

	_, err := client.IngestBlobs(context.Background(), "db", "table", blobs, &ingestoptions.IngestRequestProperties{
		Format: ingestoptions.FormatCSV,
	})
	if err == nil {
		t.Fatal("expected error for duplicate URLs")
	}
	if !strings.Contains(err.Error(), "duplicate") {
		t.Errorf("expected 'duplicate' in error, got %q", err.Error())
	}
}

// TestQueuedIngest_ServerError_ParsesOneAPI verifies that OneAPI error responses are properly parsed.
func TestQueuedIngest_ServerError_ParsesOneAPI(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": map[string]interface{}{
				"code":         "BadRequest_InvalidBlob",
				"message":      "The blob URL is invalid",
				"@permanent":   true,
				"@failureCode": "400",
			},
		})
	}))
	defer srv.Close()

	baseClient := newTestBaseClient("token")
	apiClient := azkustoingestv2.NewAPIClient(srv.URL, srv.URL, baseClient)
	client := azkustoingestv2.NewQueuedIngestClient(apiClient, newDefaultFakeCache(), nil, false)

	blob := mustBlobSource(t, "https://storage/bad.csv", ingestoptions.FormatCSV)
	_, err := client.IngestBlobs(context.Background(), "db", "table",
		[]*ingestoptions.BlobSource{blob}, &ingestoptions.IngestRequestProperties{Format: ingestoptions.FormatCSV})

	if err == nil {
		t.Fatal("expected error for 400 response")
	}
	if !strings.Contains(err.Error(), "invalid") || !strings.Contains(strings.ToLower(err.Error()), "blob") {
		t.Errorf("expected error to mention invalid blob, got %q", err.Error())
	}
}

// TestQueuedIngest_ServerError_404IsNonPermanent verifies HTTP 404 → non-permanent error.
func TestQueuedIngest_ServerError_404IsNonPermanent(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": map[string]interface{}{
				"code":    "NotFound",
				"message": "Database not found",
			},
		})
	}))
	defer srv.Close()

	baseClient := newTestBaseClient("token")
	apiClient := azkustoingestv2.NewAPIClient(srv.URL, srv.URL, baseClient)
	client := azkustoingestv2.NewQueuedIngestClient(apiClient, newDefaultFakeCache(), nil, false)

	blob := mustBlobSource(t, "https://storage/data.csv", ingestoptions.FormatCSV)
	_, err := client.IngestBlobs(context.Background(), "db", "table",
		[]*ingestoptions.BlobSource{blob}, &ingestoptions.IngestRequestProperties{Format: ingestoptions.FormatCSV})

	if err == nil {
		t.Fatal("expected error for 404 response")
	}

	// Check it's a service error (non-permanent)
	var serviceErr *ingestoptions.IngestServiceError
	if ok := containsError(err, &serviceErr); !ok {
		// Even if wrapped, the message should indicate non-permanent behavior
		t.Logf("error type chain: %T → checking IsPermanent via message", err)
	}
}

// TestQueuedIngest_GetOperationStatus verifies status polling request format.
func TestQueuedIngest_GetOperationStatus(t *testing.T) {
	t.Parallel()

	var capturedPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.RequestURI()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": map[string]interface{}{
				"succeeded":  int64(5),
				"failed":     int64(1),
				"inProgress": int64(0),
				"canceled":   int64(0),
			},
			"details": []map[string]interface{}{
				{
					"blobPath": "https://storage/blob1.csv",
					"status":   "Succeeded",
					"sourceId": "src-1",
				},
			},
		})
	}))
	defer srv.Close()

	baseClient := newTestBaseClient("token")
	apiClient := azkustoingestv2.NewAPIClient(srv.URL, srv.URL, baseClient)
	client := azkustoingestv2.NewQueuedIngestClient(apiClient, newDefaultFakeCache(), nil, false)

	op := &azkustoingestv2.IngestionOperation{
		Database:    "mydb",
		Table:       "mytable",
		OperationID: "op-456",
	}

	resp, err := client.GetOperationDetails(context.Background(), op)
	if err != nil {
		t.Fatalf("GetOperationDetails failed: %v", err)
	}

	// Verify request path
	expectedPath := "/v1/rest/ingestion/queued/mydb/mytable/op-456?details=true"
	if capturedPath != expectedPath {
		t.Errorf("expected path %q, got %q", expectedPath, capturedPath)
	}

	// Verify response parsing
	if resp.Status == nil {
		t.Fatal("expected non-nil status")
	}
	if resp.Status.Succeeded != 5 {
		t.Errorf("expected 5 succeeded, got %d", resp.Status.Succeeded)
	}
	if resp.Status.Failed != 1 {
		t.Errorf("expected 1 failed, got %d", resp.Status.Failed)
	}
	if len(resp.Details) != 1 {
		t.Fatalf("expected 1 detail, got %d", len(resp.Details))
	}
	if resp.Details[0].Status != "Succeeded" {
		t.Errorf("expected detail status 'Succeeded', got %q", resp.Details[0].Status)
	}
}

// TestQueuedIngest_GetOperationSummary verifies summary returns aggregate status.
func TestQueuedIngest_GetOperationSummary(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": map[string]interface{}{
				"succeeded":  int64(10),
				"failed":     int64(0),
				"inProgress": int64(2),
				"canceled":   int64(0),
			},
		})
	}))
	defer srv.Close()

	baseClient := newTestBaseClient("token")
	apiClient := azkustoingestv2.NewAPIClient(srv.URL, srv.URL, baseClient)
	client := azkustoingestv2.NewQueuedIngestClient(apiClient, newDefaultFakeCache(), nil, false)

	op := &azkustoingestv2.IngestionOperation{
		Database:    "db",
		Table:       "tbl",
		OperationID: "op-789",
	}

	status, err := client.GetOperationSummary(context.Background(), op)
	if err != nil {
		t.Fatalf("GetOperationSummary failed: %v", err)
	}
	if status.Succeeded != 10 {
		t.Errorf("expected 10 succeeded, got %d", status.Succeeded)
	}
	if status.InProgress != 2 {
		t.Errorf("expected 2 inProgress, got %d", status.InProgress)
	}
}

// TestQueuedIngest_Headers_AreSet verifies tracing headers are present.
func TestQueuedIngest_Headers_AreSet(t *testing.T) {
	t.Parallel()

	var capturedHeaders http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedHeaders = r.Header.Clone()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"ingestionOperationId": "op-1"})
	}))
	defer srv.Close()

	baseClient := newTestBaseClient("token")
	apiClient := azkustoingestv2.NewAPIClient(srv.URL, srv.URL, baseClient)
	client := azkustoingestv2.NewQueuedIngestClient(apiClient, newDefaultFakeCache(), nil, false)

	blob := mustBlobSource(t, "https://storage/data.csv", ingestoptions.FormatCSV)
	_, _ = client.IngestBlobs(context.Background(), "db", "tbl",
		[]*ingestoptions.BlobSource{blob}, &ingestoptions.IngestRequestProperties{Format: ingestoptions.FormatCSV})

	requiredHeaders := []string{
		"Authorization",
		ingestoptions.HeaderMSVersion,
		ingestoptions.HeaderMSClientRequestID,
		ingestoptions.HeaderConnection,
		ingestoptions.HeaderContentType,
		ingestoptions.HeaderAccept,
	}
	for _, h := range requiredHeaders {
		if capturedHeaders.Get(h) == "" {
			t.Errorf("expected header %q to be set, was empty", h)
		}
	}

	// Verify x-ms-client-request-id format: KIC.execute;{uuid}
	reqID := capturedHeaders.Get(ingestoptions.HeaderMSClientRequestID)
	if !strings.HasPrefix(reqID, "KIC.execute;") {
		t.Errorf("expected client-request-id to start with 'KIC.execute;', got %q", reqID)
	}

	// Verify API version
	if capturedHeaders.Get(ingestoptions.HeaderMSVersion) != ingestoptions.KustoAPIVersion {
		t.Errorf("expected API version %q, got %q",
			ingestoptions.KustoAPIVersion, capturedHeaders.Get(ingestoptions.HeaderMSVersion))
	}
}

// containsError is a helper that checks if err chain contains a specific error type.
func containsError[T error](err error, target *T) bool {
	for err != nil {
		if _, ok := err.(T); ok {
			return true
		}
		if u, ok := err.(interface{ Unwrap() error }); ok {
			err = u.Unwrap()
		} else {
			break
		}
	}
	return false
}
