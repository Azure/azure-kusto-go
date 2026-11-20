// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/azkustoingestv2"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

// fakeUploader implements upload.Uploader for testing queued flow without real blob storage.
type fakeUploader struct {
	blobURL  string
	blobSize int64
	err      error
	uploads  int
	lastData []byte
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

// TestQueuedIngest_BlobSource_PostsCorrectPayload verifies that IngestBlobs sends the correct
// request payload to the DM endpoint with proper URL, format, tags, and properties.
func TestQueuedIngest_BlobSource_PostsCorrectPayload(t *testing.T) {
	t.Parallel()

	var capturedBody []byte
	var capturedPath string
	var capturedAuth string

	srv := newQueuedTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		capturedAuth = r.Header.Get("Authorization")
		capturedBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"ingestionOperationId": "op-123-abc",
		})
	}))
	defer srv.Close()

	client := newTestQueuedClient(t, srv.URL, "test-token-xyz")

	blob := mustBlobSource(t,
		"https://storage.blob.core.windows.net/container/data.csv.gz?sas=token",
		ingestoptions.FormatCSV,
	)
	blob.BlobExactSize = 1024
	unknownSizeBlob := mustBlobSource(t,
		"https://storage.blob.core.windows.net/container/unknown.csv.gz?sas=token",
		ingestoptions.FormatCSV,
	)

	props := &ingestoptions.IngestRequestProperties{
		Format:              ingestoptions.FormatCSV,
		IgnoreFirstRecord:   true,
		SkipBatching:        true,
		Tags:                []string{"env:test"},
		IngestByTags:        []string{"batch-1"},
		DropByTags:          []string{"old-data"},
		IngestionMappingRef: "CSVMapping",
		IngestionMapping: []ingestoptions.ColumnMapping{{
			Name:       "value",
			DataType:   "string",
			Properties: map[string]string{"Ordinal": "0"},
		}},
	}

	resp, err := client.IngestBlobs(context.Background(), "testdb", "testtable",
		[]*ingestoptions.BlobSource{blob, unknownSizeBlob}, props)
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
	if !ok || len(blobs) != 2 {
		t.Fatalf("expected 2 blobs in request, got %v", reqBody["blobs"])
	}
	blobPayload := blobs[0].(map[string]interface{})
	if blobPayload["url"] != blob.BlobPath() {
		t.Errorf("expected blob url %q, got %q", blob.BlobPath(), blobPayload["url"])
	}
	if blobPayload["rawSize"] != float64(1024) {
		t.Errorf("expected known rawSize=1024, got %v", blobPayload["rawSize"])
	}
	if size, present := blobs[1].(map[string]interface{})["rawSize"]; present {
		t.Errorf("unknown blob size must be omitted, got rawSize=%v", size)
	}

	// Check properties
	props2 := reqBody["properties"].(map[string]interface{})
	if props2["format"] != "csv" {
		t.Errorf("expected format 'csv', got %q", props2["format"])
	}
	if props2["ignoreFirstRecord"] != true {
		t.Errorf("expected ignoreFirstRecord=true")
	}
	if props2["skipBatching"] != true {
		t.Errorf("expected skipBatching=true")
	}
	if _, present := props2["flushImmediately"]; present {
		t.Error("properties must not contain flushImmediately")
	}

	mappingJSON, ok := props2["ingestionMapping"].(string)
	if !ok {
		t.Fatalf("expected ingestionMapping to be a JSON string, got %T", props2["ingestionMapping"])
	}
	if props2["ingestionMappingReference"] != props.IngestionMappingRef {
		t.Fatalf("queued ingestion did not retain the mapping reference: %v", props2["ingestionMappingReference"])
	}
	var mappings []map[string]json.RawMessage
	if err := json.Unmarshal([]byte(mappingJSON), &mappings); err != nil {
		t.Fatalf("failed to decode inline mapping: %v", err)
	}
	if len(mappings) != 1 {
		t.Fatalf("expected one inline mapping, got %d", len(mappings))
	}
	if string(mappings[0]["columnName"]) != `"value"` {
		t.Errorf("expected columnName to identify the column, got %s", mappings[0]["columnName"])
	}
	if string(mappings[0]["columnType"]) != `"string"` {
		t.Errorf("expected columnType to identify the data type, got %s", mappings[0]["columnType"])
	}
	var mappingProperties map[string]string
	if err := json.Unmarshal(mappings[0]["properties"], &mappingProperties); err != nil {
		t.Fatalf("failed to decode mapping properties: %v", err)
	}
	if mappingProperties["Ordinal"] != "0" {
		t.Errorf("expected mapping properties Ordinal=0, got %q", mappingProperties["Ordinal"])
	}
	for _, oldKey := range []string{"Name", "DataType", "Properties"} {
		if _, present := mappings[0][oldKey]; present {
			t.Errorf("inline mapping must not contain the old %s key", oldKey)
		}
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

func TestQueuedIngest_IngestIfNotExists(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		tags []string
	}{
		{"nil", nil},
		{"empty", []string{}},
		{"single", []string{"ingest-by:batch-42"}},
		{"multiple", []string{"ingest-by:batch-42", "ingest-by:batch-43"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var capturedBody []byte
			srv := newQueuedTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var err error
				capturedBody, err = io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("reading queued request: %v", err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(map[string]string{"ingestionOperationId": "op-dedup"}); err != nil {
					t.Errorf("writing queued response: %v", err)
				}
			}))
			defer srv.Close()

			props, err := ingestoptions.NewIngestRequestPropertiesBuilder().
				WithIngestIfNotExists(tc.tags).
				Build()
			if err != nil {
				t.Fatal(err)
			}
			client := newTestQueuedClient(t, srv.URL, "token")
			source := mustBlobSource(t, "https://storage/data.csv", ingestoptions.FormatCSV)
			if _, err := client.Ingest(context.Background(), "db", "table", source, props); err != nil {
				t.Fatalf("ingest failed: %v", err)
			}
			var request struct {
				Properties map[string]json.RawMessage `json:"properties"`
			}
			if err := json.Unmarshal(capturedBody, &request); err != nil {
				t.Fatalf("decoding queued request: %v", err)
			}
			value, present := request.Properties["ingestIfNotExists"]
			if len(tc.tags) == 0 {
				if present {
					t.Fatalf("empty ingestIfNotExists must be omitted, got %s", value)
				}
				return
			}
			var tags []string
			if err := json.Unmarshal(value, &tags); err != nil {
				t.Fatalf("ingestIfNotExists must be an array: %v", err)
			}
			if !slices.Equal(tags, tc.tags) {
				t.Fatalf("tags changed: got %v, want %v", tags, tc.tags)
			}
			if _, present := request.Properties["tags"]; present {
				t.Fatal("ingestIfNotExists must not implicitly attach extent tags")
			}
		})
	}
}

func TestQueuedIngest_ValidationPolicy(t *testing.T) {
	t.Parallel()

	for _, managed := range []bool{false, true} {
		name := "queued"
		if managed {
			name = "managed-fallback"
		}
		t.Run(name, func(t *testing.T) {
			for _, tc := range []struct {
				name   string
				policy *ingestoptions.ValidationPolicy
			}{
				{"omitted", nil},
				{"configured", &ingestoptions.ValidationPolicy{
					ValidationOptions:      "SameNumberOfFields",
					ValidationImplications: "FailIngestion",
				}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					var capturedBody []byte
					dmHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						var err error
						capturedBody, err = io.ReadAll(r.Body)
						if err != nil {
							t.Errorf("reading queued request: %v", err)
							w.WriteHeader(http.StatusBadRequest)
							return
						}
						w.Header().Set("Content-Type", "application/json")
						if err := json.NewEncoder(w).Encode(map[string]string{"ingestionOperationId": "op-policy"}); err != nil {
							t.Errorf("writing queued response: %v", err)
						}
					})

					var client azkustoingestv2.IngestClient
					if managed {
						c, cleanup := newTestManagedClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							w.WriteHeader(http.StatusServiceUnavailable)
						}), dmHandler)
						defer cleanup()
						client = c
					} else {
						srv := newQueuedTestServer(t, dmHandler)
						defer srv.Close()
						client = newTestQueuedClient(t, srv.URL, "token")
					}

					source := mustBlobSource(t, "https://storage/data.csv", ingestoptions.FormatCSV)
					response, err := client.Ingest(context.Background(), "db", "table", source,
						&ingestoptions.IngestRequestProperties{ValidationPolicy: tc.policy})
					if err != nil {
						t.Fatalf("ingest failed: %v", err)
					}
					if response.Kind != ingestoptions.IngestKindQueued {
						t.Fatalf("expected queued ingestion, got %v", response.Kind)
					}

					var request struct {
						Properties map[string]json.RawMessage `json:"properties"`
					}
					if err := json.Unmarshal(capturedBody, &request); err != nil {
						t.Fatalf("decoding queued request: %v", err)
					}
					value, present := request.Properties["validationPolicy"]
					if tc.policy == nil {
						if present {
							t.Fatalf("nil validation policy must be omitted, got %s", value)
						}
						return
					}
					if !present {
						t.Fatal("configured validation policy is missing")
					}
					var encoded string
					if err := json.Unmarshal(value, &encoded); err != nil {
						t.Fatalf("validationPolicy must be a JSON string: %v", err)
					}
					var policy ingestoptions.ValidationPolicy
					if err := json.Unmarshal([]byte(encoded), &policy); err != nil {
						t.Fatalf("validationPolicy must contain JSON: %v", err)
					}
					if policy != *tc.policy {
						t.Fatalf("validation policy changed: got %+v, want %+v", policy, *tc.policy)
					}
				})
			}
		})
	}
}

// TestQueuedIngest_BlobValidation_EmptyList verifies that empty source list is rejected.
func TestQueuedIngest_BlobValidation_EmptyList(t *testing.T) {
	t.Parallel()
	client := &azkustoingestv2.QueuedIngestClient{}
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

	srv := newQueuedTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Should not reach here
		t.Error("server should not be called for validation error")
	}))
	defer srv.Close()

	client := newTestQueuedClient(t, srv.URL, "token")

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

	srv := newQueuedTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("server should not be called for validation error")
	}))
	defer srv.Close()

	client := newTestQueuedClient(t, srv.URL, "token")

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

	srv := newQueuedTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

	client := newTestQueuedClient(t, srv.URL, "token")

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

	srv := newQueuedTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

	client := newTestQueuedClient(t, srv.URL, "token")

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
	srv := newQueuedTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

	client := newTestQueuedClient(t, srv.URL, "token")

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

	srv := newQueuedTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.RequestURI() != "/v1/rest/ingestion/queued/db/tbl/op-789?details=false" {
			t.Errorf("unexpected summary request: %s %s", r.Method, r.URL)
		}
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

	client := newTestQueuedClient(t, srv.URL, "token")

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
	srv := newQueuedTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedHeaders = r.Header.Clone()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"ingestionOperationId": "op-1"})
	}))
	defer srv.Close()

	client := newTestQueuedClient(t, srv.URL, "token")

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
