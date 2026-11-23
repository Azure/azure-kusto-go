// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package azkustoingestv2

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/policy"
)

// Exercise the real V2 clients and storage SDKs against local HTTP services, not live Kusto.
type mockE2ECluster struct {
	t       *testing.T
	method  ingestoptions.UploadMethod
	engine  *httptest.Server
	dm      *httptest.Server
	storage *httptest.Server

	mu                sync.Mutex
	blobs             map[string]*mockE2EBlob
	events            []string
	streamed          [][]byte
	submissions       []ingestRequest
	statusReads       int
	statusFailure     int
	rejectUploads     bool
	streamingStatus   int
	streamingFailures int
}

type mockE2EBlob struct {
	blocks    map[string][]byte
	data      []byte
	committed bool
}

func newMockE2ECluster(t *testing.T, method ingestoptions.UploadMethod) *mockE2ECluster {
	t.Helper()
	c := &mockE2ECluster{t: t, method: method, blobs: make(map[string]*mockE2EBlob)}
	c.storage = httptest.NewServer(http.HandlerFunc(c.serveStorage))
	t.Cleanup(c.storage.Close)
	c.dm = httptest.NewServer(http.HandlerFunc(c.serveDM))
	t.Cleanup(c.dm.Close)
	c.engine = httptest.NewServer(http.HandlerFunc(c.serveEngine))
	t.Cleanup(c.engine.Close)
	return c
}

func (c *mockE2ECluster) newClient(managed bool, options ...ClientOption) IngestClient {
	c.t.Helper()
	opts := []ClientOption{
		WithTokenProvider(func(context.Context) (string, error) { return "mock-kusto-token", nil }),
		WithUploadMethod(c.method),
		WithRetryPolicy(ingestoptions.NewSimpleRetryPolicy(0, 1)),
	}
	opts = append(opts, options...)
	var client IngestClient
	if managed {
		opts = append(opts, withManagedStreamingPolicy(policy.NewDefaultManagedStreamingPolicy(
			policy.WithPolicyRetryPolicy(ingestoptions.NewSimpleRetryPolicy(0, 1)),
		)))
		managedClient, err := NewManagedStreamingClient(c.dm.URL, c.engine.URL, opts...)
		if err != nil {
			c.t.Fatal(err)
		}
		c.t.Cleanup(managedClient.queuedClient.configCache.Close)
		client = managedClient
	} else {
		queuedClient, err := NewQueuedClient(c.dm.URL, opts...)
		if err != nil {
			c.t.Fatal(err)
		}
		c.t.Cleanup(queuedClient.configCache.Close)
		client = queuedClient
	}
	c.t.Cleanup(func() {
		if err := client.Close(); err != nil {
			c.t.Errorf("close ingest client: %v", err)
		}
	})
	return client
}

func (c *mockE2ECluster) serveStorage(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.Header.Get("Authorization") != "" || r.URL.Query().Get("sig") != "mock-storage-sas" {
		c.t.Error("storage request must use its SAS, not the Kusto bearer token")
		w.WriteHeader(http.StatusForbidden)
		return
	}
	if c.rejectUploads {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `<Error><Code>AuthorizationFailure</Code><Message>Upload denied</Message></Error>`)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		c.t.Error(err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	blob := c.blobs[r.URL.Path]
	if blob == nil {
		blob = &mockE2EBlob{blocks: make(map[string][]byte)}
		c.blobs[r.URL.Path] = blob
	}
	w.Header().Set("ETag", `"mock-etag"`)
	w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
	complete := func() {
		blob.committed = true
		c.events = append(c.events, "upload-complete")
	}
	if c.method == ingestoptions.UploadMethodStorage {
		if r.Method != http.MethodPut {
			c.t.Errorf("unexpected Blob method: %s", r.Method)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		switch r.URL.Query().Get("comp") {
		case "block":
			blob.blocks[r.URL.Query().Get("blockid")] = body
		case "blocklist":
			var blocks struct {
				Latest []string `xml:"Latest"`
			}
			if err := xml.Unmarshal(body, &blocks); err != nil {
				c.t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			for _, id := range blocks.Latest {
				block, ok := blob.blocks[id]
				if !ok {
					c.t.Errorf("committing an unknown block: %s", id)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				blob.data = append(blob.data, block...)
			}
			complete()
		case "":
			blob.data = body
			complete()
		default:
			c.t.Errorf("unexpected Blob operation: %s", r.URL)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusCreated)
		return
	}
	switch {
	case r.Method == http.MethodPut && r.URL.Query().Get("resource") == "file":
		w.WriteHeader(http.StatusCreated)
	case r.Method == http.MethodPatch && r.URL.Query().Get("action") == "append":
		if r.URL.Query().Get("position") != fmt.Sprint(len(blob.data)) {
			c.t.Error("OneLake append used an incorrect offset")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		blob.data = append(blob.data, body...)
		w.WriteHeader(http.StatusAccepted)
	case r.Method == http.MethodPatch && r.URL.Query().Get("action") == "flush":
		if r.URL.Query().Get("position") != fmt.Sprint(len(blob.data)) {
			c.t.Error("OneLake flush used an incorrect size")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		complete()
		w.WriteHeader(http.StatusOK)
	default:
		c.t.Errorf("unexpected OneLake operation: %s %s", r.Method, r.URL)
		w.WriteHeader(http.StatusBadRequest)
	}
}

func (c *mockE2ECluster) serveDM(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.Header.Get("Authorization") != "Bearer mock-kusto-token" {
		c.t.Error("DM request is missing its Kusto token")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/v1/rest/ingestion/configuration":
		key := "containers"
		path := c.storage.URL + "/container?sig=mock-storage-sas"
		if c.method == ingestoptions.UploadMethodLake {
			key = "lakeFolders"
			path = c.storage.URL + "/workspace/lakehouse/Files/Ingestions?sig=mock-storage-sas"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"containerSettings": map[string]any{key: []map[string]string{{"path": path}}},
			"ingestionSettings": map[string]int{"maxBlobsPerBatch": 70},
		})
	case r.Method == http.MethodPost && r.URL.Path == "/v1/rest/ingestion/queued/db/table":
		var request ingestRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			c.t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if len(request.Blobs) != 1 {
			c.t.Errorf("expected one uploaded reference, got %+v", request.Blobs)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		u, err := url.Parse(request.Blobs[0].URL)
		if err != nil || u.Host != strings.TrimPrefix(c.storage.URL, "http://") || u.Query().Get("sig") != "mock-storage-sas" {
			c.t.Errorf("DM did not receive the uploaded SAS URL: %s", request.Blobs[0].URL)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if blob := c.blobs[u.Path]; blob == nil || !blob.committed {
			c.t.Error("DM submission referenced an uncommitted upload")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		c.submissions = append(c.submissions, request)
		c.events = append(c.events, "dm-submit")
		_, _ = io.WriteString(w, `{"ingestionOperationId":"mock-operation"}`)
	case r.Method == http.MethodGet && r.URL.Path == "/v1/rest/ingestion/queued/db/table/mock-operation":
		details := r.URL.Query().Get("details")
		if len(c.submissions) != 1 || (details != "true" && details != "false") {
			c.t.Error("status request did not reference the submitted operation with a valid details flag")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		c.statusReads++
		c.events = append(c.events, "status")
		if c.statusFailure != 0 {
			w.WriteHeader(c.statusFailure)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
				"code": "StatusLookupFailed", "message": "Unable to retrieve operation status",
				"@permanent": c.statusFailure != http.StatusNotFound,
			}})
			return
		}
		if c.statusReads == 1 {
			_ = json.NewEncoder(w).Encode(StatusResponse{Status: &OperationStatus{InProgress: 1}})
			return
		}
		submitted := c.submissions[0]
		detail := BlobStatus{
			BlobPath: submitted.Blobs[0].URL,
			SourceID: submitted.Blobs[0].SourceID,
			Status:   "Succeeded",
		}
		status := &OperationStatus{Succeeded: 1}
		if submitted.Properties.IngestionMappingRef == "MissingMapping" {
			status = &OperationStatus{Failed: 1}
			detail.Status = "Failed"
			detail.ErrorCode = "MappingNotFound"
			detail.FailureStatus = "Permanent"
			detail.Details = "The requested ingestion mapping does not exist"
		}
		response := StatusResponse{Status: status}
		if details == "true" {
			response.Details = []BlobStatus{detail}
		}
		_ = json.NewEncoder(w).Encode(response)
	default:
		c.t.Errorf("unexpected DM request: %s %s", r.Method, r.URL)
		w.WriteHeader(http.StatusNotFound)
	}
}

func (c *mockE2ECluster) serveEngine(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.Method != http.MethodPost || r.URL.Path != "/v1/rest/ingest/db/table" ||
		r.Header.Get("Authorization") != "Bearer mock-kusto-token" ||
		r.URL.Query().Get("streamFormat") != "csv" || r.URL.Query().Get("mappingName") == "" {
		c.t.Errorf("unexpected streaming request: %s %s", r.Method, r.URL)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if r.Header.Get("Content-Encoding") != "gzip" {
		c.t.Error("expected gzip streaming data")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	reader, err := gzip.NewReader(r.Body)
	if err != nil {
		c.t.Error(err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		c.t.Error(err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	c.streamed = append(c.streamed, data)
	c.events = append(c.events, "streaming")
	w.Header().Set("Content-Type", "application/json")
	status := c.streamingStatus
	if status == 0 || len(c.streamed) <= c.streamingFailures {
		status = http.StatusServiceUnavailable
	}
	if status == http.StatusOK {
		_, _ = io.WriteString(w, `{}`)
		return
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
		"code": "ServiceUnavailable", "message": "temporary streaming failure",
		"@permanent": status >= 400 && status < 500,
	}})
}

type mockE2EStream struct {
	io.Reader
	closes   atomic.Int32
	closeErr error
}

func (s *mockE2EStream) Read(p []byte) (int, error) {
	if s.closes.Load() != 0 {
		return 0, os.ErrClosed
	}
	return s.Reader.Read(p)
}

func (s *mockE2EStream) Close() error {
	s.closes.Add(1)
	return s.closeErr
}

func newMockE2ESource(t *testing.T, file bool, payload []byte, leaveOpen bool) ingestoptions.LocalSource {
	t.Helper()
	var source ingestoptions.LocalSource
	var checkOwnership func()
	if file {
		path := filepath.Join(t.TempDir(), "data.csv")
		if err := os.WriteFile(path, payload, 0600); err != nil {
			t.Fatal(err)
		}
		source = ingestoptions.NewFileSource(path, ingestoptions.FormatUnknown)
		stream, err := source.Data()
		if err != nil {
			t.Fatal(err)
		}
		handle := stream.(*os.File)
		checkOwnership = func() {
			var b [1]byte
			if _, err := handle.Read(b[:]); !errors.Is(err, os.ErrClosed) {
				t.Errorf("ingestion did not close its file handle: %v", err)
			}
		}
	} else {
		stream := &mockE2EStream{Reader: bytes.NewReader(payload)}
		source = ingestoptions.NewStreamSource(stream, ingestoptions.FormatCSV,
			ingestoptions.WithLeaveOpen(leaveOpen))
		checkOwnership = func() {
			var wantCloses int32
			if !leaveOpen {
				wantCloses = 1
			}
			if stream.closes.Load() != wantCloses {
				t.Errorf("stream closes=%d, want %d", stream.closes.Load(), wantCloses)
			}
		}
	}
	t.Cleanup(func() {
		checkOwnership()
		if err := source.Close(); err != nil {
			t.Errorf("close ingestion source: %v", err)
		}
	})
	return source
}

func TestStreamingRejectsInlineMappings(t *testing.T) {
	for _, kind := range []string{"stream", "file", "blob"} {
		for _, reference := range []string{"", "CSVMapping"} {
			t.Run(fmt.Sprintf("%s/reference=%s", kind, reference), func(t *testing.T) {
				var requests int
				client, err := NewStreamingClient("http://localhost",
					WithHTTPClient(&http.Client{Transport: endpointCorrectionTransport(func(r *http.Request) (*http.Response, error) {
						requests++
						if r.Body != nil {
							if err := r.Body.Close(); err != nil {
								return nil, err
							}
						}
						return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header),
							Body: io.NopCloser(strings.NewReader("{}")), Request: r}, nil
					})}))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = client.Close() })
				var source ingestoptions.IngestionSource
				if kind == "blob" {
					source, err = ingestoptions.NewBlobSource("https://storage.example/data.csv", ingestoptions.FormatCSV)
					if err != nil {
						t.Fatal(err)
					}
				} else {
					source = newMockE2ESource(t, kind == "file", []byte("Alice,30\n"), false)
				}
				result, err := client.Ingest(context.Background(), "db", "table", source,
					&ingestoptions.IngestRequestProperties{
						IngestionMappingRef: reference,
						IngestionMapping:    []ingestoptions.ColumnMapping{{Name: "name"}},
					})
				var clientErr *ingestoptions.IngestClientError
				if result != nil || !errors.As(err, &clientErr) || !clientErr.IsPermanent {
					t.Fatalf("expected permanent inline-mapping rejection, got result=%v err=%v", result, err)
				}
				if requests != 0 {
					t.Fatalf("invalid direct streaming options made %d HTTP requests", requests)
				}
			})
		}
	}
}

func TestManagedBlobMappingLegacyParity(t *testing.T) {
	for _, route := range []string{"streaming", "retry", "fallback", "queued-policy"} {
		for _, reference := range []string{"", "CSVMapping"} {
			t.Run(fmt.Sprintf("%s/reference=%s", route, reference), func(t *testing.T) {
				cluster := newMockE2ECluster(t, ingestoptions.UploadMethodStorage)
				blobURL := cluster.storage.URL + "/container/data.csv?sig=mock-storage-sas"
				cluster.mu.Lock()
				cluster.blobs["/container/data.csv"] = &mockE2EBlob{data: []byte("Alice,30\n"), committed: true}
				cluster.mu.Unlock()
				attempts := 0
				httpClient := &http.Client{Transport: endpointCorrectionTransport(func(r *http.Request) (*http.Response, error) {
					if !strings.HasPrefix(r.URL.String(), cluster.engine.URL+"/") {
						return http.DefaultTransport.RoundTrip(r)
					}
					attempts++
					if r.URL.Query().Get("mappingName") != reference {
						t.Errorf("streaming mapping reference=%q, want %q", r.URL.Query().Get("mappingName"), reference)
					}
					var body map[string]string
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						return nil, err
					}
					if err := r.Body.Close(); err != nil {
						return nil, err
					}
					if len(body) != 1 || body["SourceUri"] != blobURL {
						t.Errorf("unexpected managed streaming body: %v", body)
					}
					status, response := http.StatusOK, "{}"
					if route == "fallback" || route == "retry" && attempts == 1 {
						status = http.StatusServiceUnavailable
						response = `{"error":{"code":"ServiceUnavailable","message":"retry","@permanent":false}}`
					}
					return &http.Response{StatusCode: status, Header: make(http.Header),
						Body: io.NopCloser(strings.NewReader(response)), Request: r}, nil
				})}
				client := cluster.newClient(true, WithHTTPClient(httpClient)).(*ManagedStreamingIngestClient)
				if route == "retry" {
					client.policy = policy.NewDefaultManagedStreamingPolicy(
						policy.WithPolicyRetryPolicy(ingestoptions.NewSimpleRetryPolicy(0, 2)))
				}
				source, err := ingestoptions.NewBlobSource(blobURL, ingestoptions.FormatCSV)
				if err != nil {
					t.Fatal(err)
				}
				if route == "queued-policy" {
					client.policy.StreamingErrorCallback(source, "db", "table",
						policy.ManagedStreamingRequestFailureDetails{
							ErrorCategory: policy.ErrorCategoryTableConfigurationPreventsStreaming,
						})
				}
				props := &ingestoptions.IngestRequestProperties{
					IngestionMappingRef: reference,
					IngestionMapping: []ingestoptions.ColumnMapping{{
						Name: "name", DataType: "string", Properties: map[string]string{"Ordinal": "0"},
					}},
				}
				before, err := json.Marshal(props)
				if err != nil {
					t.Fatal(err)
				}
				result, err := client.Ingest(context.Background(), "db", "table", source, props)
				wantKind, wantAttempts := ingestoptions.IngestKindStreaming, 1
				if route == "retry" {
					wantAttempts = 2
				} else if route == "fallback" || route == "queued-policy" {
					wantKind = ingestoptions.IngestKindQueued
					if route == "queued-policy" {
						wantAttempts = 0
					}
				}
				if err != nil || result == nil || result.Kind != wantKind || attempts != wantAttempts {
					t.Fatalf("expected kind=%v attempts=%d, got result=%v attempts=%d err=%v",
						wantKind, wantAttempts, result, attempts, err)
				}
				after, err := json.Marshal(props)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(before, after) {
					t.Fatalf("managed ingestion mutated caller properties: before=%s after=%s", before, after)
				}
				cluster.mu.Lock()
				submissions := append([]ingestRequest(nil), cluster.submissions...)
				cluster.mu.Unlock()
				if wantKind == ingestoptions.IngestKindStreaming {
					if len(submissions) != 0 {
						t.Fatal("streaming success submitted a queued request")
					}
				} else {
					mapping, err := ingestoptions.SerializeColumnMappings(props.IngestionMapping)
					if err != nil {
						t.Fatal(err)
					}
					if len(submissions) != 1 || submissions[0].Properties.IngestionMappingRef != reference ||
						submissions[0].Properties.IngestionMapping != mapping {
						t.Fatalf("queued routing/fallback did not retain the mapping options: %+v", submissions)
					}
				}
			})
		}
	}
}

func TestMockE2EFileCleanupOnUploadFailure(t *testing.T) {
	for _, managed := range []bool{false, true} {
		for _, empty := range []bool{false, true} {
			t.Run(fmt.Sprintf("managed=%t/empty=%t", managed, empty), func(t *testing.T) {
				cluster := newMockE2ECluster(t, ingestoptions.UploadMethodStorage)
				cluster.rejectUploads = !empty
				client := cluster.newClient(managed)
				payload := []byte("name,age\nAlice,30\n")
				if empty {
					payload = nil
				}
				source := newMockE2ESource(t, true, payload, false)
				result, err := client.Ingest(context.Background(), "db", "table", source,
					&ingestoptions.IngestRequestProperties{IngestionMappingRef: "CSVMapping"})
				if err == nil || result != nil {
					t.Fatalf("expected upload failure, got result=%v err=%v", result, err)
				}
			})
		}
	}
}

func TestMockE2EIngestionLifecycle(t *testing.T) {
	for _, method := range []ingestoptions.UploadMethod{ingestoptions.UploadMethodStorage, ingestoptions.UploadMethodLake} {
		for _, managed := range []bool{false, true} {
			for _, file := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/managed=%t/file=%t", method, managed, file), func(t *testing.T) {
					cluster := newMockE2ECluster(t, method)
					client := cluster.newClient(managed)
					payload := []byte("name,age\nAlice,30\nBob,40\n")
					source := newMockE2ESource(t, file, payload, managed)
					props := &ingestoptions.IngestRequestProperties{
						IngestionMappingRef: "CSVMapping", EnableTracking: true,
						SkipBatching: true, IgnoreFirstRecord: true, Tags: []string{"mock-e2e"},
					}
					ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer cancel()
					response, err := client.Ingest(ctx, "db", "table", source, props)
					if err != nil {
						t.Fatal(err)
					}
					if response.Kind != ingestoptions.IngestKindQueued || response.Response.OperationID != "mock-operation" {
						t.Fatalf("unexpected ingestion response: %+v", response)
					}
					op := &IngestionOperation{Database: "db", Table: "table", OperationID: response.Response.OperationID, IngestKind: response.Kind}
					poller, ok := client.(OperationPoller)
					if !ok {
						t.Fatal("queued and managed clients must support completion polling")
					}
					details, err := poller.PollForCompletion(ctx, op, WithPollingInterval(time.Millisecond))
					if err != nil || details == nil || details.Status == nil || details.Status.Succeeded != 1 || len(details.Details) != 1 {
						t.Fatalf("expected successful operation details, got %+v, %v", details, err)
					}
					if details.Details[0].Status != "Succeeded" || details.Details[0].SourceID != source.SourceID().String() {
						t.Fatalf("operation details did not identify the ingested source: %+v", details.Details)
					}
					if props.Format != ingestoptions.FormatUnknown {
						t.Fatal("ingestion mutated the caller's properties")
					}

					cluster.mu.Lock()
					defer cluster.mu.Unlock()
					if len(cluster.submissions) != 1 {
						t.Fatalf("expected one DM submission, got %d", len(cluster.submissions))
					}
					submitted := cluster.submissions[0]
					if submitted.Properties.Format != "csv" || submitted.Properties.IngestionMappingRef != props.IngestionMappingRef ||
						!submitted.Properties.EnableTracking || !submitted.Properties.SkipBatching || !submitted.Properties.IgnoreFirstRecord ||
						len(submitted.Properties.Tags) != 1 || submitted.Properties.Tags[0] != "mock-e2e" {
						t.Fatalf("ingestion properties were not preserved: %+v", submitted.Properties)
					}
					u, err := url.Parse(submitted.Blobs[0].URL)
					if err != nil {
						t.Fatal(err)
					}
					if submitted.Blobs[0].SourceID != source.SourceID().String() || !strings.HasSuffix(u.Path, "_csv.gz") {
						t.Fatalf("uploaded source metadata was not preserved: %+v", submitted.Blobs[0])
					}
					reader, err := gzip.NewReader(bytes.NewReader(cluster.blobs[u.Path].data))
					if err != nil {
						t.Fatal(err)
					}
					uploaded, err := io.ReadAll(reader)
					_ = reader.Close()
					if err != nil || !bytes.Equal(uploaded, payload) {
						t.Fatalf("uploaded payload differs: %q, %v", uploaded, err)
					}
					wantEvents := []string{"upload-complete", "dm-submit", "status", "status"}
					if managed {
						wantEvents = append([]string{"streaming"}, wantEvents...)
						if len(cluster.streamed) != 1 || !bytes.Equal(cluster.streamed[0], payload) {
							t.Fatal("fallback did not preserve the original streaming payload")
						}
					} else if len(cluster.streamed) != 0 {
						t.Fatal("queued ingestion unexpectedly called the Engine")
					}
					if strings.Join(cluster.events, ",") != strings.Join(wantEvents, ",") {
						t.Fatalf("unexpected ingestion order: %v, want %v", cluster.events, wantEvents)
					}
				})
			}
		}
	}
}

func TestMockE2EIngestionFailures(t *testing.T) {
	for _, managed := range []bool{false, true} {
		for _, uploadFailure := range []bool{false, true} {
			t.Run(fmt.Sprintf("managed=%t/upload-failure=%t", managed, uploadFailure), func(t *testing.T) {
				cluster := newMockE2ECluster(t, ingestoptions.UploadMethodStorage)
				cluster.rejectUploads = uploadFailure
				client := cluster.newClient(managed)
				source := newMockE2ESource(t, false, []byte("name,age\nAlice,30\n"), managed)
				props := &ingestoptions.IngestRequestProperties{
					Format: ingestoptions.FormatCSV, IngestionMappingRef: "MissingMapping", EnableTracking: true,
				}
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				response, err := client.Ingest(ctx, "db", "table", source, props)
				if uploadFailure {
					if err == nil || response != nil {
						t.Fatalf("expected upload failure, got %+v, %v", response, err)
					}
					cluster.mu.Lock()
					defer cluster.mu.Unlock()
					if len(cluster.submissions) != 0 || cluster.statusReads != 0 {
						t.Fatal("upload failure must not submit or track an ingestion")
					}
					return
				}
				if err != nil {
					t.Fatalf("expected asynchronous failure, not immediate rejection: %v", err)
				}
				op := &IngestionOperation{Database: "db", Table: "table", OperationID: response.Response.OperationID, IngestKind: response.Kind}
				summary, err := client.GetOperationSummary(ctx, op)
				if err != nil || summary == nil || summary.InProgress != 1 {
					t.Fatalf("expected pending ingestion, got %+v, %v", summary, err)
				}
				poller, ok := client.(OperationPoller)
				if !ok {
					t.Fatal("queued and managed clients must support completion polling")
				}
				details, err := poller.PollForCompletion(ctx, op, WithPollingInterval(time.Millisecond))
				if err == nil || details == nil || details.Status == nil || details.Status.Failed != 1 || len(details.Details) != 1 {
					t.Fatalf("expected asynchronous failure details, got %+v, %v", details, err)
				}
				failure := details.Details[0]
				if failure.Status != "Failed" || failure.ErrorCode != "MappingNotFound" || failure.Details == "" || failure.FailureStatus != "Permanent" ||
					failure.SourceID != source.SourceID().String() {
					t.Fatalf("failure details were not preserved: %+v", failure)
				}
			})
		}
	}
}

func TestMockE2EStatusLookupFailures(t *testing.T) {
	for _, managed := range []bool{false, true} {
		for _, code := range []int{http.StatusNotFound, http.StatusForbidden} {
			t.Run(fmt.Sprintf("managed=%t/status=%d", managed, code), func(t *testing.T) {
				cluster := newMockE2ECluster(t, ingestoptions.UploadMethodStorage)
				cluster.statusFailure = code
				client := cluster.newClient(managed)
				source := newMockE2ESource(t, false, []byte("name,age\nAlice,30\n"), managed)
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				response, err := client.Ingest(ctx, "db", "table", source, &ingestoptions.IngestRequestProperties{
					Format: ingestoptions.FormatCSV, IngestionMappingRef: "CSVMapping", EnableTracking: true,
				})
				if err != nil {
					t.Fatal(err)
				}
				op := &IngestionOperation{Database: "db", Table: "table", OperationID: response.Response.OperationID, IngestKind: response.Kind}
				summary, summaryErr := client.GetOperationSummary(ctx, op)
				details, detailsErr := client.GetOperationDetails(ctx, op)
				if summary != nil || details != nil {
					t.Fatalf("status lookup failure returned success-shaped results: %+v, %+v", summary, details)
				}
				for _, err := range []error{summaryErr, detailsErr} {
					if err == nil || !strings.Contains(err.Error(), "Unable to retrieve operation status") {
						t.Fatalf("status lookup error was not propagated: %v", err)
					}
					if code == http.StatusNotFound {
						var transient *ingestoptions.IngestServiceError
						if !errors.As(err, &transient) || transient.IsPermanent {
							t.Fatalf("expected a non-permanent status lookup failure, got %v", err)
						}
					} else {
						var permanent *ingestoptions.IngestRequestError
						if !errors.As(err, &permanent) || !permanent.IsPermanent {
							t.Fatalf("expected a permanent status lookup failure, got %v", err)
						}
					}
				}
				cluster.mu.Lock()
				defer cluster.mu.Unlock()
				if cluster.statusReads != 2 || len(cluster.submissions) != 1 {
					t.Fatalf("status failure unexpectedly retried or resubmitted ingestion: reads=%d, submissions=%d",
						cluster.statusReads, len(cluster.submissions))
				}
			})
		}
	}
}
