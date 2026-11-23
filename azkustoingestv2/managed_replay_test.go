// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package azkustoingestv2

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/iotest"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/policy"
)

type managedReplayPolicy struct {
	policy.ManagedStreamingPolicy
	sources []ingestoptions.IngestionSource
}

func (p *managedReplayPolicy) StreamingErrorCallback(source ingestoptions.IngestionSource, database, table string, details policy.ManagedStreamingRequestFailureDetails) {
	p.sources = append(p.sources, source)
	p.ManagedStreamingPolicy.StreamingErrorCallback(source, database, table, details)
}

func (p *managedReplayPolicy) StreamingSuccessCallback(source ingestoptions.IngestionSource, database, table string, details policy.ManagedStreamingRequestSuccessDetails) {
	p.sources = append(p.sources, source)
	p.ManagedStreamingPolicy.StreamingSuccessCallback(source, database, table, details)
}

func TestManagedStreamingReplaysPayload(t *testing.T) {
	for _, tc := range []struct {
		name         string
		file         bool
		offset       bool
		failures     int
		status       int
		queued       bool
		large        bool
		compressed   bool
		dontCompress bool
	}{
		{name: "non-seekable-retry", failures: 1, status: http.StatusInternalServerError},
		{name: "non-seekable-retry-exhaustion", failures: 2, status: http.StatusInternalServerError, queued: true},
		{name: "file-retry-exhaustion", file: true, failures: 2, status: http.StatusInternalServerError, queued: true},
		{name: "file-retry", file: true, failures: 1, status: http.StatusInternalServerError},
		{name: "initial-stream-offset", offset: true, failures: 2, status: http.StatusInternalServerError, queued: true},
		{name: "initial-file-offset", file: true, offset: true, failures: 2, status: http.StatusInternalServerError, queued: true},
		{name: "already-compressed-fallback", compressed: true, failures: 2, status: http.StatusInternalServerError, queued: true},
		{name: "dont-compress-fallback", dontCompress: true, failures: 2, status: http.StatusInternalServerError, queued: true},
		{name: "large-non-seekable-prefix", large: true, queued: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := []byte("name\nAlice\n")
			if tc.large {
				payload = bytes.Repeat([]byte("x"), int(ingestoptions.StreamingMaxRequestBodySize)+512)
			}
			input := payload
			if tc.compressed {
				var compressed bytes.Buffer
				writer := gzip.NewWriter(&compressed)
				if _, err := writer.Write(payload); err != nil {
					t.Fatal(err)
				}
				if err := writer.Close(); err != nil {
					t.Fatal(err)
				}
				input = compressed.Bytes()
			}
			const prefix = "skip these bytes\n"
			if tc.offset {
				input = append([]byte(prefix), input...)
			}
			var source ingestoptions.LocalSource
			if tc.file {
				path := filepath.Join(t.TempDir(), "data.csv")
				if err := os.WriteFile(path, input, 0600); err != nil {
					t.Fatal(err)
				}
				source = ingestoptions.NewFileSource(path, ingestoptions.FormatUnknown)
			} else {
				var reader io.ReadCloser = io.NopCloser(bytes.NewReader(input))
				if tc.offset {
					reader = managedReplayReader{bytes.NewReader(input)}
				}
				var opts []ingestoptions.StreamSourceOption
				if tc.compressed {
					opts = append(opts, ingestoptions.WithStreamCompression(ingestoptions.CompressionGZip))
				}
				if tc.dontCompress {
					opts = append(opts, ingestoptions.DontCompress())
				}
				source = ingestoptions.NewStreamSource(reader, ingestoptions.FormatUnknown, opts...)
			}
			var fileHandle *os.File
			if tc.file {
				stream, err := source.Data()
				if err != nil {
					t.Fatal(err)
				}
				fileHandle = stream.(*os.File)
			}
			t.Cleanup(func() { _ = source.Close() })
			if tc.offset {
				reader, err := source.Data()
				if err != nil {
					t.Fatal(err)
				}
				if _, err := reader.(io.Seeker).Seek(int64(len(prefix)), io.SeekStart); err != nil {
					t.Fatal(err)
				}
			}

			var mu sync.Mutex
			var attempts, submissions int
			var uploaded []byte
			var server *httptest.Server
			server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				switch {
				case strings.HasPrefix(r.URL.Path, "/v1/rest/ingest/"):
					attempts++
					var body io.Reader = r.Body
					if r.Header.Get("Content-Encoding") != "gzip" {
						if !tc.dontCompress {
							t.Error("streaming request is missing gzip encoding")
						}
					} else {
						reader, err := gzip.NewReader(r.Body)
						if err != nil {
							t.Error(err)
							w.WriteHeader(http.StatusBadRequest)
							return
						}
						defer reader.Close()
						body = reader
					}
					data, err := io.ReadAll(body)
					if err != nil || !bytes.Equal(data, payload) {
						t.Errorf("streaming attempt %d did not contain the complete payload: size=%d, err=%v", attempts, len(data), err)
					}
					if r.URL.Query().Get("streamFormat") != "csv" {
						t.Error("streaming format was not preserved")
					}
					w.Header().Set("Content-Type", "application/json")
					if attempts <= tc.failures {
						w.WriteHeader(tc.status)
						_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
							"code": "Failure", "message": "temporary streaming failure",
							"@permanent": false,
						}})
					} else {
						_, _ = io.WriteString(w, `{}`)
					}
				case r.URL.Path == "/v1/rest/ingestion/configuration":
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(map[string]any{
						"containerSettings": map[string]any{
							"lakeFolders": []map[string]string{{"path": server.URL + "/workspace/lakehouse/Files/Ingestions"}},
						},
						"ingestionSettings": map[string]int{"maxBlobsPerBatch": 70},
					})
				case r.URL.Path == "/v1/rest/ingestion/queued/db/table":
					submissions++
					var request ingestRequest
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
						t.Error(err)
					}
					if request.Properties.Format != "csv" || len(request.Blobs) != 1 {
						t.Errorf("unexpected DM request: %+v", request)
					} else if request.Blobs[0].SourceID != source.SourceID().String() {
						t.Error("source ID was changed")
					}
					if len(uploaded) == 0 {
						t.Error("DM API was called before upload")
					}
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, `{"ingestionOperationId":"fallback-operation"}`)
				case r.URL.Query().Get("action") == "append":
					data, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
					}
					uploaded = append(uploaded, data...)
					w.WriteHeader(http.StatusAccepted)
				case r.URL.Query().Get("action") == "flush":
					w.WriteHeader(http.StatusOK)
				case r.URL.Query().Get("resource") == "file":
					w.WriteHeader(http.StatusCreated)
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer server.Close()
			p := &managedReplayPolicy{ManagedStreamingPolicy: policy.NewDefaultManagedStreamingPolicy(
				policy.WithPolicyRetryPolicy(ingestoptions.NewSimpleRetryPolicy(0, 2)),
			)}
			client, err := NewManagedStreamingClient(server.URL, server.URL,
				WithHTTPClient(server.Client()), withManagedStreamingPolicy(p))
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			t.Cleanup(client.queuedClient.configCache.Close)
			response, err := client.Ingest(context.Background(), "db", "table", source, nil)
			if err != nil {
				t.Fatal(err)
			}
			if fileHandle != nil {
				var b [1]byte
				if _, err := fileHandle.Read(b[:]); !errors.Is(err, os.ErrClosed) {
					t.Fatalf("managed ingestion did not close its file after retries/fallback: %v", err)
				}
			}
			mu.Lock()
			defer mu.Unlock()
			wantAttempts := tc.failures
			wantKind := ingestoptions.IngestKindQueued
			if !tc.queued {
				wantAttempts++
				wantKind = ingestoptions.IngestKindStreaming
			}
			if attempts != wantAttempts || response.Kind != wantKind {
				t.Fatalf("got %d attempts and kind %v; want %d and %v", attempts, response.Kind, wantAttempts, wantKind)
			}
			for _, callbackSource := range p.sources {
				if callbackSource != source {
					t.Error("policy callback did not receive the original source")
				}
			}
			if !tc.queued {
				if submissions != 0 || len(uploaded) != 0 {
					t.Fatal("successful streaming unexpectedly used the queued path")
				}
				return
			}
			if submissions != 1 || response.Response.OperationID != "fallback-operation" {
				t.Fatalf("expected one successful DM submission, got %d and %+v", submissions, response)
			}
			got := uploaded
			if !tc.dontCompress {
				reader, err := gzip.NewReader(bytes.NewReader(uploaded))
				if err != nil {
					t.Fatal(err)
				}
				got, err = io.ReadAll(reader)
				_ = reader.Close()
				if err != nil {
					t.Fatal(err)
				}
			}
			if !bytes.Equal(got, payload) {
				t.Fatalf("fallback upload differs: got %d bytes, want %d", len(got), len(payload))
			}
		})
	}
}

func TestManagedStreamingReadFailureDoesNotSubmit(t *testing.T) {
	want := errors.New("source read failed")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("read failure must not submit a request: %s", r.URL)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	client, err := NewManagedStreamingClient(server.URL, server.URL)
	if err != nil {
		t.Fatal(err)
	}

	defer client.Close()
	t.Cleanup(client.queuedClient.configCache.Close)
	source := ingestoptions.NewStreamSource(io.NopCloser(iotest.ErrReader(want)), ingestoptions.FormatCSV)
	_, err = client.Ingest(context.Background(), "db", "table", source, nil)
	if !errors.Is(err, want) {
		t.Fatalf("expected source read failure, got %v", err)
	}
}

func TestManagedStreamingCancellationDoesNotFallBack(t *testing.T) {
	for _, tc := range []struct {
		name      string
		file      bool
		leaveOpen bool
	}{
		{name: "file", file: true},
		{name: "stream-close"},
		{name: "stream-open", leaveOpen: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !strings.HasPrefix(r.URL.Path, "/v1/rest/ingest/") {
					t.Errorf("canceled ingestion must not use the queued path: %s", r.URL)
				}
				_, _ = io.Copy(io.Discard, r.Body)
				cancel()
				w.WriteHeader(http.StatusInternalServerError)
			}))
			defer server.Close()
			client, err := NewManagedStreamingClient(server.URL, server.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			t.Cleanup(client.queuedClient.configCache.Close)
			source := newMockE2ESource(t, tc.file, []byte("name\nAlice\n"), tc.leaveOpen)
			_, err = client.Ingest(ctx, "db", "table", source, nil)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("expected context cancellation, got %v", err)
			}
		})
	}
}
