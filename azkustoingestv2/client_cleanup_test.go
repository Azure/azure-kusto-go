// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package azkustoingestv2

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/config"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/policy"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/upload"
)

func TestStreamingSourceOwnership(t *testing.T) {
	for _, fail := range []bool{false, true} {
		for _, kind := range []string{"file", "stream", "leave-open-stream"} {
			t.Run(fmt.Sprintf("%s/fail=%t", kind, fail), func(t *testing.T) {
				payload := []byte("name,age\nAlice,30\n")
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					reader, err := gzip.NewReader(r.Body)
					if err != nil {
						t.Error(err)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					defer reader.Close()
					data, err := io.ReadAll(reader)
					if err != nil || !bytes.Equal(data, payload) {
						t.Errorf("streaming payload changed: data=%q err=%v", data, err)
					}
					if fail {
						w.WriteHeader(http.StatusForbidden)
						return
					}
					w.WriteHeader(http.StatusOK)
				}))
				defer server.Close()
				client, err := NewStreamingClient(server.URL)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = client.Close() })
				var source ingestoptions.LocalSource
				var input *mockE2EStream
				if kind == "file" {
					source = newMockE2ESource(t, true, payload, false)
				} else {
					input = &mockE2EStream{Reader: bytes.NewReader(payload)}
					source = ingestoptions.NewStreamSource(input, ingestoptions.FormatCSV,
						ingestoptions.WithLeaveOpen(kind == "leave-open-stream"))
					t.Cleanup(func() { _ = input.Close() })
				}
				attempts := 1
				if kind == "file" && !fail {
					attempts = 2
				}
				for range attempts {
					stream, err := source.Data()
					if err != nil {
						t.Fatal(err)
					}
					result, err := client.Ingest(context.Background(), "db", "table", source, nil)
					if fail {
						if err == nil || result != nil {
							t.Fatalf("expected streaming failure, got result=%v err=%v", result, err)
						}
					} else if err != nil {
						t.Fatal(err)
					}
					if handle, ok := stream.(*os.File); ok {
						var b [1]byte
						if _, err := handle.Read(b[:]); !errors.Is(err, os.ErrClosed) {
							t.Fatalf("file was not closed after ingestion: %v", err)
						}
					} else {
						var wantCloses int32
						if kind == "stream" {
							wantCloses = 1
						}
						if input.closes.Load() != wantCloses {
							t.Fatalf("stream closes after ingestion=%d, want %d", input.closes.Load(), wantCloses)
						}
						if err := source.Close(); err != nil {
							t.Fatal(err)
						}
						if input.closes.Load() != wantCloses {
							t.Fatalf("stream closes=%d, want %d", input.closes.Load(), wantCloses)
						}
					}
				}
			})
		}
	}
}

func TestManagedStreamingSourceOwnership(t *testing.T) {
	for _, tc := range []struct {
		name        string
		streaming   bool
		queued      bool
		large       bool
		readFailure bool
		fail        bool
		status      int
		failures    int
	}{
		{name: "streaming-success", streaming: true, status: http.StatusOK},
		{name: "retry-success", streaming: true, status: http.StatusOK, failures: 1},
		{name: "permanent-streaming-failure", streaming: true, status: http.StatusForbidden, fail: true},
		{name: "fallback", streaming: true},
		{name: "fallback-failure", streaming: true, fail: true},
		{name: "queued-policy", queued: true},
		{name: "large-queued", large: true},
		{name: "read-failure", readFailure: true, fail: true},
	} {
		for _, leaveOpen := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/leave-open=%t", tc.name, leaveOpen), func(t *testing.T) {
				cluster := newMockE2ECluster(t, ingestoptions.UploadMethodStorage)
				cluster.rejectUploads = tc.fail
				cluster.streamingStatus, cluster.streamingFailures = tc.status, tc.failures
				payload := []byte("name,age\nAlice,30\n")
				if tc.large {
					payload = bytes.Repeat([]byte("x"), int(ingestoptions.StreamingMaxRequestBodySize)+512)
				}
				var reader io.Reader = bytes.NewReader(payload)
				if tc.readFailure {
					reader = iotest.ErrReader(errors.New("source read failed"))
				}
				input := &mockE2EStream{Reader: reader}
				t.Cleanup(func() { _ = input.Close() })
				source := ingestoptions.NewStreamSource(input, ingestoptions.FormatCSV,
					ingestoptions.WithLeaveOpen(leaveOpen))
				httpClient := &http.Client{Transport: endpointCorrectionTransport(func(r *http.Request) (*http.Response, error) {
					if r.Method != http.MethodGet && input.closes.Load() != 0 {
						t.Errorf("reader closed before %s %s completed", r.Method, r.URL.Path)
					}
					return http.DefaultTransport.RoundTrip(r)
				})}
				client := cluster.newClient(true, WithHTTPClient(httpClient)).(*ManagedStreamingIngestClient)
				client.policy = policy.NewDefaultManagedStreamingPolicy(
					policy.WithPolicyRetryPolicy(ingestoptions.NewSimpleRetryPolicy(0, 2)))
				if tc.queued {
					client.policy.StreamingErrorCallback(source, "db", "table",
						policy.ManagedStreamingRequestFailureDetails{
							ErrorCategory: policy.ErrorCategoryTableConfigurationPreventsStreaming,
						})
				}
				result, err := client.Ingest(context.Background(), "db", "table", source,
					&ingestoptions.IngestRequestProperties{
						IngestionMappingRef: "CSVMapping",
						IngestionMapping:    []ingestoptions.ColumnMapping{{Name: "name"}},
					})
				if tc.fail {
					if err == nil || result != nil {
						t.Fatalf("expected ingestion failure, got result=%v err=%v", result, err)
					}
				} else {
					wantKind := ingestoptions.IngestKindQueued
					if tc.status == http.StatusOK {
						wantKind = ingestoptions.IngestKindStreaming
					}
					if err != nil || result == nil || result.Kind != wantKind {
						t.Fatalf("expected ingestion kind %v, got result=%v err=%v", wantKind, result, err)
					}
				}
				var wantCloses int32
				if !leaveOpen {
					wantCloses = 1
				}
				if input.closes.Load() != wantCloses {
					t.Fatalf("stream closes=%d, want %d", input.closes.Load(), wantCloses)
				}
				cluster.mu.Lock()
				attempts := len(cluster.streamed)
				submissions := len(cluster.submissions)
				cluster.mu.Unlock()
				wantAttempts := 0
				if tc.streaming {
					wantAttempts = 2
					if tc.status == http.StatusOK {
						wantAttempts = tc.failures + 1
					} else if tc.status == http.StatusForbidden {
						wantAttempts = 1
					}
				}
				if attempts != wantAttempts {
					t.Fatalf("streaming attempts=%d, want %d", attempts, wantAttempts)
				}
				if (tc.status != 0 || tc.readFailure) && submissions != 0 {
					t.Fatal("streaming success or permanent/read failure submitted a queued request")
				}
			})
		}
	}
}

func TestQueuedSourceOwnership(t *testing.T) {
	for _, method := range []ingestoptions.UploadMethod{ingestoptions.UploadMethodStorage, ingestoptions.UploadMethodLake} {
		for _, scenario := range []string{
			"compressed-success", "uncompressed-success", "upload-failure",
			"configuration-failure", "submission-failure", "read-failure",
			"both-mappings-success", "upload-cancellation", "close-failure",
		} {
			for _, leaveOpen := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/leave-open=%t", method, scenario, leaveOpen), func(t *testing.T) {
					cluster := newMockE2ECluster(t, method)
					cluster.rejectUploads = scenario == "upload-failure"
					var reader io.Reader = strings.NewReader("name,age\nAlice,30\n")
					readErr := errors.New("source read failed")
					if scenario == "read-failure" {
						reader = iotest.ErrReader(readErr)
					}
					input := &mockE2EStream{Reader: reader}
					if scenario == "close-failure" {
						input.closeErr = errors.New("source close failed")
					}
					t.Cleanup(func() { _ = input.Close() })
					opts := []ingestoptions.StreamSourceOption{ingestoptions.WithLeaveOpen(leaveOpen)}
					if scenario == "uncompressed-success" {
						opts = append(opts, ingestoptions.DontCompress())
					}
					source := ingestoptions.NewStreamSource(input, ingestoptions.FormatCSV, opts...)
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					httpClient := newOwnershipHTTPClient(cluster, scenario, cancel, func(r *http.Request) {
						if input.closes.Load() != 0 {
							t.Errorf("reader closed before %s %s completed", r.Method, r.URL.Path)
						}
					})
					client := cluster.newClient(false, WithHTTPClient(httpClient)).(*QueuedIngestClient)
					var logs bytes.Buffer
					client.logger = slog.New(slog.NewTextHandler(&logs, nil))
					props := &ingestoptions.IngestRequestProperties{Format: ingestoptions.FormatJSON}
					if scenario == "both-mappings-success" {
						props.IngestionMappingRef = "mapping"
						props.IngestionMapping = []ingestoptions.ColumnMapping{{Name: "name"}}
					}
					result, err := client.Ingest(ctx, "db", "table", source, props)
					wantFailure := scenario != "compressed-success" && scenario != "uncompressed-success" &&
						scenario != "both-mappings-success" && scenario != "close-failure"
					if wantFailure {
						if err == nil || result != nil {
							t.Fatalf("expected ingestion failure, got result=%v err=%v", result, err)
						}
					} else if err != nil || result == nil {
						t.Fatalf("expected queued success, got result=%v err=%v", result, err)
					}
					if scenario == "both-mappings-success" {
						cluster.mu.Lock()
						retained := len(cluster.submissions) == 1 &&
							cluster.submissions[0].Properties.IngestionMappingRef == props.IngestionMappingRef &&
							cluster.submissions[0].Properties.IngestionMapping != ""
						cluster.mu.Unlock()
						if !retained {
							t.Fatal("queued ingestion did not retain both mapping options")
						}
					}
					if scenario == "read-failure" && !errors.Is(err, readErr) {
						t.Fatalf("source read error was not preserved: %v", err)
					}
					if scenario == "upload-cancellation" && !errors.Is(err, context.Canceled) {
						t.Fatalf("cancellation was not preserved: %v", err)
					}
					var wantCloses int32
					if !leaveOpen {
						wantCloses = 1
					}
					if input.closes.Load() != wantCloses {
						t.Fatalf("stream closes=%d, want %d", input.closes.Load(), wantCloses)
					}
					if scenario == "close-failure" && !leaveOpen && !strings.Contains(logs.String(), input.closeErr.Error()) {
						t.Fatal("source close failure was not logged")
					}
					if err := source.Close(); err != nil {
						t.Fatal(err)
					}
					if input.closes.Load() != wantCloses {
						t.Fatal("explicit source.Close changed the close count after ingestion")
					}
				})
			}
		}
	}
}

func newOwnershipHTTPClient(cluster *mockE2ECluster, scenario string, cancel context.CancelFunc, assertOpen func(*http.Request)) *http.Client {
	return &http.Client{Transport: endpointCorrectionTransport(func(r *http.Request) (*http.Response, error) {
		assertOpen(r)
		injectFailure := (scenario == "configuration-failure" && strings.HasSuffix(r.URL.Path, "/configuration")) ||
			(scenario == "submission-failure" && strings.Contains(r.URL.Path, "/ingestion/queued/"))
		injectCancellation := scenario == "upload-cancellation" && strings.HasPrefix(r.URL.String(), cluster.storage.URL)
		if injectFailure || injectCancellation {
			if r.Body != nil {
				_ = r.Body.Close()
			}
			if injectCancellation {
				cancel()
				return nil, context.Canceled
			}
			return &http.Response{
				StatusCode: http.StatusForbidden,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(`{"error":{"code":"Rejected","message":"request rejected","@permanent":true}}`)),
				Request:    r,
			}, nil
		}
		return http.DefaultTransport.RoundTrip(r)
	})}
}

func TestQueuedFileSourceOwnership(t *testing.T) {
	for _, method := range []ingestoptions.UploadMethod{ingestoptions.UploadMethodStorage, ingestoptions.UploadMethodLake} {
		for _, scenario := range []string{
			"compressed-success", "uncompressed-success", "configuration-failure",
			"upload-failure", "submission-failure", "both-mappings-success", "upload-cancellation",
		} {
			t.Run(fmt.Sprintf("%s/%s", method, scenario), func(t *testing.T) {
				cluster := newMockE2ECluster(t, method)
				cluster.rejectUploads = scenario == "upload-failure"
				path := filepath.Join(t.TempDir(), "data.csv")
				if err := os.WriteFile(path, []byte("name,age\nAlice,30\n"), 0600); err != nil {
					t.Fatal(err)
				}
				var opts []ingestoptions.FileSourceOption
				if scenario == "uncompressed-success" {
					opts = append(opts, ingestoptions.DontCompress())
				}
				source := ingestoptions.NewFileSource(path, ingestoptions.FormatCSV, opts...)
				t.Cleanup(func() { _ = source.Close() })
				stream, err := source.Data()
				if err != nil {
					t.Fatal(err)
				}
				handle := stream.(*os.File)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				httpClient := newOwnershipHTTPClient(cluster, scenario, cancel, func(r *http.Request) {
					if _, err := handle.Seek(0, io.SeekCurrent); err != nil {
						t.Errorf("file closed before %s %s completed: %v", r.Method, r.URL.Path, err)
					}
				})
				client := cluster.newClient(false, WithHTTPClient(httpClient))
				props := &ingestoptions.IngestRequestProperties{Format: ingestoptions.FormatJSON}
				if scenario == "both-mappings-success" {
					props.IngestionMappingRef = "mapping"
					props.IngestionMapping = []ingestoptions.ColumnMapping{{Name: "name"}}
				}
				result, err := client.Ingest(ctx, "db", "table", source, props)
				if scenario == "compressed-success" || scenario == "uncompressed-success" || scenario == "both-mappings-success" {
					if err != nil || result == nil {
						t.Fatalf("expected queued success, got result=%v err=%v", result, err)
					}
				} else if err == nil || result != nil {
					t.Fatalf("expected ingestion failure, got result=%v err=%v", result, err)
				}
				if scenario == "both-mappings-success" {
					cluster.mu.Lock()
					retained := len(cluster.submissions) == 1 &&
						cluster.submissions[0].Properties.IngestionMappingRef == props.IngestionMappingRef &&
						cluster.submissions[0].Properties.IngestionMapping != ""
					cluster.mu.Unlock()
					if !retained {
						t.Fatal("queued ingestion did not retain both mapping options")
					}
				}
				if scenario == "upload-cancellation" && !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation was not preserved: %v", err)
				}
				var b [1]byte
				if _, err := handle.Read(b[:]); !errors.Is(err, os.ErrClosed) {
					t.Fatalf("SDK did not close its file: %v", err)
				}
				if _, err := os.Stat(path); err != nil {
					t.Fatalf("source file was removed or made inaccessible: %v", err)
				}
				if err := source.Close(); err != nil {
					t.Fatalf("explicit close after ingestion failed: %v", err)
				}
			})
		}
	}
}

type cleanupCache struct {
	config.ConfigurationCache
	closes atomic.Int32
}

func (c *cleanupCache) Close() {
	c.closes.Add(1)
	if c.ConfigurationCache != nil {
		c.ConfigurationCache.Close()
	}
}

type cleanupUploader struct {
	upload.Uploader
	closes atomic.Int32
	err    error
}

func (u *cleanupUploader) Close() error {
	u.closes.Add(1)
	if u.Uploader != nil {
		if err := u.Uploader.Close(); err != nil {
			return errors.Join(err, u.err)
		}
	}
	return u.err
}

func TestFactoryClientCloseReleasesOwnedResources(t *testing.T) {
	for _, managed := range []bool{false, true} {
		name := "queued"
		if managed {
			name = "managed"
		}
		t.Run(name, func(t *testing.T) {
			var client IngestClient
			var queued *QueuedIngestClient
			if managed {
				c, err := NewManagedStreamingClient("https://ingest-cluster.kusto.windows.net", "https://cluster.kusto.windows.net")
				if err != nil {
					t.Fatal(err)
				}
				client, queued = c, c.queuedClient
			} else {
				c, err := NewQueuedClient("https://ingest-cluster.kusto.windows.net")
				if err != nil {
					t.Fatal(err)
				}
				client, queued = c, c
			}
			t.Cleanup(func() { _ = client.Close() })
			cache := &cleanupCache{ConfigurationCache: queued.configCache}
			closeFailure := errors.New("uploader close failed")
			uploader := &cleanupUploader{Uploader: queued.uploader, err: closeFailure}
			queued.configCache, queued.uploader = cache, uploader
			var wg sync.WaitGroup
			for range 8 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := client.Close(); !errors.Is(err, closeFailure) {
						t.Errorf("close did not retain the uploader error: %v", err)
					}
				}()
			}
			wg.Wait()
			if cache.closes.Load() != 1 || uploader.closes.Load() != 1 {
				t.Fatalf("owned resources were not closed exactly once: cache=%d, uploader=%d",
					cache.closes.Load(), uploader.closes.Load())
			}
		})
	}
}

func TestClientClosePreservesInjectedCacheOwnership(t *testing.T) {
	for _, ownsUploader := range []bool{false, true} {
		name := "caller-owned-uploader"
		if ownsUploader {
			name = "client-owned-uploader"
		}
		t.Run(name, func(t *testing.T) {
			cache := &cleanupCache{}
			uploader := &cleanupUploader{}
			client := newQueuedIngestClient(nil, cache, uploader, ownsUploader)
			for range 2 {
				if err := client.Close(); err != nil {
					t.Fatal(err)
				}
			}
			var wantUploaderCloses int32
			if ownsUploader {
				wantUploaderCloses = 1
			}
			if cache.closes.Load() != 0 || uploader.closes.Load() != wantUploaderCloses {
				t.Fatalf("client closed caller-owned resources: cache=%d, uploader=%d",
					cache.closes.Load(), uploader.closes.Load())
			}
		})
	}
}
