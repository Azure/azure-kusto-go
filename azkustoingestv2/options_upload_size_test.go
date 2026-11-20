// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package azkustoingestv2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/policy"
)

func TestUploadSizeFailureDoesNotSubmitQueuedRequest(t *testing.T) {
	for _, method := range []ingestoptions.UploadMethod{ingestoptions.UploadMethodStorage, ingestoptions.UploadMethodLake} {
		for _, managed := range []bool{false, true} {
			for _, tc := range []struct {
				name    string
				payload string
				code    ingestoptions.UploadErrorCode
			}{
				{"empty", "", ingestoptions.UploadErrorSourceIsEmpty},
				{"oversize", strings.Repeat("x", 8193), ingestoptions.UploadErrorSourceSizeLimitExceed},
			} {
				t.Run(fmt.Sprintf("%s/managed=%v/%s", method, managed, tc.name), func(t *testing.T) {
					var submitted atomic.Int32
					var server *httptest.Server
					server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						switch r.URL.Path {
						case "/v1/rest/ingestion/configuration":
							key := "containers"
							if method == ingestoptions.UploadMethodLake {
								key = "lakeFolders"
							}
							w.Header().Set("Content-Type", "application/json")
							_ = json.NewEncoder(w).Encode(map[string]any{
								"containerSettings": map[string]any{
									key: []map[string]string{{"path": server.URL + "/storage"}},
								},
								"ingestionSettings": map[string]int{"maxBlobsPerBatch": 70},
							})
						case "/v1/rest/ingestion/queued/db/table":
							submitted.Add(1)
							_, _ = io.WriteString(w, `{"ingestionOperationId":"unexpected"}`)
						default:
							_, _ = io.Copy(io.Discard, r.Body)
							w.Header().Set("ETag", `"uploaded"`)
							if r.URL.Query().Get("action") == "append" {
								w.WriteHeader(http.StatusAccepted)
							} else if r.URL.Query().Get("action") == "flush" {
								w.WriteHeader(http.StatusOK)
							} else {
								w.WriteHeader(http.StatusCreated)
							}
						}
					}))
					defer server.Close()
					source := ingestoptions.NewStreamSource(io.NopCloser(strings.NewReader(tc.payload)), ingestoptions.FormatCSV)
					t.Cleanup(func() { _ = source.Close() })
					opts := []ClientOption{WithMaxDataSize(8192), WithUploadMethod(method)}
					var client IngestClient
					var err error
					if managed {
						p := policy.NewDefaultManagedStreamingPolicy()
						p.StreamingErrorCallback(source, "db", "table", policy.ManagedStreamingRequestFailureDetails{
							ErrorCategory: policy.ErrorCategoryTableConfigurationPreventsStreaming,
						})
						client, err = NewManagedStreamingClient(server.URL, server.URL,
							append(opts, withManagedStreamingPolicy(p))...)
					} else {
						client, err = NewQueuedClient(server.URL, opts...)
					}
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = client.Close() })
					result, err := client.Ingest(context.Background(), "db", "table", source, nil)
					var uploadErr *ingestoptions.UploadFailedError
					if result != nil || !errors.As(err, &uploadErr) || uploadErr.ErrorCode != tc.code {
						t.Fatalf("expected %s, got result=%v err=%v", tc.code, result, err)
					}
					if submitted.Load() != 0 {
						t.Fatalf("invalid source submitted to DM %d times", submitted.Load())
					}
				})
			}
		}
	}
}
