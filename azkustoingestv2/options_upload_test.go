// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package azkustoingestv2

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	azpolicy "github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/policy"
)

type oneLakeCredential struct{}

func (oneLakeCredential) GetToken(context.Context, azpolicy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: "storage-token", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

func TestClientUploadsToOneLakeBeforeQueuedIngestion(t *testing.T) {
	for _, tc := range []struct {
		name      string
		managed   bool
		extension string
		source    ingestoptions.DataFormat
		props     *ingestoptions.IngestRequestProperties
		format    ingestoptions.DataFormat
		compress  bool
	}{
		{"queued", false, "csv", ingestoptions.FormatCSV, &ingestoptions.IngestRequestProperties{Format: ingestoptions.FormatCSV}, ingestoptions.FormatCSV, true},
		{"managed", true, "csv", ingestoptions.FormatCSV, &ingestoptions.IngestRequestProperties{Format: ingestoptions.FormatCSV}, ingestoptions.FormatCSV, true},
		{"queued-inferred-binary", false, "parquet", ingestoptions.FormatUnknown, nil, ingestoptions.FormatParquet, false},
		{"managed-inferred-binary", true, "parquet", ingestoptions.FormatUnknown, nil, ingestoptions.FormatParquet, false},
		{"request-overrides-path", false, "csv", ingestoptions.FormatUnknown, &ingestoptions.IngestRequestProperties{Format: ingestoptions.FormatParquet}, ingestoptions.FormatParquet, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var uploaded []byte
			var submitted bool
			payload := []byte("name\nAlice\n")
			fileName := filepath.Join(t.TempDir(), "data."+tc.extension)
			if err := os.WriteFile(fileName, payload, 0600); err != nil {
				t.Fatal(err)
			}
			source := ingestoptions.NewFileSource(fileName, tc.source)
			t.Cleanup(func() { _ = source.Close() })

			var server *httptest.Server
			server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				if r.Header.Get(ingestoptions.HeaderMSS2SActorAuthorization) != "Bearer actor-token" ||
					r.Header.Get(ingestoptions.HeaderMSFabricS2SAccessContext) != "workspace-context" {
					t.Error("S2S options were not propagated")
				}
				if strings.HasPrefix(r.URL.Path, "/v1/") {
					if r.Header.Get("Authorization") != "Bearer kusto-token" {
						t.Error("wrong Kusto credential")
					}
				} else if r.Header.Get("Authorization") != "Bearer storage-token" {
					t.Error("wrong OneLake credential")
				}
				switch {
				case r.URL.Path == "/v1/rest/ingestion/configuration":
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(map[string]any{
						"containerSettings": map[string]any{
							"lakeFolders": []map[string]string{{"path": server.URL + "/workspace/lakehouse/Files/Ingestions"}},
						},
						"ingestionSettings": map[string]int{"maxBlobsPerBatch": 70},
					})
				case r.URL.Path == "/v1/rest/ingestion/queued/db/table":
					if len(uploaded) == 0 {
						t.Error("ingestion submitted before data was uploaded")
					}
					var request ingestRequest
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
						t.Error(err)
					}
					if request.Properties.Format != tc.format.String() {
						t.Errorf("request format = %q, want %q", request.Properties.Format, tc.format)
					}
					if len(request.Blobs) != 1 {
						t.Errorf("unexpected blobs: %+v", request.Blobs)
					} else {
						blob := request.Blobs[0]
						if blob.SourceID != source.SourceID().String() || blob.RawSize != int64(len(payload)) ||
							!strings.Contains(blob.URL, "/workspace/lakehouse/Files/Ingestions/") {
							t.Errorf("incorrect uploaded source metadata: %+v", blob)
						}
						suffix := "_" + tc.format.String()
						if tc.compress {
							suffix += ".gz"
						}
						if u, err := url.Parse(blob.URL); err != nil || !strings.HasSuffix(u.Path, suffix) {
							t.Errorf("upload name does not reflect effective format/compression: %s", blob.URL)
						}
					}
					submitted = true
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, `{"ingestionOperationId":"operation-id"}`)
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
					t.Errorf("unexpected request %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer server.Close()
			opts := []ClientOption{
				WithHTTPClient(server.Client()),
				WithTokenProvider(func(context.Context) (string, error) { return "kusto-token", nil }),
				WithUploadTokenCredential(oneLakeCredential{}),
				WithS2STokenProvider(func(context.Context) (*ingestoptions.S2SToken, error) {
					return &ingestoptions.S2SToken{Scheme: "Bearer", Token: "actor-token"}, nil
				}),
				WithS2SFabricAccessContext("workspace-context"),
			}
			var client IngestClient
			if tc.managed {
				p := policy.NewDefaultManagedStreamingPolicy()
				p.StreamingErrorCallback(source, "db", "table", policy.ManagedStreamingRequestFailureDetails{
					ErrorCategory: policy.ErrorCategoryTableConfigurationPreventsStreaming,
				})
				managedClient, err := NewManagedStreamingClient(server.URL, server.URL, append(opts, withManagedStreamingPolicy(p))...)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(managedClient.queuedClient.configCache.Close)
				client = managedClient
			} else {
				queuedClient, err := NewQueuedClient(server.URL, opts...)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(queuedClient.configCache.Close)
				client = queuedClient
			}
			t.Cleanup(func() { _ = client.Close() })
			response, err := client.Ingest(context.Background(), "db", "table", source, tc.props)
			if err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			if !submitted || response.Response.OperationID != "operation-id" || response.Kind != ingestoptions.IngestKindQueued {
				t.Fatalf("unexpected ingestion result: %+v", response)
			}
			if source.Format() != tc.source {
				t.Fatal("source format was mutated")
			}
			got := uploaded
			if tc.compress {
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
				t.Fatalf("uploaded data differs: %q", got)
			}
		})
	}
}
