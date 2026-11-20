// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package azkustoingestv2

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/policy"
)

type endpointCorrectionTransport func(*http.Request) (*http.Response, error)

func (f endpointCorrectionTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestFactoriesCorrectEndpointRequests(t *testing.T) {
	const (
		engine = "https://cluster.kusto.windows.net"
		dm     = "https://ingest-cluster.kusto.windows.net"
	)
	for _, factory := range []struct {
		name string
		new  func(string, string, ...ClientOption) (IngestClient, error)
	}{
		{"queued", func(dm, _ string, opts ...ClientOption) (IngestClient, error) {
			return NewQueuedClient(dm, opts...)
		}},
		{"streaming", func(_, engine string, opts ...ClientOption) (IngestClient, error) {
			return NewStreamingClient(engine, opts...)
		}},
		{"managed", func(dm, engine string, opts ...ClientOption) (IngestClient, error) {
			return NewManagedStreamingClient(dm, engine, opts...)
		}},
	} {
		for _, tc := range []struct {
			name       string
			dm, engine string
			opts       []ClientOption
			wantDM     string
			wantEngine string
		}{
			{"correct-roles", engine, dm, nil, dm, engine},
			{"already-correct", dm, engine, nil, dm, engine},
			{"shared-cluster-url", engine, engine, nil, dm, engine},
			{"option-overrides", dm, engine, []ClientOption{
				WithDMURL("https://override.kusto.windows.net"),
				WithEngineURL("https://ingest-override.kusto.windows.net"),
			}, "https://ingest-override.kusto.windows.net", "https://override.kusto.windows.net"},
			{"opt-out", engine, dm, []ClientOption{WithoutEndpointCorrection()}, engine, dm},
			{"trust-bypass-keeps-correction", "https://custom.example", "https://ingest-custom.example",
				[]ClientOption{WithSkipSecurityChecks(true)}, "https://ingest-custom.example", "https://custom.example"},
			{"opt-out-overrides", dm, engine, []ClientOption{
				WithoutEndpointCorrection(),
				WithDMURL("https://override.kusto.windows.net"),
				WithEngineURL("https://ingest-override.kusto.windows.net"),
			}, "https://override.kusto.windows.net", "https://ingest-override.kusto.windows.net"},
		} {
			t.Run(factory.name+"/"+tc.name, func(t *testing.T) {
				var queued, streamed atomic.Bool
				transport := endpointCorrectionTransport(func(r *http.Request) (*http.Response, error) {
					if r.Body != nil {
						defer r.Body.Close()
					}
					status, body, wantURL := http.StatusOK, "", ""
					switch r.URL.Path {
					case "/v1/rest/ingestion/configuration":
						wantURL = tc.wantDM
						body = `{"ingestionSettings":{"maxBlobsPerBatch":70}}`
					case "/v1/rest/ingestion/queued/db/table":
						queued.Store(true)
						wantURL = tc.wantDM
						body = `{"ingestionOperationId":"operation"}`
					case "/v1/rest/ingest/db/table":
						streamed.Store(true)
						wantURL = tc.wantEngine
						if factory.name == "managed" {
							status = http.StatusServiceUnavailable
							body = `{"error":{"code":"ServiceUnavailable","message":"Try later"}}`
						}
					default:
						t.Errorf("unexpected request: %s", r.URL)
						status = http.StatusBadRequest
					}
					if got := r.URL.Scheme + "://" + r.URL.Host; got != wantURL {
						t.Errorf("request endpoint = %q, want %q", got, wantURL)
					}
					return &http.Response{
						StatusCode: status,
						Header:     make(http.Header),
						Body:       io.NopCloser(strings.NewReader(body)),
						Request:    r,
					}, nil
				})
				opts := append([]ClientOption{
					WithHTTPClient(&http.Client{Transport: transport}),
					withManagedStreamingPolicy(policy.NewDefaultManagedStreamingPolicy(
						policy.WithPolicyRetryPolicy(ingestoptions.NewSimpleRetryPolicy(0, 1)),
					)),
				}, tc.opts...)
				client, err := factory.new(tc.dm, tc.engine, opts...)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = client.Close() })
				source, err := ingestoptions.NewBlobSource("https://storage.example/container/data.csv", ingestoptions.FormatCSV)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := client.Ingest(context.Background(), "db", "table", source, nil); err != nil {
					t.Fatal(err)
				}
				if queued.Load() != (factory.name != "streaming") || streamed.Load() != (factory.name != "queued") {
					t.Fatalf("unexpected routes: queued=%v, streaming=%v", queued.Load(), streamed.Load())
				}
			})
		}
	}
}

func TestEndpointCorrectionOptOutStillValidatesTrust(t *testing.T) {
	client, err := NewQueuedClient("https://custom.example", WithoutEndpointCorrection())
	if err == nil {
		_ = client.Close()
		t.Fatal("expected an untrusted endpoint to be rejected")
	}
}

func TestFactoriesValidateEndpoints(t *testing.T) {
	for _, factory := range []struct {
		name string
		new  func(string, ...ClientOption) (IngestClient, error)
	}{
		{"queued", func(endpoint string, opts ...ClientOption) (IngestClient, error) {
			return NewQueuedClient(endpoint, opts...)
		}},
		{"streaming", func(endpoint string, opts ...ClientOption) (IngestClient, error) {
			return NewStreamingClient(endpoint, opts...)
		}},
		{"managed", func(endpoint string, opts ...ClientOption) (IngestClient, error) {
			return NewManagedStreamingClient(endpoint, endpoint, opts...)
		}},
	} {
		for _, tc := range []struct {
			name     string
			endpoint string
			skip     bool
			wantErr  bool
		}{
			{"trusted", "https://cluster.kusto.windows.net", false, false},
			{"local", "http://127.0.0.1:8080", false, false},
			{"untrusted", "https://custom.example", false, true},
			{"explicit-bypass", "https://custom.example", true, false},
		} {
			t.Run(factory.name+"/"+tc.name, func(t *testing.T) {
				client, err := factory.new(tc.endpoint, WithSkipSecurityChecks(tc.skip))
				if err == nil {
					t.Cleanup(func() { _ = client.Close() })
				}
				if (err != nil) != tc.wantErr {
					t.Fatalf("unexpected constructor error: %v", err)
				}
				if tc.wantErr {
					var clientErr *ingestoptions.IngestClientError
					if !errors.As(err, &clientErr) || !clientErr.IsPermanent {
						t.Fatalf("expected permanent client error, got %v", err)
					}
				}
			})
		}
	}
}

func TestManagedFactoryValidatesBothEndpoints(t *testing.T) {
	for _, tc := range []struct {
		name   string
		dm     string
		engine string
		opts   []ClientOption
	}{
		{"dm", "https://custom.example", "https://cluster.kusto.windows.net", nil},
		{"engine", "https://ingest-cluster.kusto.windows.net", "https://custom.example", nil},
		{"override", "https://ingest-cluster.kusto.windows.net", "https://cluster.kusto.windows.net",
			[]ClientOption{WithEngineURL("https://custom.example")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, err := NewManagedStreamingClient(tc.dm, tc.engine, tc.opts...)
			if err == nil {
				_ = client.Close()
				t.Fatal("expected untrusted endpoint to be rejected")
			}
		})
	}
}
