// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package azkustoingestv2

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"testing/iotest"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/httpclient"
)

type cleanupResponseBody struct {
	io.Reader
	closes atomic.Int32
}

func (b *cleanupResponseBody) Close() error {
	b.closes.Add(1)
	return nil
}

type cleanupTransport struct {
	body   *cleanupResponseBody
	status int
}

func (t cleanupTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: t.status,
		Header:     make(http.Header),
		Body:       t.body,
		Request:    r,
	}, nil
}

func TestAPIResourceNamesAreEscaped(t *testing.T) {
	for _, endpoint := range []struct {
		name    string
		path    string
		success string
		invoke  func(*APIClient) error
	}{
		{
			name:    "queued",
			path:    "/v1/rest/ingestion/queued/db%20%23%3F/events%20%2F%20100%25",
			success: `{"ingestionOperationId":"operation"}`,
			invoke: func(api *APIClient) error {
				_, err := api.PostQueuedIngest(context.Background(), "db #?", "events / 100%", &ingestRequest{})
				return err
			},
		},
		{
			name:    "status",
			path:    "/v1/rest/ingestion/queued/db%20%23%3F/events%20%2F%20100%25/operation%2F%23%3F",
			success: `{"status":{"succeeded":1}}`,
			invoke: func(api *APIClient) error {
				_, err := api.GetIngestStatus(context.Background(), "db #?", "events / 100%", "operation/#?", true)
				return err
			},
		},
		{
			name: "streaming",
			path: "/v1/rest/ingest/db%20%23%3F/events%20%2F%20100%25",
			invoke: func(api *APIClient) error {
				_, err := api.PostStreamingIngest(context.Background(), "db #?", "events / 100%",
					strings.NewReader("data"), "application/octet-stream", "csv", "mapping #?", "", ingestoptions.CompressionNone)
				return err
			},
		},
	} {
		for _, trailingSlash := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/trailing-slash=%t", endpoint.name, trailingSlash), func(t *testing.T) {
				var calls int
				base := httpclient.NewBaseClient(nil, nil, httpclient.WithHTTPClient(&http.Client{
					Transport: endpointCorrectionTransport(func(r *http.Request) (*http.Response, error) {
						calls++
						if r.URL.EscapedPath() != endpoint.path || r.URL.Fragment != "" {
							t.Errorf("resource names changed the URL: %s", r.URL)
						}
						if endpoint.name == "streaming" &&
							(r.URL.Query().Get("streamFormat") != "csv" || r.URL.Query().Get("mappingName") != "mapping #?") {
							t.Errorf("streaming query parameters changed: %s", r.URL.RawQuery)
						}
						if endpoint.name == "status" && r.URL.Query().Get("details") != "true" {
							t.Errorf("status query parameters changed: %s", r.URL.RawQuery)
						}
						return &http.Response{
							StatusCode: http.StatusOK,
							Header:     make(http.Header),
							Body:       io.NopCloser(strings.NewReader(endpoint.success)),
							Request:    r,
						}, nil
					}),
				}))
				url := "https://cluster.kusto.windows.net"
				if trailingSlash {
					url += "/"
				}
				if err := endpoint.invoke(NewAPIClient(url, url, base)); err != nil {
					t.Fatal(err)
				}
				if calls != 1 {
					t.Fatalf("expected one HTTP request, got %d", calls)
				}
			})
		}
	}
}

func TestAPIClosesResponseBodiesExactlyOnce(t *testing.T) {
	for _, endpoint := range []struct {
		name    string
		success string
		invoke  func(*APIClient) error
	}{
		{
			"queued", `{"ingestionOperationId":"operation"}`,
			func(api *APIClient) error {
				_, err := api.PostQueuedIngest(context.Background(), "db", "table", &ingestRequest{})
				return err
			},
		},
		{
			"status", `{"status":{"succeeded":1}}`,
			func(api *APIClient) error {
				_, err := api.GetIngestStatus(context.Background(), "db", "table", "operation", true)
				return err
			},
		},
		{
			"streaming", "",
			func(api *APIClient) error {
				_, err := api.PostStreamingIngest(context.Background(), "db", "table",
					strings.NewReader("data"), "application/octet-stream", "csv", "", "", ingestoptions.CompressionNone)
				return err
			},
		},
	} {
		for _, tc := range []struct {
			name      string
			status    int
			reader    io.Reader
			wantError bool
		}{
			{"success", 200, strings.NewReader(endpoint.success), false},
			{"one-api-error", 400, strings.NewReader(`{"error":{"code":"BadRequest","message":"Rejected"}}`), true},
			{"large-error", 500, strings.NewReader(strings.Repeat("x", 16384)), true},
			{"error-read-failure", 503, iotest.ErrReader(io.ErrUnexpectedEOF), true},
			{"unexpected-success-status", 202, strings.NewReader(endpoint.success), true},
		} {
			t.Run(fmt.Sprintf("%s/%s", endpoint.name, tc.name), func(t *testing.T) {
				body := &cleanupResponseBody{Reader: tc.reader}
				base := httpclient.NewBaseClient(nil, nil, httpclient.WithHTTPClient(&http.Client{
					Transport: cleanupTransport{body: body, status: tc.status},
				}))
				api := NewAPIClient("https://ingest-cluster.kusto.windows.net", "https://cluster.kusto.windows.net", base)
				err := endpoint.invoke(api)
				if (err != nil) != tc.wantError {
					t.Fatalf("unexpected API result: %v", err)
				}
				if body.closes.Load() != 1 {
					t.Fatalf("response body closed %d times, want exactly once", body.closes.Load())
				}
			})
		}
		if endpoint.name != "streaming" {
			t.Run(endpoint.name+"/invalid-success-json", func(t *testing.T) {
				body := &cleanupResponseBody{Reader: strings.NewReader("{")}
				base := httpclient.NewBaseClient(nil, nil, httpclient.WithHTTPClient(&http.Client{
					Transport: cleanupTransport{body: body, status: http.StatusOK},
				}))
				if err := endpoint.invoke(NewAPIClient("https://ingest-cluster.kusto.windows.net", "", base)); err == nil || body.closes.Load() != 1 {
					t.Fatalf("JSON failure did not close the body exactly once: error=%v, closes=%d", err, body.closes.Load())
				}
			})
		}
	}
}
