// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package azkustoingestv2

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/policy"
)

func withManagedStreamingPolicy(p policy.ManagedStreamingPolicy) ClientOption {
	return func(c *clientConfig) { c.ManagedStreamingPolicy = p }
}

type managedErrorPolicy struct {
	policy.ManagedStreamingPolicy
	failures []policy.ManagedStreamingRequestFailureDetails
}

func (p *managedErrorPolicy) StreamingErrorCallback(source ingestoptions.IngestionSource, database, table string, details policy.ManagedStreamingRequestFailureDetails) {
	p.failures = append(p.failures, details)
	p.ManagedStreamingPolicy.StreamingErrorCallback(source, database, table, details)
}

func TestManagedStreamingRetriesHTTPTransientErrors(t *testing.T) {
	for _, tc := range []struct {
		name     string
		status   int
		body     string
		category policy.ManagedStreamingErrorCategory
	}{
		{"throttle-without-metadata", 429, `{"error":{"code":"TooManyRequests","message":"maximum allowed request rate exceeded"}}`, policy.ErrorCategoryThrottled},
		{"throttle-with-service-code", 429, `{"error":{"code":"Busy","message":"Try later","@failureCode":"12345"}}`, policy.ErrorCategoryThrottled},
		{"throttle-without-json", 429, "Too many requests", policy.ErrorCategoryThrottled},
		{"server-error-without-json", 500, "Internal Server Error", policy.ErrorCategoryOther},
		{"unavailable-with-conflicting-service-code", 503, `{"error":{"code":"Unavailable","message":"Try later","@failureCode":"429"}}`, policy.ErrorCategoryOther},
		{"request-timeout", 408, "Request timed out", policy.ErrorCategoryOther},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var attempts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/v1/rest/ingest/db/table" {
					t.Errorf("temporary streaming error unexpectedly used another endpoint: %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if _, err := io.Copy(io.Discard, r.Body); err != nil {
					t.Error(err)
				}
				if attempts.Add(1) == 1 {
					w.WriteHeader(tc.status)
					_, _ = io.WriteString(w, tc.body)
					return
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()
			p := &managedErrorPolicy{ManagedStreamingPolicy: policy.NewDefaultManagedStreamingPolicy(
				policy.WithPolicyRetryPolicy(ingestoptions.NewSimpleRetryPolicy(0, 2)),
			)}
			client, err := NewManagedStreamingClient(server.URL, server.URL, withManagedStreamingPolicy(p))
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			t.Cleanup(client.queuedClient.configCache.Close)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			source := ingestoptions.NewStreamSource(io.NopCloser(strings.NewReader("a,b\n1,2\n")), ingestoptions.FormatCSV)
			t.Cleanup(func() { _ = source.Close() })
			response, err := client.Ingest(ctx, "db", "table", source, nil)
			if err != nil {
				t.Fatal(err)
			}
			if attempts.Load() != 2 || response.Kind != ingestoptions.IngestKindStreaming {
				t.Fatalf("expected streaming retry then success, got %d attempts and %+v", attempts.Load(), response)
			}
			if len(p.failures) != 1 || p.failures[0].IsPermanent || p.failures[0].ErrorCategory != tc.category {
				t.Fatalf("incorrect failure policy details: %+v", p.failures)
			}
		})
	}
}

func TestManagedStreamingReturnsPermanentErrorsWithoutFallback(t *testing.T) {
	for _, tc := range []struct {
		name           string
		status         int
		body           string
		firstTransient bool
	}{
		{"forbidden", http.StatusForbidden, "Forbidden", false},
		{"schema", http.StatusBadRequest, `{"error":{"code":"BadRequest","message":"incompatible schema","@permanent":true}}`, false},
		{"streaming-disabled", http.StatusBadRequest, `{"error":{"code":"BadRequest","message":"streaming ingestion is disabled","@permanent":true}}`, false},
		{"payload-too-large", http.StatusRequestEntityTooLarge, "Payload too large", false},
		{"explicit-permanent-service-error", http.StatusServiceUnavailable, `{"error":{"code":"Rejected","@permanent":true}}`, false},
		{"explicit-permanent-throttle", http.StatusTooManyRequests, `{"error":{"code":"Rejected","@permanent":true}}`, false},
		{"transient-then-permanent", http.StatusBadRequest, `{"error":{"code":"BadRequest","message":"incompatible schema","@permanent":true}}`, true},
	} {
		for _, blob := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/blob=%v", tc.name, blob), func(t *testing.T) {
				var attempts, dmRequests atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path != "/v1/rest/ingest/db/table" {
						dmRequests.Add(1)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					if _, err := io.Copy(io.Discard, r.Body); err != nil {
						t.Error(err)
					}
					if attempts.Add(1) == 1 && tc.firstTransient {
						w.WriteHeader(http.StatusTooManyRequests)
						_, _ = io.WriteString(w, "Try later")
						return
					}
					w.WriteHeader(tc.status)
					_, _ = io.WriteString(w, tc.body)
				}))
				defer server.Close()
				p := &managedErrorPolicy{ManagedStreamingPolicy: policy.NewDefaultManagedStreamingPolicy(
					policy.WithContinueWhenUnavailable(true),
					policy.WithPolicyRetryPolicy(ingestoptions.NewSimpleRetryPolicy(0, 3)),
				)}
				client, err := NewManagedStreamingClient(server.URL, server.URL, withManagedStreamingPolicy(p))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = client.Close() })
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				for range 2 {
					var source ingestoptions.IngestionSource
					if blob {
						source, err = ingestoptions.NewBlobSource("https://storage/data.csv", ingestoptions.FormatCSV)
						if err != nil {
							t.Fatal(err)
						}
					} else {
						local := ingestoptions.NewStreamSource(io.NopCloser(strings.NewReader("a,b\n1,2\n")), ingestoptions.FormatCSV)
						t.Cleanup(func() { _ = local.Close() })
						source = local
					}
					response, err := client.Ingest(ctx, "db", "table", source, nil)
					if response != nil || err == nil || !isPermError(err) || extractStatusCode(err) != tc.status {
						t.Fatalf("expected original permanent HTTP %d error, got response=%v err=%v", tc.status, response, err)
					}
					if len(p.failures) == 0 || err != p.failures[len(p.failures)-1].Err {
						t.Fatalf("original streaming error was not returned: %v", err)
					}
					if p.ShouldDefaultToQueuedIngestion(source, "db", "table") {
						t.Fatal("permanent failure caused subsequent requests to bypass streaming")
					}
				}
				wantAttempts := int32(2)
				if tc.firstTransient {
					wantAttempts++
				}
				if attempts.Load() != wantAttempts || dmRequests.Load() != 0 || len(p.failures) != int(wantAttempts) {
					t.Fatalf("expected %d streaming attempts and no DM calls; got streaming=%d DM=%d callbacks=%d",
						wantAttempts, attempts.Load(), dmRequests.Load(), len(p.failures))
				}
			})
		}
	}
}

func TestCategorizeErrorPrefersHTTPStatus(t *testing.T) {
	for _, tc := range []struct {
		name     string
		err      error
		category policy.ManagedStreamingErrorCategory
	}{
		{
			"http-throttle-over-service-code-and-message",
			&ingestoptions.IngestServiceError{IngestError: ingestoptions.IngestError{
				HTTPStatusCode: 429, FailureCode: 413, Message: "maximum allowed requests exceeded",
			}},
			policy.ErrorCategoryThrottled,
		},
		{
			"http-size-over-service-code",
			&ingestoptions.IngestRequestError{IngestError: ingestoptions.IngestError{
				HTTPStatusCode: 413, FailureCode: 429, Message: "Request rejected",
			}},
			policy.ErrorCategoryRequestPropertiesPreventStreaming,
		},
		{
			"legacy-caller-failure-code",
			ingestoptions.NewIngestServiceError("Try later", nil, 429, ""),
			policy.ErrorCategoryThrottled,
		},
		{
			"wrapped-http-status",
			fmt.Errorf("request failed: %w", ingestoptions.NewIngestClientError("wrapper",
				&ingestoptions.IngestError{HTTPStatusCode: 429}, false)),
			policy.ErrorCategoryThrottled,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := categorizeError(tc.err); got != tc.category {
				t.Fatalf("category = %v, want %v", got, tc.category)
			}
		})
	}
}
