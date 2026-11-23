// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package azkustoingestv2

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/httpclient"
)

func newPollingTestClient(t *testing.T, managed bool, handler http.HandlerFunc) OperationPoller {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/rest/ingestion/queued/db/table/operation" ||
			r.URL.Query().Get("details") != "true" {
			t.Errorf("unexpected polling request: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		handler(w, r)
	}))
	t.Cleanup(server.Close)
	api := NewAPIClient(server.URL, server.URL, httpclient.NewBaseClient(nil, nil))
	queued := newQueuedIngestClient(api, nil, nil, false)
	if managed {
		return newManagedStreamingIngestClient(NewStreamingIngestClient(api), queued, nil)
	}
	return queued
}

func pollingTestOperation() *IngestionOperation {
	return &IngestionOperation{Database: "db", Table: "table", OperationID: "operation", IngestKind: ingestoptions.IngestKindQueued}
}

func TestPollForCompletion(t *testing.T) {
	succeeded := BlobStatus{SourceID: "source-success", Status: "Succeeded"}
	failed := BlobStatus{
		SourceID: "source-failure", Status: "Failed", ErrorCode: "MappingNotFound",
		FailureStatus: "Permanent", Details: "mapping is missing", Message: "legacy error message",
	}
	for _, managed := range []bool{false, true} {
		name := "queued"
		if managed {
			name = "managed"
		}
		t.Run(name, func(t *testing.T) {
			for _, tc := range []struct {
				name    string
				states  []StatusResponse
				wantErr bool
			}{
				{
					name:   "already-complete",
					states: []StatusResponse{{Status: &OperationStatus{Succeeded: 1}, Details: []BlobStatus{succeeded}}},
				},
				{
					name: "empty-and-unknown-details-stay-pending",
					states: []StatusResponse{
						{Status: &OperationStatus{Succeeded: 1}},
						{Details: []BlobStatus{{Status: "Unknown"}}},
						{Status: &OperationStatus{Succeeded: 1}, Details: []BlobStatus{succeeded}},
					},
				},
				{
					name: "wait-for-all-blobs-before-reporting-failure",
					states: []StatusResponse{
						{Status: &OperationStatus{Failed: 1, InProgress: 1}, Details: []BlobStatus{failed, {Status: "InProgress"}}},
						{Status: &OperationStatus{Failed: 1, Succeeded: 1}, Details: []BlobStatus{failed, succeeded}},
					},
					wantErr: true,
				},
				{
					name:    "failed-detail-without-summary",
					states:  []StatusResponse{{Details: []BlobStatus{failed}}},
					wantErr: true,
				},
				{
					name: "canceled-operation",
					states: []StatusResponse{{
						Status: &OperationStatus{Canceled: 1}, Details: []BlobStatus{{SourceID: "canceled-source", Status: "Canceled"}},
					}},
					wantErr: true,
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					var calls atomic.Int32
					client := newPollingTestClient(t, managed, func(w http.ResponseWriter, r *http.Request) {
						index := min(int(calls.Add(1))-1, len(tc.states)-1)
						w.Header().Set("Content-Type", "application/json")
						_ = json.NewEncoder(w).Encode(tc.states[index])
					})
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					var opts []PollingOption
					if len(tc.states) > 1 {
						opts = append(opts, WithPollingInterval(time.Millisecond))
					}
					result, err := client.PollForCompletion(ctx, pollingTestOperation(), opts...)
					if tc.wantErr {
						var failure *ingestoptions.IngestError
						if !errors.As(err, &failure) || !failure.IsPermanent || !strings.Contains(err.Error(), "operation") {
							t.Fatalf("expected a permanent ingestion failure, got %v", err)
						}
						for _, detail := range tc.states[len(tc.states)-1].Details {
							if detail.Status == "Failed" && (!strings.Contains(err.Error(), detail.SourceID) ||
								!strings.Contains(err.Error(), detail.ErrorCode) || !strings.Contains(err.Error(), detail.Message) ||
								!strings.Contains(err.Error(), detail.Details) || !strings.Contains(err.Error(), detail.FailureStatus)) {
								t.Fatalf("error omitted blob failure details: %v", err)
							}
						}
					} else if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(result, &tc.states[len(tc.states)-1]) {
						t.Fatalf("expected final status %+v, got %+v", tc.states[len(tc.states)-1], result)
					}
					if int(calls.Load()) != len(tc.states) {
						t.Fatalf("polling stopped at the wrong point: %d calls, want %d", calls.Load(), len(tc.states))
					}
				})
			}
		})
	}
}

func TestPollForCompletionStatusErrorStopsPolling(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusForbidden} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var calls atomic.Int32
			client := newPollingTestClient(t, false, func(w http.ResponseWriter, r *http.Request) {
				attempt := calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				if status == http.StatusNotFound && attempt == 1 {
					_, _ = w.Write([]byte(`{"status":{"inProgress":1},"details":[{"status":"InProgress"}]}`))
					return
				}
				w.WriteHeader(status)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
					"code": "LookupFailed", "message": "status unavailable", "@permanent": status != http.StatusNotFound,
				}})
			})
			result, err := client.PollForCompletion(context.Background(), pollingTestOperation(), WithPollingInterval(time.Millisecond))
			wantCalls := int32(1)
			if status == http.StatusNotFound {
				wantCalls = 2
				if result == nil || result.Status == nil || result.Status.InProgress != 1 {
					t.Fatalf("status error did not preserve the previous result: %+v", result)
				}
			} else if result != nil {
				t.Fatalf("first lookup failure returned a status: %+v", result)
			}
			if err == nil || calls.Load() != wantCalls {
				t.Fatalf("expected polling to stop on the failed lookup, got error=%v, calls=%d", err, calls.Load())
			}
			if status == http.StatusNotFound {
				var transient *ingestoptions.IngestServiceError
				if !errors.As(err, &transient) || transient.IsPermanent {
					t.Fatalf("lookup classification was not preserved: %v", err)
				}
			} else {
				var permanent *ingestoptions.IngestRequestError
				if !errors.As(err, &permanent) || !permanent.IsPermanent {
					t.Fatalf("lookup classification was not preserved: %v", err)
				}
			}
		})
	}
}

func TestPollForCompletionTimeout(t *testing.T) {
	for _, parentDeadline := range []bool{false, true} {
		name := "polling-timeout"
		if parentDeadline {
			name = "earlier-context-deadline"
		}
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			pending := StatusResponse{Status: &OperationStatus{InProgress: 1}, Details: []BlobStatus{{Status: "InProgress"}}}
			client := newPollingTestClient(t, false, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(pending)
			})
			parentTimeout, pollingTimeout := time.Second, 200*time.Millisecond
			if parentDeadline {
				parentTimeout, pollingTimeout = pollingTimeout, parentTimeout
			}
			ctx, cancel := context.WithTimeout(context.Background(), parentTimeout)
			defer cancel()
			result, err := client.PollForCompletion(ctx, pollingTestOperation(),
				WithPollingInterval(time.Hour), WithPollingTimeout(pollingTimeout))
			if !errors.Is(err, context.DeadlineExceeded) || !reflect.DeepEqual(result, &pending) || calls.Load() != 1 {
				t.Fatalf("timeout did not interrupt the wait and retain status: result=%+v, error=%v, calls=%d", result, err, calls.Load())
			}
		})
	}
}

func TestPollForCompletionCancellation(t *testing.T) {
	for _, alreadyCanceled := range []bool{false, true} {
		name := "in-flight-request"
		if alreadyCanceled {
			name = "before-first-request"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var calls atomic.Int32
			client := newPollingTestClient(t, false, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				cancel()
				<-r.Context().Done()
			})
			var wantCalls int32 = 1
			if alreadyCanceled {
				cancel()
				wantCalls = 0
			}
			result, err := client.PollForCompletion(ctx, pollingTestOperation())
			if !errors.Is(err, context.Canceled) || result != nil || calls.Load() != wantCalls {
				t.Fatalf("cancellation did not stop polling: result=%+v, error=%v, calls=%d", result, err, calls.Load())
			}
		})
	}
}

func TestPollForCompletionInvalidArguments(t *testing.T) {
	invalidDatabase := pollingTestOperation()
	invalidDatabase.Database = ""
	invalidTable := pollingTestOperation()
	invalidTable.Table = ""
	invalidID := pollingTestOperation()
	invalidID.OperationID = ""
	streaming := pollingTestOperation()
	streaming.IngestKind = ingestoptions.IngestKindStreaming
	for _, tc := range []struct {
		name string
		op   *IngestionOperation
		opts []PollingOption
	}{
		{name: "nil-operation"},
		{name: "empty-database", op: invalidDatabase},
		{name: "empty-table", op: invalidTable},
		{name: "empty-operation-id", op: invalidID},
		{name: "streaming-operation", op: streaming},
		{name: "nil-option", op: pollingTestOperation(), opts: []PollingOption{nil}},
		{name: "zero-interval", op: pollingTestOperation(), opts: []PollingOption{WithPollingInterval(0)}},
		{name: "negative-interval", op: pollingTestOperation(), opts: []PollingOption{WithPollingInterval(-time.Second)}},
		{name: "zero-timeout", op: pollingTestOperation(), opts: []PollingOption{WithPollingTimeout(0)}},
		{name: "negative-timeout", op: pollingTestOperation(), opts: []PollingOption{WithPollingTimeout(-time.Second)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &QueuedIngestClient{}
			if result, err := client.PollForCompletion(context.Background(), tc.op, tc.opts...); err == nil || result != nil {
				t.Fatalf("expected validation failure before making a request: result=%+v, error=%v", result, err)
			}
		})
	}
}

func TestPollForCompletionPreservesWireFailureDetails(t *testing.T) {
	client := newPollingTestClient(t, false, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"status":{"failed":1},
			"details":[{
				"sourceId":"source-id","status":"Failed","errorCode":"DownloadFailed",
				"failureStatus":"Transient","details":"Unable to download the blob"
			}]
		}`))
	})
	result, err := client.PollForCompletion(context.Background(), pollingTestOperation())
	if err == nil || result == nil || len(result.Details) != 1 {
		t.Fatalf("expected blob failure details, got result=%+v, error=%v", result, err)
	}
	if result.Details[0].FailureStatus != "Transient" || result.Details[0].Details != "Unable to download the blob" {
		t.Fatalf("service failure details were lost: %+v", result.Details[0])
	}
	if !strings.Contains(err.Error(), "DownloadFailed") || !strings.Contains(err.Error(), "Unable to download the blob") {
		t.Fatalf("polling error omitted service failure diagnostics: %v", err)
	}
}
