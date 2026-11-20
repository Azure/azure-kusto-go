// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package test

import (
	"bytes"
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

	"github.com/Azure/azure-kusto-go/azkustoingestv2"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

// newTestManagedClient creates a managed streaming client backed by httptest servers.
// The engineHandler handles streaming requests; the dmHandler handles queued requests.
func newTestManagedClient(t *testing.T, engineHandler, dmHandler http.HandlerFunc) (*azkustoingestv2.ManagedStreamingIngestClient, func()) {
	t.Helper()

	engineSrv := httptest.NewServer(engineHandler)
	dmSrv := newQueuedTestServer(t, dmHandler)

	managed, err := azkustoingestv2.NewManagedStreamingClient(
		dmSrv.URL, engineSrv.URL,
		azkustoingestv2.WithTokenProvider(func(ctx context.Context) (string, error) { return "token", nil }),
		azkustoingestv2.WithClientDetails(ingestoptions.NewClientDetails("test", "user", "1.0")),
	)
	if err != nil {
		engineSrv.Close()
		dmSrv.Close()
		t.Fatal(err)
	}

	cleanup := func() {
		_ = managed.Close()
		engineSrv.Close()
		dmSrv.Close()
	}
	return managed, cleanup
}

// TestManagedStreaming_SmallData_UsesStreaming verifies that small data uses streaming path.
func TestManagedStreaming_SmallData_UsesStreaming(t *testing.T) {
	t.Parallel()

	var engineCalled atomic.Int32
	var dmCalled atomic.Int32

	engineHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		engineCalled.Add(1)
		w.WriteHeader(http.StatusOK)
	})
	dmHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dmCalled.Add(1)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"ingestionOperationId": "op-1"})
	})

	managed, cleanup := newTestManagedClient(t, engineHandler, dmHandler)
	defer cleanup()

	csvData := "a,b\n1,2\n"
	source := ingestoptions.NewStreamSource(
		io.NopCloser(bytes.NewReader([]byte(csvData))),
		ingestoptions.FormatCSV,
	)

	resp, err := managed.Ingest(context.Background(), "db", "tbl", source,
		&ingestoptions.IngestRequestProperties{Format: ingestoptions.FormatCSV})
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	if resp.Kind != ingestoptions.IngestKindStreaming {
		t.Errorf("expected Streaming kind, got %v", resp.Kind)
	}
	if engineCalled.Load() != 1 {
		t.Errorf("expected engine called once, got %d", engineCalled.Load())
	}
	if dmCalled.Load() != 0 {
		t.Errorf("expected DM not called, got %d", dmCalled.Load())
	}
}

// TestManagedStreaming_TransientError_RetriesThenSucceeds tests retry on transient failure.
func TestManagedStreaming_TransientError_RetriesThenSucceeds(t *testing.T) {
	t.Parallel()

	var attempt atomic.Int32

	engineHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempt.Add(1)
		if n <= 2 {
			// First 2 attempts: return transient error (non-permanent)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"error": map[string]interface{}{
					"code":       "ServiceUnavailable",
					"message":    "Temporary server issue",
					"@permanent": false,
				},
			})
			return
		}
		// 3rd attempt: succeed
		w.WriteHeader(http.StatusOK)
	})
	dmHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("DM should not be called when streaming succeeds after retry")
	})

	managed, cleanup := newTestManagedClient(t, engineHandler, dmHandler)
	defer cleanup()

	csvData := "a,b\n1,2\n"
	source := ingestoptions.NewStreamSource(
		io.NopCloser(bytes.NewReader([]byte(csvData))),
		ingestoptions.FormatCSV,
	)

	resp, err := managed.Ingest(context.Background(), "db", "tbl", source,
		&ingestoptions.IngestRequestProperties{Format: ingestoptions.FormatCSV})
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	if resp.Kind != ingestoptions.IngestKindStreaming {
		t.Errorf("expected Streaming kind, got %v", resp.Kind)
	}
	if attempt.Load() != 3 {
		t.Errorf("expected 3 attempts, got %d", attempt.Load())
	}
}

func TestManagedStreaming_PermanentError_ReturnsWithoutFallback(t *testing.T) {
	t.Parallel()

	var engineCalled atomic.Int32

	engineHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		engineCalled.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": map[string]interface{}{
				"code":       "BadRequest",
				"message":    "Table has incompatible schema configuration",
				"@permanent": true,
			},
		})
	})
	dmHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("DM should not be called after a permanent streaming error")
		w.WriteHeader(http.StatusBadRequest)
	})

	managed, cleanup := newTestManagedClient(t, engineHandler, dmHandler)
	defer cleanup()

	blob := mustBlobSource(t, "https://storage/data.csv", ingestoptions.FormatCSV)

	resp, err := managed.Ingest(context.Background(), "db", "tbl", blob,
		&ingestoptions.IngestRequestProperties{Format: ingestoptions.FormatCSV})
	var requestErr *ingestoptions.IngestRequestError
	if resp != nil || !errors.As(err, &requestErr) || !requestErr.IsPermanent ||
		requestErr.HTTPStatusCode != http.StatusBadRequest || !strings.Contains(err.Error(), "incompatible schema") {
		t.Fatalf("expected the permanent streaming error, got response=%v err=%v", resp, err)
	}
	// Streaming should have been attempted exactly once (permanent = no retry)
	if engineCalled.Load() != 1 {
		t.Errorf("expected engine called once (permanent error, no retry), got %d", engineCalled.Load())
	}
}

func TestManagedStreaming_StreamingDisabled_NoFallback(t *testing.T) {
	t.Parallel()

	engineHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": map[string]interface{}{
				"code":       "BadRequest",
				"message":    "Streaming ingestion is disabled for this cluster",
				"@permanent": true,
			},
		})
	})
	dmHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("DM should not be called after a permanent streaming-disabled error")
	})

	managed, cleanup := newTestManagedClient(t, engineHandler, dmHandler)
	defer cleanup()

	csvData := "a,b\n1,2\n"
	source := ingestoptions.NewStreamSource(
		io.NopCloser(bytes.NewReader([]byte(csvData))),
		ingestoptions.FormatCSV,
	)

	_, err := managed.Ingest(context.Background(), "db", "tbl", source,
		&ingestoptions.IngestRequestProperties{Format: ingestoptions.FormatCSV})
	if err == nil {
		t.Fatal("expected the permanent streaming-disabled error")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "disabled") {
		t.Errorf("expected 'disabled' in error, got %q", err.Error())
	}
}

func TestManagedStreaming_PermanentError_DoesNotBypassNextStreamingRequest(t *testing.T) {
	t.Parallel()

	var engineCalls atomic.Int32
	dmHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("permanent streaming errors must not route later requests to DM")
		w.WriteHeader(http.StatusBadRequest)
	})

	engineDisabledSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if engineCalls.Add(1) > 1 {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": map[string]interface{}{
				"code":       "BadRequest",
				"message":    "Table has incompatible schema",
				"@permanent": true,
			},
		})
	}))
	defer engineDisabledSrv.Close()

	dmSrv := newQueuedTestServer(t, dmHandler)
	defer dmSrv.Close()

	managed, err := azkustoingestv2.NewManagedStreamingClient(
		dmSrv.URL, engineDisabledSrv.URL,
		azkustoingestv2.WithTokenProvider(func(ctx context.Context) (string, error) { return "token", nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer managed.Close()

	blob1 := mustBlobSource(t, "https://storage/data1.csv", ingestoptions.FormatCSV)
	if resp, err := managed.Ingest(context.Background(), "db", "tbl", blob1,
		&ingestoptions.IngestRequestProperties{Format: ingestoptions.FormatCSV}); resp != nil || err == nil {
		t.Fatalf("expected a permanent streaming error, got response=%v err=%v", resp, err)
	}

	blob2 := mustBlobSource(t, "https://storage/data2.csv", ingestoptions.FormatCSV)
	resp, err := managed.Ingest(context.Background(), "db", "tbl", blob2,
		&ingestoptions.IngestRequestProperties{Format: ingestoptions.FormatCSV})
	if err != nil {
		t.Fatalf("Second ingest failed: %v", err)
	}
	if resp.Kind != ingestoptions.IngestKindStreaming || engineCalls.Load() != 2 {
		t.Errorf("expected the next request to use streaming, got response=%v engine calls=%d", resp, engineCalls.Load())
	}
}

// TestManagedStreaming_EmptyFields_Rejected verifies validation for empty database/table.
func TestManagedStreaming_EmptyFields_Rejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		database string
		table    string
		errMsg   string
	}{
		{"empty_database", "", "tbl", "database"},
		{"empty_table", "db", "", "table"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			managed, err := azkustoingestv2.NewManagedStreamingClient(
				"https://ingest-cluster.kusto.windows.net", "https://cluster.kusto.windows.net")
			if err != nil {
				t.Fatal(err)
			}
			defer managed.Close()
			_, err = managed.Ingest(context.Background(), tt.database, tt.table, nil, nil)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.errMsg) {
				t.Errorf("expected %q in error, got %q", tt.errMsg, err.Error())
			}
		})
	}
}

// TestManagedStreaming_GetOperationSummary_DelegatesBasedOnKind verifies that summary
// delegates to queued client for queued ops and returns empty for streaming ops.
func TestManagedStreaming_GetOperationSummary_DelegatesBasedOnKind(t *testing.T) {
	t.Parallel()

	t.Run("streaming_returns_empty", func(t *testing.T) {
		t.Parallel()
		managed, err := azkustoingestv2.NewManagedStreamingClient(
			"https://ingest-cluster.kusto.windows.net", "https://cluster.kusto.windows.net")
		if err != nil {
			t.Fatal(err)
		}
		defer managed.Close()
		status, err := managed.GetOperationSummary(context.Background(), &azkustoingestv2.IngestionOperation{
			IngestKind: ingestoptions.IngestKindStreaming,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if status.Succeeded != 0 || status.Failed != 0 {
			t.Errorf("expected empty status for streaming, got %+v", status)
		}
	})

	t.Run("queued_delegates_to_api", func(t *testing.T) {
		t.Parallel()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status": map[string]interface{}{
					"succeeded": 42,
				},
			})
		}))
		defer srv.Close()

		managed, err := azkustoingestv2.NewManagedStreamingClient(srv.URL, srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		defer managed.Close()
		status, err := managed.GetOperationSummary(context.Background(), &azkustoingestv2.IngestionOperation{
			Database:    "db",
			Table:       "tbl",
			OperationID: "op-1",
			IngestKind:  ingestoptions.IngestKindQueued,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if status.Succeeded != 42 {
			t.Errorf("expected 42 succeeded, got %d", status.Succeeded)
		}
	})
}

// TestManagedStreaming_AllRetriesExhausted_FallsBackToQueued tests that after all retries
// are exhausted on transient errors, the client falls back to queued ingestion.
func TestManagedStreaming_AllRetriesExhausted_FallsBackToQueued(t *testing.T) {
	t.Parallel()

	var engineCalls atomic.Int32

	engineHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		engineCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": map[string]interface{}{
				"code":       "ServiceUnavailable",
				"message":    "Server too busy",
				"@permanent": false,
			},
		})
	})
	dmHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"ingestionOperationId": "op-fallback"})
	})

	managed, cleanup := newTestManagedClient(t, engineHandler, dmHandler)
	defer cleanup()

	blob := mustBlobSource(t, "https://storage/data.csv", ingestoptions.FormatCSV)

	resp, err := managed.Ingest(context.Background(), "db", "tbl", blob,
		&ingestoptions.IngestRequestProperties{Format: ingestoptions.FormatCSV})
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	if resp.Kind != ingestoptions.IngestKindQueued {
		t.Errorf("expected Queued kind after retry exhaustion, got %v", resp.Kind)
	}

	// Default retry policy has 3 retries, so engine should be called 4 times (1 initial + 3 retries)
	// or 3 times if ShouldRetry checks happen before the attempt
	calls := engineCalls.Load()
	if calls < 2 {
		t.Errorf("expected multiple engine attempts before fallback, got %d", calls)
	}

	fmt.Printf("  → Engine was called %d times before falling back to queued\n", calls)
}
