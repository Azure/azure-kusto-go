// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package azkustoingestv2

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

type customNilLocalSource struct {
	*ingestoptions.StreamSource
}

type typedNilReadCloser struct {
	io.ReadCloser
}

func newInputValidationClient(t *testing.T, mode string, calls *atomic.Int32) IngestClient {
	t.Helper()
	options := []ClientOption{
		WithTokenProvider(func(context.Context) (string, error) {
			calls.Add(1)
			return "token", nil
		}),
		WithHTTPClient(&http.Client{Transport: endpointCorrectionTransport(func(*http.Request) (*http.Response, error) {
			calls.Add(1)
			return nil, errors.New("unexpected HTTP request for invalid input")
		})}),
	}
	var client IngestClient
	var err error
	switch mode {
	case "queued":
		client, err = NewQueuedClient("http://localhost", options...)
	case "streaming":
		client, err = NewStreamingClient("http://localhost", options...)
	case "managed":
		client, err = NewManagedStreamingClient("http://localhost", "http://localhost", options...)
	default:
		t.Fatalf("unknown client mode %q", mode)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	return client
}

func assertInvalidInputCall(t *testing.T, call func() error) {
	t.Helper()
	defer func() {
		if p := recover(); p != nil {
			t.Errorf("invalid API input caused a panic: %v", p)
		}
	}()
	err := call()
	var clientErr *ingestoptions.IngestClientError
	if !errors.As(err, &clientErr) || !clientErr.IsPermanent {
		t.Fatalf("expected permanent SDK argument error, got %v", err)
	}
}

func TestIngestRejectsNilSources(t *testing.T) {
	cases := []struct {
		name   string
		source ingestoptions.IngestionSource
	}{
		{name: "nil-interface"},
		{name: "typed-nil-file", source: (*ingestoptions.FileSource)(nil)},
		{name: "typed-nil-stream", source: (*ingestoptions.StreamSource)(nil)},
		{name: "typed-nil-blob", source: (*ingestoptions.BlobSource)(nil)},
		{name: "typed-nil-custom", source: (*customNilLocalSource)(nil)},
	}
	for _, mode := range []string{"queued", "streaming", "managed"} {
		for _, tc := range cases {
			for _, explicitFormat := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/explicit-format=%t", mode, tc.name, explicitFormat), func(t *testing.T) {
					var calls atomic.Int32
					client := newInputValidationClient(t, mode, &calls)
					var props *ingestoptions.IngestRequestProperties
					if explicitFormat {
						props = &ingestoptions.IngestRequestProperties{Format: ingestoptions.FormatCSV}
					}
					assertInvalidInputCall(t, func() error {
						result, err := client.Ingest(context.Background(), "db", "table", tc.source, props)
						if result != nil {
							t.Errorf("invalid source returned a result: %+v", result)
						}
						return err
					})
					if calls.Load() != 0 {
						t.Fatal("invalid source reached authentication or HTTP")
					}
				})
			}
		}
	}
}

func TestIngestRejectsTypedNilReaders(t *testing.T) {
	for _, mode := range []string{"queued", "streaming", "managed"} {
		for _, leaveOpen := range []bool{false, true} {
			for _, custom := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/leave-open=%t/custom=%t", mode, leaveOpen, custom), func(t *testing.T) {
					defer func() {
						if p := recover(); p != nil {
							t.Errorf("nil reader caused a panic: %v", p)
						}
					}()
					var calls atomic.Int32
					client := newInputValidationClient(t, mode, &calls)
					var source ingestoptions.IngestionSource = ingestoptions.NewStreamSource(
						(*typedNilReadCloser)(nil), ingestoptions.FormatCSV,
						ingestoptions.WithLeaveOpen(leaveOpen),
					)
					if custom {
						source = &customNilLocalSource{StreamSource: source.(*ingestoptions.StreamSource)}
					}
					result, err := client.Ingest(context.Background(), "db", "table", source, nil)
					var clientErr *ingestoptions.IngestClientError
					var uploadErr *ingestoptions.UploadFailedError
					permanent := errors.As(err, &clientErr) && clientErr.IsPermanent ||
						errors.As(err, &uploadErr) && uploadErr.IsPermanent
					if result != nil || !permanent {
						t.Fatalf("expected permanent SDK error, got result=%v err=%v", result, err)
					}
					if calls.Load() != 0 {
						t.Fatal("nil reader reached authentication or HTTP")
					}
				})
			}
		}
	}
}

func TestOperationLookupsRejectNil(t *testing.T) {
	for _, mode := range []string{"queued", "streaming", "managed"} {
		for _, details := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/details=%t", mode, details), func(t *testing.T) {
				var calls atomic.Int32
				client := newInputValidationClient(t, mode, &calls)
				assertInvalidInputCall(t, func() error {
					if details {
						result, err := client.GetOperationDetails(context.Background(), nil)
						if result != nil {
							t.Errorf("nil operation returned details: %+v", result)
						}
						return err
					}
					result, err := client.GetOperationSummary(context.Background(), nil)
					if result != nil {
						t.Errorf("nil operation returned a summary: %+v", result)
					}
					return err
				})
				if calls.Load() != 0 {
					t.Fatal("nil operation reached authentication or HTTP")
				}
			})
		}
	}
}

func TestIngestBlobsRejectsNilEntries(t *testing.T) {
	blob, err := ingestoptions.NewBlobSource("https://storage.example/data.csv", ingestoptions.FormatCSV)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		sources []*ingestoptions.BlobSource
	}{
		{name: "nil-list"},
		{name: "empty-list", sources: []*ingestoptions.BlobSource{}},
		{name: "nil-entry", sources: []*ingestoptions.BlobSource{nil}},
		{name: "nil-first", sources: []*ingestoptions.BlobSource{nil, blob}},
		{name: "nil-last", sources: []*ingestoptions.BlobSource{blob, nil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			client := newInputValidationClient(t, "queued", &calls).(*QueuedIngestClient)
			assertInvalidInputCall(t, func() error {
				result, err := client.IngestBlobs(context.Background(), "db", "table", tc.sources, nil)
				if result != nil {
					t.Errorf("invalid source list returned a result: %+v", result)
				}
				return err
			})
			if calls.Load() != 0 {
				t.Fatal("invalid blob list reached configuration lookup, authentication or HTTP")
			}
		})
	}
}
