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
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"testing/iotest"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

type requestDrivenReader struct {
	started *atomic.Bool
	reads   atomic.Int32
	input   io.Reader
}

func (r *requestDrivenReader) Read(p []byte) (int, error) {
	r.reads.Add(1)
	if !r.started.Load() {
		return 0, errors.New("source read before HTTP request")
	}
	return r.input.Read(p)
}

func TestStreamingNilReaderReturnsError(t *testing.T) {
	client := NewStreamingIngestClient(nil)
	result, err := client.Ingest(context.Background(), "db", "table",
		ingestoptions.NewStreamSource(nil, ingestoptions.FormatCSV), nil)
	var clientErr *ingestoptions.IngestClientError
	if result != nil || !errors.As(err, &clientErr) || !clientErr.IsPermanent {
		t.Fatalf("expected nil source data error before HTTP, got result=%v err=%v", result, err)
	}
}

func TestStreamingRequestDrivenReader(t *testing.T) {
	for _, format := range []ingestoptions.DataFormat{ingestoptions.FormatCSV, ingestoptions.FormatParquet} {
		for _, leaveOpen := range []bool{false, true} {
			for _, scenario := range []string{"success", "read-failure", "early-response", "auth-failure", "cancellation"} {
				t.Run(fmt.Sprintf("%s/%s/leave-open=%t", format, scenario, leaveOpen), func(t *testing.T) {
					payload := []byte(strings.Repeat("Alice,30\n", 10000))
					failure := errors.New("source read failed")
					payloadReader := bytes.NewReader(payload)
					var input io.Reader = payloadReader
					if scenario == "read-failure" {
						input = io.MultiReader(input, iotest.ErrReader(failure))
					}

					var started atomic.Bool
					reader := &requestDrivenReader{started: &started, input: input}
					stream := &mockE2EStream{Reader: reader}
					source := ingestoptions.NewStreamSource(stream, format, ingestoptions.WithLeaveOpen(leaveOpen))
					t.Cleanup(func() { _ = stream.Close() })
					var body io.ReadCloser
					authFailure := errors.New("authentication failed")
					client, err := NewStreamingClient("http://localhost",
						WithTokenProvider(func(context.Context) (string, error) {
							if scenario == "auth-failure" {
								return "", authFailure
							}
							return "token", nil
						}),
						WithHTTPClient(&http.Client{Transport: endpointCorrectionTransport(func(r *http.Request) (*http.Response, error) {
							started.Store(true)
							body = r.Body
							if scenario == "cancellation" {
								return nil, r.Context().Err()
							}
							status, response := http.StatusOK, "{}"
							if scenario == "early-response" {
								status = http.StatusRequestEntityTooLarge
								response = `{"error":{"code":"RequestTooLarge","message":"too large","@permanent":true}}`
							} else {
								var first [1]byte
								n, err := r.Body.Read(first[:])
								if err != nil {
									return nil, err
								}
								if payloadReader.Len() == 0 {
									t.Error("HTTP reader buffered the complete source before returning initial bytes")
								}
								rest, err := io.ReadAll(r.Body)
								if err != nil {
									return nil, err
								}
								data := append(first[:n:n], rest...)
								if format == ingestoptions.FormatCSV {
									if r.Header.Get("Content-Encoding") != "gzip" {
										t.Error("automatic gzip header was not set")
									}
									gzipReader, err := gzip.NewReader(bytes.NewReader(data))
									if err != nil {
										return nil, err
									}
									data, err = io.ReadAll(gzipReader)
									closeErr := gzipReader.Close()
									if err != nil || closeErr != nil {
										return nil, errors.Join(err, closeErr)
									}
								} else if r.Header.Get("Content-Encoding") != "" {
									t.Error("binary source was compressed")
								}
								if !bytes.Equal(data, payload) {
									t.Error("incremental streaming changed the payload")
								}
							}
							return &http.Response{StatusCode: status, Header: make(http.Header),
								Body: io.NopCloser(strings.NewReader(response)), Request: r}, nil
						})}))
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = client.Close() })
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					if scenario == "cancellation" {
						cancel()
					}
					result, err := client.Ingest(ctx, "db", "table", source, nil)
					switch scenario {
					case "success":
						if err != nil || result == nil || result.Kind != ingestoptions.IngestKindStreaming {
							t.Fatalf("expected streaming success, got result=%v err=%v", result, err)
						}
					case "read-failure":
						if result != nil || !errors.Is(err, failure) {
							t.Fatalf("source read error was not preserved: result=%v err=%v", result, err)
						}
					case "auth-failure":
						if result != nil || !errors.Is(err, authFailure) || started.Load() {
							t.Fatalf("authentication failure was not preserved before transport: result=%v err=%v", result, err)
						}
					case "cancellation":
						if result != nil || !errors.Is(err, context.Canceled) {
							t.Fatalf("cancellation was not preserved: result=%v err=%v", result, err)
						}
					case "early-response":
						var serviceErr *ingestoptions.IngestRequestError
						if result != nil || !errors.As(err, &serviceErr) || serviceErr.HTTPStatusCode != http.StatusRequestEntityTooLarge {
							t.Fatalf("early service rejection was not preserved: result=%v err=%v", result, err)
						}
					}
					if scenario == "early-response" || scenario == "auth-failure" || scenario == "cancellation" {
						if reader.reads.Load() != 0 {
							t.Fatalf("source was read %d times without HTTP consuming the body", reader.reads.Load())
						}
					}
					wantCloses := int32(1)
					if leaveOpen {
						wantCloses = 0
					}
					if stream.closes.Load() != wantCloses {
						t.Fatalf("source closes=%d, want %d", stream.closes.Load(), wantCloses)
					}
					if body != nil {
						var p [1]byte
						if _, err := body.Read(p[:]); !errors.Is(err, io.ErrClosedPipe) {
							t.Fatalf("request reader was not closed on return: %v", err)
						}
					}
				})
			}
		}
	}
}
