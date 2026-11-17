package azkustodata

import (
	"compress/flate"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/azkustodata/kql"
	"github.com/stretchr/testify/require"
)

const stalledV2Frames = `[{"FrameType":"DataSetHeader","IsProgressive":false,"Version":"v2.0","IsFragmented":true,"ErrorReportingPlacement":"EndOfTable"}
,{"FrameType":"DataTable","TableId":0,"TableKind":"QueryProperties","TableName":"@ExtendedProperties","Columns":[],"Rows":[]}
,{"FrameType":"TableHeader","TableId":1,"TableKind":"PrimaryResult","TableName":"PrimaryResult","Columns":[{"ColumnName":"a","ColumnType":"int"}]}
`

// TestIterativeQueryCloseStalledCompressedRead checks that closing an iterative dataset whose (optionally compressed)
// response stalls mid-stream returns nil and lets the read goroutine finish.
func TestIterativeQueryCloseStalledCompressedRead(t *testing.T) {
	for _, enc := range []string{"identity", "gzip", "deflate"} {
		enc := enc
		t.Run(enc, func(t *testing.T) {
			done := make(chan struct{})
			disconnected := make(chan struct{})
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !strings.HasSuffix(r.URL.Path, "/v2/rest/query") {
					// Auth metadata lookup: an empty 404 selects the default cloud info.
					w.WriteHeader(http.StatusNotFound)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				if enc != "identity" {
					w.Header().Set("Content-Encoding", enc)
				}
				w.WriteHeader(http.StatusOK)

				var cw io.WriteCloser
				var flusher interface{ Flush() error }
				if enc == "identity" {
					cw, flusher = nopWriteCloser{w}, nopFlusher{}
				} else if enc == "gzip" {
					g := gzip.NewWriter(w)
					cw, flusher = g, g
				} else {
					f, err := flate.NewWriter(w, flate.DefaultCompression)
					if err != nil {
						return
					}
					cw, flusher = f, f
				}
				_, _ = io.Copy(cw, strings.NewReader(stalledV2Frames))
				_ = flusher.Flush()
				w.(http.Flusher).Flush()

				// Stall: send no more data until the client goes away.
				select {
				case <-r.Context().Done():
					close(disconnected)
				case <-done:
				}
			}))
			defer srv.Close()
			defer close(done)

			tt := &trackingTransport{base: http.DefaultTransport}
			client, err := New(NewConnectionStringBuilder(srv.URL), WithHttpClient(&http.Client{Transport: tt}))
			require.NoError(t, err)
			defer client.Close()

			ds, err := client.IterativeQuery(t.Context(), "db", kql.New("print 1"))
			require.NoError(t, err)

			select {
			case tr := <-ds.Tables():
				require.NotNil(t, tr)
			case <-time.After(2 * time.Second):
				t.Fatal("primary table was not delivered before the stall")
			}
			body := tt.body.Load()
			require.NotNil(t, body)
			// The stream is stalled once a body Read is in flight and stays blocked (the server sends nothing more).
			waitFor(t, "a stalled body read", 100*time.Millisecond, 2*time.Second, func() bool { return body.inRead.Load() > 0 })
			require.False(t, body.closeOnce.Load(), "body must not be closed before dataset Close")

			start := time.Now()
			require.NoError(t, ds.Close())

			select {
			case <-drain(ds.Tables()):
			case <-time.After(2 * time.Second):
				t.Fatal("dataset read goroutine did not terminate after Close")
			}
			// The SDK itself must close the response body, and only after the blocked read returned.
			select {
			case <-body.closed:
			case <-time.After(2 * time.Second):
				t.Fatal("response body was not closed after Close")
			}
			require.False(t, body.closedMidRead.Load(), "body was closed while a read was still in flight")
			select {
			case <-disconnected:
			case <-time.After(2 * time.Second):
				t.Fatal("server did not see the connection released")
			}
			require.Less(t, time.Since(start), 4*time.Second)
		})
	}
}

// trackingTransport wraps response bodies so a test can observe in-flight reads and Close calls.
type trackingTransport struct {
	base http.RoundTripper
	body atomic.Pointer[trackingBody]
}

func (t *trackingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(r)
	if err != nil || !strings.HasSuffix(r.URL.Path, "/v2/rest/query") {
		return resp, err
	}
	tb := &trackingBody{ReadCloser: resp.Body, closed: make(chan struct{})}
	t.body.Store(tb)
	resp.Body = tb
	return resp, nil
}

type trackingBody struct {
	io.ReadCloser
	inRead        atomic.Int32 // number of Read calls currently in flight
	closedMidRead atomic.Bool  // Close was called while a Read was in flight
	closed        chan struct{}
	closeOnce     atomic.Bool
	closeCount    atomic.Int32 // total Close calls
}

func (b *trackingBody) Read(p []byte) (int, error) {
	b.inRead.Add(1)
	defer b.inRead.Add(-1)
	return b.ReadCloser.Read(p)
}

func (b *trackingBody) Close() error {
	b.closeCount.Add(1)
	if b.inRead.Load() > 0 {
		b.closedMidRead.Store(true)
	}
	if b.closeOnce.CompareAndSwap(false, true) {
		close(b.closed)
	}
	return b.ReadCloser.Close()
}

// waitFor polls cond until it holds continuously for hold, or fails after timeout.
func waitFor(t *testing.T, what string, hold, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var since time.Time
	for time.Now().Before(deadline) {
		if cond() {
			if since.IsZero() {
				since = time.Now()
			} else if time.Since(since) >= hold {
				return
			}
		} else {
			since = time.Time{}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// drain consumes the channel until it is closed and signals completion.
func drain[T any](ch <-chan T) <-chan struct{} {
	out := make(chan struct{})
	go func() {
		defer close(out)
		for range ch {
		}
	}()
	return out
}

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

type nopFlusher struct{}

func (nopFlusher) Flush() error { return nil }
