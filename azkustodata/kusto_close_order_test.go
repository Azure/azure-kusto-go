package azkustodata

import (
	"compress/flate"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/azkustodata/internal/response"
	"github.com/Azure/azure-kusto-go/azkustodata/kql"
	"github.com/stretchr/testify/require"
)

type orderLog struct {
	mu     sync.Mutex
	events []string
}

func (l *orderLog) add(e string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, e)
}

func (l *orderLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

func (l *orderLog) count(e string) int {
	n := 0
	for _, x := range l.snapshot() {
		if x == e {
			n++
		}
	}
	return n
}

type spyDecomp struct {
	io.ReadCloser
	log *orderLog
}

func (s *spyDecomp) Read(p []byte) (int, error) {
	n, err := s.ReadCloser.Read(p)
	s.log.add("decomp-read")
	return n, err
}

func (s *spyDecomp) Close() error {
	s.log.add("decomp-close")
	return s.ReadCloser.Close()
}

// TestIterativeQueryCloseOrderCompressed runs the full client stack against a server whose gzip/deflate V2 response
// stalls mid-stream, and checks that Close happens only after the blocked read returned, in the order
// HTTP read -> decompressor read -> decompressor close -> body close, each exactly once.
func TestIterativeQueryCloseOrderCompressed(t *testing.T) {
	for _, enc := range []string{"gzip", "deflate"} {
		enc := enc
		t.Run(enc, func(t *testing.T) {
			evs := &orderLog{}
			oldG, oldF := response.NewGzipReader, response.NewFlateReader
			t.Cleanup(func() { response.NewGzipReader, response.NewFlateReader = oldG, oldF })
			response.NewGzipReader = func(r io.Reader) (io.ReadCloser, error) {
				g, err := oldG(r)
				if err != nil {
					return nil, err
				}
				return &spyDecomp{g, evs}, nil
			}
			response.NewFlateReader = func(r io.Reader) io.ReadCloser { return &spyDecomp{oldF(r), evs} }

			done := make(chan struct{})
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !strings.HasSuffix(r.URL.Path, "/v2/rest/query") {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Content-Encoding", enc)
				w.WriteHeader(http.StatusOK)
				var cw io.WriteCloser
				var flusher interface{ Flush() error }
				if enc == "gzip" {
					g := gzip.NewWriter(w)
					cw, flusher = g, g
				} else {
					f, _ := flate.NewWriter(w, flate.DefaultCompression)
					cw, flusher = f, f
				}
				_, _ = io.Copy(cw, strings.NewReader(stalledV2Frames))
				_ = flusher.Flush()
				w.(http.Flusher).Flush()
				select { // stall until the client goes away
				case <-r.Context().Done():
				case <-done:
				}
			}))
			defer srv.Close()
			defer close(done)

			tt := &trackingTransport{base: http.DefaultTransport, log: evs.add}
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
			// Synchronize on the stall: a body read is blocked in flight and nothing may be closed yet.
			waitFor(t, "a blocked body read", 100*time.Millisecond, 2*time.Second, func() bool { return body.inRead.Load() > 0 })
			require.Zero(t, evs.count("decomp-close"), "decompressor closed while the read is blocked")
			require.Zero(t, evs.count("body-close"), "body closed while the read is blocked")

			require.NoError(t, ds.Close())
			select {
			case <-drain(ds.Tables()):
			case <-time.After(2 * time.Second):
				t.Fatal("dataset read goroutine did not terminate after Close")
			}
			select {
			case <-body.closed:
			case <-time.After(2 * time.Second):
				t.Fatal("response body was not closed after Close")
			}

			events := evs.snapshot()
			require.False(t, body.closedMidRead.Load(), "body closed while a read was in flight")
			require.Equal(t, 1, evs.count("decomp-close"), "events: %v", events)
			require.Equal(t, 1, evs.count("body-close"), "events: %v", events)
			require.Equal(t, int32(1), body.closeCount.Load())
			require.NotEmpty(t, events)
			require.Equal(t, "http-read", events[0], "HTTP read must come first: %v", events)
			require.Equal(t, []string{"decomp-close", "body-close"}, events[len(events)-2:],
				"decompressor close must come right before body close, after every read: %v", events)
			require.Contains(t, events, "decomp-read")
		})
	}
}
