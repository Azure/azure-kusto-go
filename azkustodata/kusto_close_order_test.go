package azkustodata

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/azkustodata/internal/response"
	"github.com/Azure/azure-kusto-go/azkustodata/kql"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
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

// spyDecomp wraps the real gzip/flate reader. Read is logged when it returns;
// Close is logged when it starts and when it returns.
type spyDecomp struct {
	io.ReadCloser
	log      *orderLog
	closeErr error
	inRead   atomic.Int32
}

func (s *spyDecomp) Read(p []byte) (int, error) {
	s.inRead.Add(1)
	defer s.inRead.Add(-1)
	n, err := s.ReadCloser.Read(p)
	s.log.add("decomp-read")
	return n, err
}

func (s *spyDecomp) Close() error {
	s.log.add("decomp-close-start")
	err := s.ReadCloser.Close()
	s.log.add("decomp-close-done")
	if s.closeErr != nil {
		return errors.Join(err, s.closeErr)
	}
	return err
}

// largeV2Frames is a fragmented V2 payload bigger than the frame reader's buffer, so a gzip
// trailer stall happens on a later read, after the dataset has already been constructed.
func largeV2Frames() string {
	var b strings.Builder
	b.WriteString(stalledV2Frames)
	row := ",{\"FrameType\":\"TableFragment\",\"TableFragmentType\":\"DataAppend\",\"TableId\":1,\"Rows\":[[1]]}\n"
	for b.Len() < 8192 {
		b.WriteString(row)
	}
	return b.String()
}

func gzipWithoutTrailer(t *testing.T, payload string) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	_, err := io.WriteString(w, payload)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	raw := buf.Bytes()
	require.Greater(t, len(raw), 8)
	return raw[:len(raw)-8]
}

// TestIterativeQueryCloseOrderCompressed closes an iterative dataset while its real gzip or
// deflate reader is blocked on the HTTP body. Cancellation must unblock that read, and only
// then may cleanup run, in this order:
//
//	HTTP Read returns → decompressor Read returns → decompressor Close starts and completes → HTTP body Close starts
//
// It covers a gzip payload stall, a gzip trailer stall, a deflate payload stall, and a
// decompressor Close error that must not skip the HTTP body.
func TestIterativeQueryCloseOrderCompressed(t *testing.T) {
	cases := []struct {
		name string
		enc  string
		mode string
	}{
		{name: "gzip/payload", enc: "gzip", mode: "payload"},
		{name: "gzip/trailer", enc: "gzip", mode: "trailer"},
		{name: "deflate/payload", enc: "deflate", mode: "payload"},
		{name: "gzip/close-error", enc: "gzip", mode: "close-error"},
		{name: "deflate/close-error", enc: "deflate", mode: "close-error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runCloseOrder(t, tc.enc, tc.mode)
		})
	}
}

func runCloseOrder(t *testing.T, enc, mode string) {
	t.Helper()
	evs := &orderLog{}
	var spy atomic.Pointer[spyDecomp]
	closeErr := error(nil)
	if mode == "close-error" {
		closeErr = errors.New("decompressor close failed")
	}
	oldG, oldF := response.NewGzipReader, response.NewFlateReader
	t.Cleanup(func() { response.NewGzipReader, response.NewFlateReader = oldG, oldF })
	response.NewGzipReader = func(r io.Reader) (io.ReadCloser, error) {
		g, err := oldG(r)
		if err != nil {
			return nil, err
		}
		s := &spyDecomp{ReadCloser: g, log: evs, closeErr: closeErr}
		spy.Store(s)
		return s, nil
	}
	response.NewFlateReader = func(r io.Reader) io.ReadCloser {
		s := &spyDecomp{ReadCloser: oldF(r), log: evs, closeErr: closeErr}
		spy.Store(s)
		return s
	}

	var trailer []byte
	if mode == "trailer" {
		trailer = gzipWithoutTrailer(t, largeV2Frames())
	}

	done := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/v2/rest/query") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Encoding", enc)
		w.WriteHeader(http.StatusOK)
		if mode == "trailer" {
			_, _ = w.Write(trailer)
			w.(http.Flusher).Flush()
		} else {
			var cw io.WriteCloser
			var flusher interface{ Flush() error }
			if enc == "gzip" {
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
		}
		select {
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

	ds := queryWithin(t, client)
	if mode != "trailer" {
		select {
		case tr := <-ds.Tables():
			require.NotNil(t, tr)
		case <-time.After(2 * time.Second):
			t.Fatal("primary table was not delivered before the stall")
		}
	}

	body := tt.body.Load()
	require.NotNil(t, body)
	waitFor(t, "a blocked decompressor read", 100*time.Millisecond, 3*time.Second, func() bool {
		s := spy.Load()
		return s != nil && s.inRead.Load() > 0 && body.inRead.Load() > 0
	})
	require.Zero(t, evs.count("decomp-close-start"), "decompressor closed while its read is blocked")
	require.Zero(t, evs.count("body-close"), "body closed while a read is blocked")
	if mode == "trailer" {
		require.NotZero(t, evs.count("decomp-read"), "trailer stall must follow a completed decompressor read")
	}

	evs.add("dataset-close")
	require.NoError(t, ds.Close())
	select {
	case <-drain(ds.Tables()):
	case <-time.After(3 * time.Second):
		t.Fatal("dataset read goroutine did not terminate after Close")
	}
	select {
	case <-body.closed:
	case <-time.After(3 * time.Second):
		t.Fatal("response body was not closed after Close")
	}

	events := evs.snapshot()
	require.False(t, body.closedMidRead.Load(), "body closed while a read was in flight: %v", events)
	require.Equal(t, int32(1), body.closeCount.Load(), "events: %v", events)
	assertCancelCloseOrder(t, events)
}

func queryWithin(t *testing.T, client *Client) query.IterativeDataset {
	t.Helper()
	type result struct {
		ds  query.IterativeDataset
		err error
	}
	ch := make(chan result, 1)
	go func() {
		ds, err := client.IterativeQuery(t.Context(), "db", kql.New("print 1"))
		ch <- result{ds, err}
	}()
	select {
	case res := <-ch:
		require.NoError(t, res.err)
		return res.ds
	case <-time.After(3 * time.Second):
		t.Fatal("IterativeQuery did not return; the response stalled before the dataset was constructed")
	}
	return nil
}

func assertCancelCloseOrder(t *testing.T, events []string) {
	t.Helper()
	require.Equal(t, 1, countEvent(events, "dataset-close"), "events: %v", events)
	require.Equal(t, 1, countEvent(events, "decomp-close-start"), "events: %v", events)
	require.Equal(t, 1, countEvent(events, "decomp-close-done"), "events: %v", events)
	require.Equal(t, 1, countEvent(events, "body-close"), "events: %v", events)
	mark := indexEvent(events, "dataset-close")
	httpRet := lastIndexEvent(events, "http-read")
	decompRet := lastIndexEvent(events, "decomp-read")
	start := indexEvent(events, "decomp-close-start")
	done := indexEvent(events, "decomp-close-done")
	body := indexEvent(events, "body-close")
	require.Greater(t, httpRet, mark, "stalled HTTP read must return after Close: %v", events)
	require.Greater(t, decompRet, httpRet, "decompressor Read must return after that HTTP read: %v", events)
	require.Greater(t, start, decompRet, "decompressor Close must start after Read returns: %v", events)
	require.Greater(t, done, start, "decompressor Close must complete: %v", events)
	require.Greater(t, body, done, "HTTP body Close must start after decompressor Close completes: %v", events)
}

func countEvent(events []string, e string) int {
	n := 0
	for _, x := range events {
		if x == e {
			n++
		}
	}
	return n
}

func indexEvent(events []string, e string) int {
	for i, x := range events {
		if x == e {
			return i
		}
	}
	return -1
}

func lastIndexEvent(events []string, e string) int {
	for i := len(events) - 1; i >= 0; i-- {
		if events[i] == e {
			return i
		}
	}
	return -1
}
