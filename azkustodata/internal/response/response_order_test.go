package response

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	stderrors "errors"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Azure/azure-kusto-go/azkustodata/errors"
)

// eventBody is a response body fed from a channel of chunks, so a test can stall the stream (payload or trailer)
// and record the order of the HTTP-level Read and Close calls made by the real gzip/flate reader on top of it.
type eventBody struct {
	chunks chan []byte
	cur    []byte

	mu         sync.Mutex
	events     []string
	closeCalls int
	inRead     bool
	closedMid  bool
}

func newEventBody() *eventBody { return &eventBody{chunks: make(chan []byte, 8)} }

func (b *eventBody) log(e string) {
	b.mu.Lock()
	b.events = append(b.events, e)
	b.mu.Unlock()
}

func (b *eventBody) snapshot() ([]string, int, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.events...), b.closeCalls, b.closedMid
}

func (b *eventBody) Read(p []byte) (int, error) {
	if len(b.cur) == 0 {
		b.mu.Lock()
		b.inRead = true
		b.mu.Unlock()
		b.log("http-read-wait")
		c, ok := <-b.chunks
		b.mu.Lock()
		b.inRead = false
		b.mu.Unlock()
		if !ok {
			return 0, io.EOF
		}
		b.cur = c
	}
	n := copy(p, b.cur)
	b.cur = b.cur[n:]
	b.log("http-read")
	return n, nil
}

func (b *eventBody) Close() error {
	b.mu.Lock()
	b.closeCalls++
	if b.inRead {
		b.closedMid = true
	}
	b.mu.Unlock()
	b.log("body-close")
	return nil
}

func compress(t *testing.T, enc string, payload []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	var w io.WriteCloser
	switch enc {
	case "gzip":
		w = gzip.NewWriter(&buf)
	case "deflate":
		fw, err := flate.NewWriter(&buf, flate.DefaultCompression)
		require.NoError(t, err)
		w = fw
	}
	_, err := w.Write(payload)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return buf.Bytes()
}

// spyReader wraps the real decompressor and records its Read/Close calls in the same event log as the HTTP body.
type spyReader struct {
	io.ReadCloser
	body     *eventBody
	closeErr error
}

func (s *spyReader) Read(p []byte) (int, error) {
	n, err := s.ReadCloser.Read(p)
	s.body.log("decomp-read")
	return n, err
}

func (s *spyReader) Close() error {
	s.body.log("decomp-close")
	return stderrors.Join(s.ReadCloser.Close(), s.closeErr)
}

// spyDecompressors replaces the package's decompressor constructors with spies bound to body for the test.
func spyDecompressors(t *testing.T, body *eventBody, closeErr error) {
	t.Helper()
	oldG, oldF := NewGzipReader, NewFlateReader
	t.Cleanup(func() { NewGzipReader, NewFlateReader = oldG, oldF })
	NewGzipReader = func(r io.Reader) (io.ReadCloser, error) {
		g, err := oldG(r)
		if err != nil {
			return nil, err
		}
		return &spyReader{ReadCloser: g, body: body, closeErr: closeErr}, nil
	}
	NewFlateReader = func(r io.Reader) io.ReadCloser {
		return &spyReader{ReadCloser: oldF(r), body: body, closeErr: closeErr}
	}
}

func count(events []string, e string) int {
	n := 0
	for _, x := range events {
		if x == e {
			n++
		}
	}
	return n
}

func index(events []string, e string) int {
	for i, x := range events {
		if x == e {
			return i
		}
	}
	return -1
}

// TestTranslateBodyCloseOrder drives the real gzip/flate readers through a stalled payload and checks the order
// HTTP read -> decompressor read -> decompressor close -> body close, that nothing is closed while the payload is
// stalled or mid-read, and that each is closed exactly once.
func TestTranslateBodyCloseOrder(t *testing.T) {
	payload := bytes.Repeat([]byte("kusto-frame,"), 200)
	for _, enc := range []string{"gzip", "deflate"} {
		enc := enc
		t.Run(enc, func(t *testing.T) {
			r := require.New(t)
			full := compress(t, enc, payload)
			half := len(full) / 2

			body := newEventBody()
			spyDecompressors(t, body, nil)
			// gzip.NewReader reads the header eagerly, so the first chunk must be available before TranslateBody.
			body.chunks <- full[:half]
			rc, err := TranslateBody(&http.Response{
				Header: http.Header{"Content-Encoding": []string{enc}},
				Body:   body,
			}, errors.OpQuery)
			r.NoError(err)

			got := make(chan []byte, 1)
			go func() {
				b, _ := io.ReadAll(rc)
				got <- b
			}()

			// Stall the payload: the reader must be blocked on the HTTP body and nothing may be closed.
			time.Sleep(100 * time.Millisecond)
			events, closes, _ := body.snapshot()
			r.Zero(closes, "body closed while the payload is stalled")
			r.Zero(count(events, "decomp-close"), "decompressor closed while the payload is stalled")

			body.chunks <- full[half:]
			close(body.chunks)
			select {
			case b := <-got:
				r.Equal(payload, b)
			case <-time.After(2 * time.Second):
				t.Fatal("read did not finish")
			}

			r.NoError(rc.Close())
			events, closes, mid := body.snapshot()
			r.Equal(1, closes, "body must be closed exactly once")
			r.False(mid, "body closed while a read was in flight")
			r.Equal(1, count(events, "decomp-close"), "decompressor must be closed exactly once")
			r.Less(index(events, "http-read"), index(events, "decomp-read"), "HTTP read must precede decompressor read")
			r.Equal([]string{"decomp-close", "body-close"}, events[len(events)-2:],
				"decompressor close must come right before body close, after every read")
		})
	}
}

// TestTranslateBodyCloseStalledTrailer stalls a gzip stream before its trailer, then cuts it; the body must still be
// closed exactly once after the decompressor.
func TestTranslateBodyCloseStalledTrailer(t *testing.T) {
	r := require.New(t)
	full := compress(t, "gzip", []byte("some payload"))
	body := newEventBody()
	spyDecompressors(t, body, nil)
	body.chunks <- full[:len(full)-8] // everything but the CRC/size trailer
	rc, err := TranslateBody(&http.Response{
		Header: http.Header{"Content-Encoding": []string{"gzip"}},
		Body:   body,
	}, errors.OpQuery)
	r.NoError(err)

	done := make(chan error, 1)
	go func() {
		_, err := io.ReadAll(rc)
		done <- err
	}()
	time.Sleep(100 * time.Millisecond)
	events, closes, _ := body.snapshot()
	r.Zero(closes)
	r.Zero(count(events, "decomp-close"))

	close(body.chunks) // truncated stream: trailer never arrives
	select {
	case err := <-done:
		r.Error(err)
	case <-time.After(2 * time.Second):
		t.Fatal("read did not finish")
	}

	r.NoError(rc.Close())
	events, closes, mid := body.snapshot()
	r.Equal(1, closes, "body must be closed exactly once")
	r.False(mid)
	r.Equal([]string{"decomp-close", "body-close"}, events[len(events)-2:])
}

// TestTranslateBodyDecompressorCloseError checks that an error from the decompressor's Close is reported but does
// not stop the response body from being closed.
func TestTranslateBodyDecompressorCloseError(t *testing.T) {
	for _, enc := range []string{"gzip", "deflate"} {
		enc := enc
		t.Run(enc, func(t *testing.T) {
			r := require.New(t)
			boom := stderrors.New("decompressor close failed")
			body := newEventBody()
			spyDecompressors(t, body, boom)
			body.chunks <- compress(t, enc, []byte("payload"))
			close(body.chunks)
			rc, err := TranslateBody(&http.Response{
				Header: http.Header{"Content-Encoding": []string{enc}},
				Body:   body,
			}, errors.OpQuery)
			r.NoError(err)
			_, err = io.ReadAll(rc)
			r.NoError(err)

			err = rc.Close()
			r.ErrorIs(err, boom)
			events, closes, _ := body.snapshot()
			r.Equal(1, closes, "body must still be closed when the decompressor Close fails")
			r.Equal(1, count(events, "decomp-close"))
		})
	}
}
