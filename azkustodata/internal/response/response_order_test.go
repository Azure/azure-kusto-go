package response

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
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

// TestTranslateBodyCloseOrder drives the real gzip/flate readers through a stalled payload and (for gzip) a stalled
// trailer and checks that the response body is read before it is closed, is never closed mid-read, and is closed
// exactly once.
func TestTranslateBodyCloseOrder(t *testing.T) {
	payload := bytes.Repeat([]byte("kusto-frame,"), 200)
	for _, enc := range []string{"gzip", "deflate"} {
		enc := enc
		t.Run(enc, func(t *testing.T) {
			r := require.New(t)
			full := compress(t, enc, payload)
			half := len(full) / 2

			body := newEventBody()
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
			_, closes, _ := body.snapshot()
			r.Zero(closes, "body closed while the payload is stalled")

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
			r.Equal("body-close", events[len(events)-1], "body close must come after every HTTP read")
			r.Contains(events, "http-read")
		})
	}
}

// TestTranslateBodyCloseStalledTrailer stalls a gzip stream before its trailer; once the stalled read is released
// with an error the decompressor's Close error must not stop the body from being closed.
func TestTranslateBodyCloseStalledTrailer(t *testing.T) {
	r := require.New(t)
	full := compress(t, "gzip", []byte("some payload"))
	body := newEventBody()
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
	_, closes, _ := body.snapshot()
	r.Zero(closes)

	close(body.chunks) // truncated stream: trailer never arrives
	select {
	case err := <-done:
		r.Error(err)
	case <-time.After(2 * time.Second):
		t.Fatal("read did not finish")
	}

	closeErr := rc.Close()
	_, closes, mid := body.snapshot()
	r.Equal(1, closes, "body must be closed exactly once even when the decompressor Close fails (err=%v)", closeErr)
	r.False(mid)
}
