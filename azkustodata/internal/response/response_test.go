package response

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	stderrors "errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Azure/azure-kusto-go/azkustodata/errors"
)

type trackingBody struct {
	io.Reader
	closeErr error
	closed   bool
}

func (b *trackingBody) Close() error {
	b.closed = true
	return b.closeErr
}

type errReader struct {
	err error
}

func (r errReader) Read([]byte) (int, error) {
	return 0, r.err
}

// regression test: a mid-read error (e.g. a canceled request context) sticks
// in the decompressor, and its Close returning it made originalCloser skip
// closing the underlying body
func TestTranslateBodyClose(t *testing.T) {
	r := require.New(t)
	errBodyRead := stderrors.New("body read failed")

	body := &trackingBody{Reader: errReader{err: errBodyRead}}
	resp := &http.Response{
		Header: http.Header{"Content-Encoding": []string{"deflate"}},
		Body:   body,
	}

	rc, err := TranslateBody(resp, errors.OpQuery)
	r.NoError(err)

	_, err = io.ReadAll(rc)
	r.ErrorIs(err, errBodyRead)

	r.ErrorIs(rc.Close(), errBodyRead)
	r.True(body.closed, "underlying response body must be closed even when the read failed mid-stream")
}

func TestOriginalCloserClose(t *testing.T) {
	errWrapper := stderrors.New("wrapper close failed")
	errOriginal := stderrors.New("original close failed")

	tests := []struct {
		name        string
		wrapperErr  error
		originalErr error
	}{
		{name: "no errors"},
		{name: "wrapper fails", wrapperErr: errWrapper},
		{name: "original fails", originalErr: errOriginal},
		{name: "both fail", wrapperErr: errWrapper, originalErr: errOriginal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)

			wrapper := &trackingBody{closeErr: tt.wrapperErr}
			original := &trackingBody{closeErr: tt.originalErr}

			closer := &originalCloser{original: original, ReadCloser: wrapper}
			err := closer.Close()

			r.True(wrapper.closed)
			r.True(original.closed)
			if tt.wrapperErr == nil && tt.originalErr == nil {
				r.NoError(err)
			}
			if tt.wrapperErr != nil {
				r.ErrorIs(err, tt.wrapperErr)
			}
			if tt.originalErr != nil {
				r.ErrorIs(err, tt.originalErr)
			}
		})
	}
}

func TestOriginalCloserRead(t *testing.T) {
	r := require.New(t)

	oc := &originalCloser{
		original:   &trackingBody{},
		ReadCloser: io.NopCloser(strings.NewReader("kusto payload")),
	}

	data, err := io.ReadAll(oc)
	r.NoError(err)
	r.Equal("kusto payload", string(data))
}

// Close must interrupt the transport before touching an active decompressor.
func TestTranslateBodyCloseInterruptsCompressedRead(t *testing.T) {
	for _, encoding := range []string{"gzip", "deflate"} {
		t.Run(encoding, func(t *testing.T) {
			var compressed bytes.Buffer
			var writer interface {
				Write([]byte) (int, error)
				Flush() error
			}
			if encoding == "gzip" {
				writer = gzip.NewWriter(&compressed)
			} else {
				w, err := flate.NewWriter(&compressed, flate.DefaultCompression)
				require.NoError(t, err)
				writer = w
			}
			_, err := writer.Write([]byte("[payload\n"))
			require.NoError(t, err)
			require.NoError(t, writer.Flush())
			body := &compressedStalledBody{reader: bytes.NewReader(compressed.Bytes()), reading: make(chan struct{}), closed: make(chan struct{})}
			rc, err := TranslateBody(&http.Response{Body: body, Header: http.Header{"Content-Encoding": []string{encoding}}}, errors.OpQuery)
			require.NoError(t, err)
			done := make(chan struct{})
			go func() { defer close(done); _, _ = io.Copy(io.Discard, rc) }()
			select {
			case <-body.reading:
			case <-time.After(time.Second):
				t.Fatal("read did not stall")
			}
			closeDone := make(chan struct{})
			go func() { _ = rc.Close(); close(closeDone) }()
			select {
			case <-closeDone:
			case <-time.After(time.Second):
				// Release the reader even if a regression deadlocks Close.
				body.Close()
				<-closeDone
				t.Fatal("Close did not interrupt compressed read")
			}
			<-done
			_ = rc.Close()
		})
	}
}

type compressedStalledBody struct {
	reader  *bytes.Reader
	reading chan struct{}
	closed  chan struct{}
	once    sync.Once
}

func (b *compressedStalledBody) Read(p []byte) (int, error) {
	if b.reader.Len() > 0 {
		return b.reader.Read(p)
	}
	close(b.reading)
	<-b.closed
	return 0, io.ErrClosedPipe
}
func (b *compressedStalledBody) Close() error {
	b.once.Do(func() { close(b.closed) })
	return nil
}

func TestOriginalCloserClosesTransportFirst(t *testing.T) {
	original := &trackingBody{}
	wrapper := &transportOrderCloser{t: t, original: original}
	closer := &originalCloser{original: original, ReadCloser: wrapper}
	require.NoError(t, closer.Close())
}

type transportOrderCloser struct {
	t        *testing.T
	original *trackingBody
}

func (c *transportOrderCloser) Read([]byte) (int, error) { return 0, io.EOF }
func (c *transportOrderCloser) Close() error {
	require.True(c.t, c.original.closed, "transport must close before decompressor")
	return nil
}
