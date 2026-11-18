package response

import (
	stderrors "errors"
	"io"
	"net/http"
	"strings"
	"testing"

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
