package response

import (
	"compress/flate"
	"compress/gzip"
	stderrors "errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/Azure/azure-kusto-go/azkustodata/errors"
)

type originalCloser struct {
	io.ReadCloser
	original  io.Closer
	readMu    sync.Mutex
	closed    bool
	closeOnce sync.Once
	closeErr  error
}

func (o *originalCloser) Read(p []byte) (int, error) {
	o.readMu.Lock()
	defer o.readMu.Unlock()
	if o.closed {
		return 0, io.ErrClosedPipe
	}
	return o.ReadCloser.Read(p)
}

func (o *originalCloser) Close() error {
	o.closeOnce.Do(func() {
		// Interrupt a blocked read before waiting to release the decompressor.
		originalErr := o.original.Close()
		o.readMu.Lock()
		defer o.readMu.Unlock()
		o.closed = true
		o.closeErr = stderrors.Join(o.ReadCloser.Close(), originalErr)
	})
	return o.closeErr
}

func TranslateBody(resp *http.Response, op errors.Op) (io.ReadCloser, error) {
	body := resp.Body
	var wrapper io.ReadCloser
	switch enc := strings.ToLower(resp.Header.Get("Content-Encoding")); enc {
	case "":
		return body, nil
	case "gzip":
		var err error
		wrapper, err = gzip.NewReader(resp.Body)
		if err != nil {
			return nil, errors.E(op, errors.KInternal, fmt.Errorf("gzip reader error: %w", err))
		}
	case "deflate":
		wrapper = flate.NewReader(resp.Body)
	default:
		return nil, errors.ES(op, errors.KInternal, "Content-Encoding was unrecognized: %s", enc)
	}
	return &originalCloser{
		original:   body,
		ReadCloser: wrapper,
	}, nil
}
