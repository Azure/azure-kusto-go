package response

import (
	"compress/flate"
	"compress/gzip"
	stderrors "errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/Azure/azure-kusto-go/azkustodata/errors"
)

// NewGzipReader and NewFlateReader construct the decompressing readers; they are variables (in an internal package) so
// tests can wrap them and observe Read/Close order.
var (
	NewGzipReader  = func(r io.Reader) (io.ReadCloser, error) { return gzip.NewReader(r) }
	NewFlateReader = func(r io.Reader) io.ReadCloser { return flate.NewReader(r) }
)

type originalCloser struct {
	io.ReadCloser
	original io.Closer
}

func (o *originalCloser) Close() error {
	return stderrors.Join(o.ReadCloser.Close(), o.original.Close())
}

func TranslateBody(resp *http.Response, op errors.Op) (io.ReadCloser, error) {
	body := resp.Body
	var wrapper io.ReadCloser
	switch enc := strings.ToLower(resp.Header.Get("Content-Encoding")); enc {
	case "":
		return body, nil
	case "gzip":
		var err error
		wrapper, err = NewGzipReader(resp.Body)
		if err != nil {
			return nil, errors.E(op, errors.KInternal, fmt.Errorf("gzip reader error: %w", err))
		}
	case "deflate":
		wrapper = NewFlateReader(resp.Body)
	default:
		return nil, errors.ES(op, errors.KInternal, "Content-Encoding was unrecognized: %s", enc)
	}
	return &originalCloser{
		original:   body,
		ReadCloser: wrapper,
	}, nil
}
