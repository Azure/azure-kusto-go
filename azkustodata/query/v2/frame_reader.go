package v2

import (
	"bufio"
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"io"
)

type frameReader struct {
	orig   io.ReadCloser
	reader bufio.Reader
	line   []byte
	ctx    context.Context
}

func newFrameReader(r io.ReadCloser, ctx context.Context) (*frameReader, error) {
	br, err := prepareReadBuffer(r)
	if err != nil {
		return nil, err
	}
	return &frameReader{orig: r, reader: *br, ctx: ctx}, nil
}

func prepareReadBuffer(r io.Reader) (*bufio.Reader, error) {
	br := bufio.NewReader(r)
	first, err := br.Peek(1)
	if err != nil {
		return nil, err
	}
	if len(first) == 0 {
		return nil, errors.ES(errors.OpUnknown, errors.KInternal, "No data")
	}

	if first[0] != '[' {
		all, err := io.ReadAll(br)
		if err != nil {
			return nil, err
		}
		return nil, errors.ES(errors.OpUnknown, errors.KInternal, "Got error: %v", string(all))
	}
	return br, nil
}

func (fr *frameReader) advance() error {
	if fr.ctx.Err() != nil {
		return fr.ctx.Err()
	}
	line, err := fr.reader.ReadBytes('\n')
	if err != nil {
		return err
	}
	if len(line) == 0 {
		return errors.ES(errors.OpUnknown, errors.KInternal, "No data")
	}

	if line[0] == ']' {
		return io.EOF
	}

	fr.line = line[1:]

	return nil
}

// Close closes the underlying reader.
func (fr *frameReader) Close() error {
	return fr.orig.Close()
}
