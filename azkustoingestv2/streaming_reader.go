// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package azkustoingestv2

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"sync/atomic"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

// streamingSourceReader stops HTTP reads without closing the borrowed source.
type streamingSourceReader struct {
	source io.Reader
	closed atomic.Bool
}

func (r *streamingSourceReader) Read(p []byte) (int, error) {
	if r.closed.Load() {
		return 0, io.ErrClosedPipe
	}
	return r.source.Read(p)
}

func (r *streamingSourceReader) Close() error {
	r.closed.Store(true)
	return nil
}

// streamingGzipReader compresses on HTTP reads without closing the borrowed source.
type streamingGzipReader struct {
	streamingSourceReader
	output bytes.Buffer
	writer *gzip.Writer
	buffer [ingestoptions.StreamCompressionBufferSize]byte
	err    error
}

func newStreamingGzipReader(source io.Reader) *streamingGzipReader {
	r := &streamingGzipReader{streamingSourceReader: streamingSourceReader{source: source}}
	r.writer = gzip.NewWriter(&r.output)
	return r
}

func (r *streamingGzipReader) Read(p []byte) (int, error) {
	if r.closed.Load() {
		return 0, io.ErrClosedPipe
	}
	if len(p) == 0 {
		return 0, nil
	}
	for r.output.Len() == 0 {
		if r.err != nil {
			return 0, r.err
		}
		n, readErr := r.source.Read(r.buffer[:])
		if r.closed.Load() {
			return 0, io.ErrClosedPipe
		}
		if n > 0 {
			if _, writeErr := r.writer.Write(r.buffer[:n]); writeErr != nil {
				r.err = errors.Join(writeErr, readErr, r.writer.Close())
			}
		}
		if r.err == nil && readErr != nil {
			closeErr := r.writer.Close()
			if readErr == io.EOF && closeErr == nil {
				r.err = io.EOF
			} else if readErr == io.EOF {
				r.err = closeErr
			} else {
				r.err = errors.Join(readErr, closeErr)
			}
		}
		if n == 0 && readErr == nil {
			return 0, nil
		}
	}
	return r.output.Read(p)
}
