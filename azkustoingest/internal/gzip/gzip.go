// Package gzip provides a streaming object for taking in io.ReadCloser that is being written to
// and providing an io.ReadCloser that outputs the original content gzip compressed.
package gzip

import (
	"compress/gzip"
	"io"
	"sync"
)

var compressPool = &sync.Pool{
	New: func() interface{} {
		return gzip.NewWriter(nil)
	},
}

// Streamer implements an io.ReadCloser that converts data from a non-compressed stream to a compressed stream.
type Streamer struct {
	userInput   io.ReadCloser
	outputRead  *io.PipeReader
	outputWrite *io.PipeWriter
	size        *int64
}

// New creates a new streamer object. Use Reset() to initialize it.
func New() *Streamer {
	return &Streamer{}
}

// Reset resets the streamer object to defaults and accepts the io.ReadCloser.
// You can only use Reset after a previous reader has closed.
func (s *Streamer) Reset(reader io.ReadCloser) {
	s.userInput = reader
	s.outputRead, s.outputWrite = io.Pipe()
	s.size = new(int64)

	s.run()
}

// InputSize returns the amount of data that the Streamer streamed. This will only be accurate for
// the full stream after Read() has returned io.EOF and not before.
func (s *Streamer) InputSize() int64 {
	if s.size == nil {
		return 0
	}
	return *s.size
}

func Compress(payload io.Reader) io.Reader {
	var closer io.ReadCloser
	var ok bool
	if closer, ok = payload.(io.ReadCloser); !ok {
		closer = io.NopCloser(payload)
	}
	zw := New()
	zw.Reset(closer)

	return zw
}

// run copies the file into a buffer that we stream back via our Read() call.
func (s *Streamer) run() {
	// The producer may outlive Close() while blocked on the source, so it must only use the
	// state of its own stream and never the fields that a later Reset() replaces.
	input, output, size := s.userInput, s.outputWrite, s.size

	zw := compressPool.Get().(*gzip.Writer)
	zw.Reset(output)

	go func() {
		defer compressPool.Put(zw)

		amount, err := io.Copy(zw, input)
		*size = amount

		if err == nil {
			err = zw.Close()
		}

		// A nil error ends the stream with io.EOF. On failure the gzip trailer is not written and
		// the consumer receives the error, so partial data is never taken as complete.
		_ = output.CloseWithError(err)
	}()
}

// Read implements io.Reader.
func (s *Streamer) Read(b []byte) (int, error) {
	amount, err := s.outputRead.Read(b)
	return amount, err
}

// Close implements io.Closer.
func (s *Streamer) Close() error {
	return s.outputRead.Close()
}
