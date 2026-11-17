// Package gzip provides a streaming object for taking in io.ReadCloser that is being written to
// and providing an io.ReadCloser that outputs the original content gzip compressed.
package gzip

import (
	"compress/gzip"
	"io"
	"sync"
	"sync/atomic"

	klauspostgzip "github.com/klauspost/compress/gzip"
)

var compressPool = &sync.Pool{
	New: func() interface{} {
		return gzip.NewWriter(nil)
	},
}

var klauspostCompressPool = &sync.Pool{
	New: func() interface{} {
		return klauspostgzip.NewWriter(nil)
	},
}

type writer interface {
	io.WriteCloser
	Flush() error
	Reset(io.Writer)
}

// Streamer implements an io.ReadCloser that converts data from a non-compressed stream to a compressed stream.
type Streamer struct {
	userInput                     io.ReadCloser
	outputRead                    *io.PipeReader
	outputWrite                   *io.PipeWriter
	size                          int64
	err                           atomic.Value // holds error
	useStandardLibraryCompression bool
}

// New creates a new streamer object. Use Reset() to initialize it.
// With no argument or true, it uses standard-library gzip; false selects klauspost gzip.
// At most one argument may be supplied.
func New(useStandardLibraryCompression ...bool) *Streamer {
	if len(useStandardLibraryCompression) > 1 {
		panic("gzip.New: at most one compression selection is allowed")
	}
	s := &Streamer{useStandardLibraryCompression: true}
	if len(useStandardLibraryCompression) == 1 {
		s.useStandardLibraryCompression = useStandardLibraryCompression[0]
	}
	return s
}

// Reset resets the streamer object to defaults and accepts the io.ReadCloser.
// You can only use Reset after a previous reader has closed.
func (s *Streamer) Reset(reader io.ReadCloser) {
	s.userInput = reader
	s.outputRead, s.outputWrite = io.Pipe()
	s.size = 0
	s.err = atomic.Value{}

	s.run()
}

// InputSize returns the amount of data that the Streamer streamed. This will only be accurate for
// the full stream after Read() has returned io.EOF and not before.
func (s *Streamer) InputSize() int64 {
	return s.size
}

// Compress returns a gzip-compressed reader, using standard-library gzip by default.
// Its optional compression selection has the same behavior as New.
func Compress(payload io.Reader, useStandardLibraryCompression ...bool) io.Reader {
	var closer io.ReadCloser
	var ok bool
	if closer, ok = payload.(io.ReadCloser); !ok {
		closer = io.NopCloser(payload)
	}
	zw := New(useStandardLibraryCompression...)
	zw.Reset(closer)

	return zw
}

// run copies the file into a buffer that we stream back via our Read() call.
func (s *Streamer) run() {
	pool := compressPool
	if !s.useStandardLibraryCompression {
		pool = klauspostCompressPool
	}
	zw := pool.Get().(writer)
	zw.Reset(s.outputWrite)

	go func() {
		defer pool.Put(zw)
		defer s.outputWrite.Close()
		defer zw.Close()
		defer zw.Flush()

		amount, err := io.Copy(zw, s.userInput)
		s.size = int64(amount)

		if err != nil {
			s.err.Store(err)
		}
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
