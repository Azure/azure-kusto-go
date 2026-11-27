package gzip

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func TestStreamer(t *testing.T) {
	t.Parallel()

	str := randStringBytes(4 * 1024 * 1024)

	f, err := os.CreateTemp("", "")
	if err != nil {
		panic(err)
	}
	_, err = f.Write([]byte(str))
	if err != nil {
		panic(err)
	}
	_ = f.Close()

	r, err := os.Open(f.Name())
	if err != nil {
		panic(err)
	}

	streamer := New()
	streamer.Reset(r)

	compressedBuf := bytes.Buffer{}
	if _, err := io.Copy(&compressedBuf, streamer); err != nil {
		t.Fatalf("TestStreamer: got err == %s, want err == nil", err)
	}

	gzipReader, err := gzip.NewReader(&compressedBuf)
	if err != nil {
		t.Fatalf("TestStreamer(gzip.NewReader(compressedBuf)): got err == %s, want err == nil", err)
	}

	gotBuf := bytes.Buffer{}
	if _, err := io.Copy(&gotBuf, gzipReader); err != nil {
		t.Fatalf("TestStreamer(decompressing stream, len==%d): got err == %s, want err == nil", gotBuf.Len(), err)
	}

	if gotBuf.String() != str {
		t.Fatalf("TestStreamer(input/output comparison): after compression/decompression the data was not the same")
	}

	if int64(len(str)) != streamer.InputSize() {
		t.Fatalf("TestStreamer(InputSize): got %d, want %d", streamer.InputSize(), len(str))
	}
}

type failingReader struct {
	data []byte
	err  error
}

func (f *failingReader) Read(b []byte) (int, error) {
	n := copy(b, f.data)
	f.data = f.data[n:]
	return n, f.err
}

func TestStreamerReadError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("source read failed")
	tests := map[string]io.Reader{
		"data returned with the error": &failingReader{data: []byte("a,b,c\n"), err: wantErr},
		"error after a successful read": io.MultiReader(
			strings.NewReader("a,b,c\n"),
			&failingReader{err: wantErr},
		),
	}

	for name, source := range tests {
		t.Run(name, func(t *testing.T) {
			compressed := bytes.Buffer{}
			if _, err := io.Copy(&compressed, Compress(source)); !errors.Is(err, wantErr) {
				t.Fatalf("got err == %v, want %v", err, wantErr)
			}

			// The partial data must not form a complete gzip stream.
			gzipReader, err := gzip.NewReader(&compressed)
			if err == nil {
				_, err = io.ReadAll(gzipReader)
			}
			if err == nil {
				t.Fatalf("decompressing a failed stream: got err == nil, want err != nil")
			}
		})
	}
}

// blockingReader signals when Read is called and then waits to be released before failing.
type blockingReader struct {
	started chan struct{}
	release chan struct{}
}

func (r *blockingReader) Read([]byte) (int, error) {
	close(r.started)
	<-r.release
	return 0, errors.New("first source read failed")
}

func TestStreamerResetWhileSourceBlocked(t *testing.T) {
	t.Parallel()

	first := &blockingReader{started: make(chan struct{}), release: make(chan struct{})}
	streamer := New()
	streamer.Reset(io.NopCloser(first))
	<-first.started
	if err := streamer.Close(); err != nil {
		t.Fatalf("Close: got err == %v, want nil", err)
	}

	secondRead, secondWrite := io.Pipe()
	streamer.Reset(secondRead)

	close(first.release)
	// Give the first producer time to finish while the second stream is still open.
	time.Sleep(100 * time.Millisecond)

	const want = "d,e,f\n"
	go func() {
		_, err := io.WriteString(secondWrite, want)
		_ = secondWrite.CloseWithError(err)
	}()

	gzipReader, err := gzip.NewReader(streamer)
	if err != nil {
		t.Fatalf("gzip.NewReader: got err == %v, want nil", err)
	}
	got, err := io.ReadAll(gzipReader)
	if err != nil {
		t.Fatalf("reading the second stream: got err == %v, want nil", err)
	}
	if string(got) != want {
		t.Fatalf("second stream: got %q, want %q", got, want)
	}
	if streamer.InputSize() != int64(len(want)) {
		t.Fatalf("InputSize: got %d, want %d", streamer.InputSize(), len(want))
	}
}
