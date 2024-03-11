package gzip

import (
	"bytes"
	"compress/gzip"
	"io"
	"math/rand"
	"os"
	"testing"
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
