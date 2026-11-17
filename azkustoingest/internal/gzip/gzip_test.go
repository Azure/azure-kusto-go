package gzip

import (
	"bytes"
	"compress/gzip"
	"io"
	"math/rand"
	"os"
	"strings"
	"testing"

	klauspostgzip "github.com/klauspost/compress/gzip"
	"github.com/stretchr/testify/require"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func TestCompressionProviders(t *testing.T) {
	t.Parallel()

	payload := []byte(strings.Repeat("gzip ingestion payload,12345\n", 1024))
	tests := []struct {
		name        string
		useStandard []bool
		newWriter   func(io.Writer) writer
	}{
		{"default", nil, func(w io.Writer) writer { return gzip.NewWriter(w) }},
		{"standard library", []bool{true}, func(w io.Writer) writer { return gzip.NewWriter(w) }},
		{"klauspost", []bool{false}, func(w io.Writer) writer { return klauspostgzip.NewWriter(w) }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var expected bytes.Buffer
			zw := test.newWriter(&expected)
			_, err := zw.Write(payload)
			require.NoError(t, err)
			require.NoError(t, zw.Flush())
			require.NoError(t, zw.Close())

			compressed, err := io.ReadAll(Compress(bytes.NewReader(payload), test.useStandard...))
			require.NoError(t, err)
			require.Equal(t, expected.Bytes(), compressed)

			zr, err := gzip.NewReader(bytes.NewReader(compressed))
			require.NoError(t, err)
			defer zr.Close()
			got, err := io.ReadAll(zr)
			require.NoError(t, err)
			require.Equal(t, payload, got)
		})
	}
}

func TestCompressionSelectionRejectsMultipleArguments(t *testing.T) {
	require.Panics(t, func() { New(true, false) })
	require.Panics(t, func() { Compress(bytes.NewReader(nil), true, false) })
}

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
