// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package upload

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

func TestUploadSizeValidation(t *testing.T) {
	for _, method := range []ingestoptions.UploadMethod{ingestoptions.UploadMethodStorage, ingestoptions.UploadMethodLake} {
		for _, format := range []ingestoptions.DataFormat{ingestoptions.FormatCSV, ingestoptions.FormatParquet} {
			for _, file := range []bool{false, true} {
				for _, tc := range []struct {
					name    string
					payload string
					ignore  bool
					code    ingestoptions.UploadErrorCode
				}{
					{"empty", "", false, ingestoptions.UploadErrorSourceIsEmpty},
					{"empty-ignore-limit", "", true, ingestoptions.UploadErrorSourceIsEmpty},
					{"boundary", "12345678", false, ""},
					{"oversize", "123456789", false, ingestoptions.UploadErrorSourceSizeLimitExceed},
					{"ignore-limit", "123456789", true, ""},
				} {
					t.Run(fmt.Sprintf("%s/%s/file=%v/%s", method, format, file, tc.name), func(t *testing.T) {
						requests := make(chan uploadRequest, 16)
						server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							recordUpload(t, requests, w, r)
						}))
						defer server.Close()
						var source ingestoptions.LocalSource
						if file {
							path := filepath.Join(t.TempDir(), "input")
							if err := os.WriteFile(path, []byte(tc.payload), 0600); err != nil {
								t.Fatal(err)
							}
							source = ingestoptions.NewFileSource(path, format)
						} else {
							source = ingestoptions.NewStreamSource(
								io.NopCloser(iotest.OneByteReader(strings.NewReader(tc.payload))), format)
						}
						t.Cleanup(func() { _ = source.Close() })
						u := newTestUploader(t, method, []string{server.URL + "/container"},
							WithUploaderMaxDataSize(8), WithUploaderIgnoreSizeLimit(tc.ignore))
						blob, err := u.Upload(context.Background(), source)
						if tc.code != "" {
							var uploadErr *ingestoptions.UploadFailedError
							if blob != nil || !errors.As(err, &uploadErr) || uploadErr.ErrorCode != tc.code {
								t.Fatalf("expected %s, got blob=%v err=%v", tc.code, blob, err)
							}
							if file && len(requests) != 0 {
								t.Fatalf("known invalid file reached storage: %d requests", len(requests))
							}
							return
						}
						if err != nil {
							t.Fatal(err)
						}
						var uploaded []byte
						for len(requests) > 0 {
							req := <-requests
							if req.query.Get("action") == "append" ||
								method == ingestoptions.UploadMethodStorage && req.query.Get("comp") != "blocklist" {
								uploaded = append(uploaded, req.body...)
							}
						}
						if source.ShouldCompress() {
							reader, err := gzip.NewReader(bytes.NewReader(uploaded))
							if err != nil {
								t.Fatal(err)
							}
							uploaded, err = io.ReadAll(reader)
							_ = reader.Close()
							if err != nil {
								t.Fatal(err)
							}
						}
						if string(uploaded) != tc.payload {
							t.Fatalf("uploaded %q, want %q", uploaded, tc.payload)
						}
					})
				}
			}
		}
	}
}

func TestUploadSizeReaderBoundsReadsAndPreservesErrors(t *testing.T) {
	raw := bytes.NewReader(bytes.Repeat([]byte("x"), 32))
	checked := &uploadSizeReader{reader: raw, name: "input", max: 8}
	_, err := io.ReadAll(checked)
	var uploadErr *ingestoptions.UploadFailedError
	if !errors.As(err, &uploadErr) || uploadErr.ErrorCode != ingestoptions.UploadErrorSourceSizeLimitExceed || raw.Len() != 23 {
		t.Fatalf("limit must fail after at most max+1 source bytes: err=%v remaining=%d", err, raw.Len())
	}
	want := errors.New("source read failed")
	checked = &uploadSizeReader{
		reader: io.MultiReader(strings.NewReader("data"), iotest.ErrReader(want)),
		name:   "input", max: 1, ignore: true,
	}
	_, err = io.ReadAll(checked)
	if !errors.Is(err, want) {
		t.Fatalf("ignoring size limit hid source error: %v", err)
	}
}
