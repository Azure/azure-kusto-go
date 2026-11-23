// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package upload

import (
	"fmt"
	"io"
	"os"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

func remainingUploadSize(stream io.Reader) (int64, error) {
	if sizer, ok := stream.(interface{ Len() int }); ok {
		return int64(sizer.Len()), nil
	}
	if file, ok := stream.(interface {
		Stat() (os.FileInfo, error)
		io.Seeker
	}); ok {
		info, err := file.Stat()
		if err != nil {
			return -1, err
		}
		if info.Mode().IsRegular() {
			offset, err := file.Seek(0, io.SeekCurrent)
			if err != nil {
				return -1, err
			}
			if offset >= info.Size() {
				return 0, nil
			}
			return info.Size() - offset, nil
		}
	}
	return -1, nil
}

type uploadSizeReader struct {
	reader io.Reader
	name   string
	max    int64
	ignore bool
	read   int64
	err    error
}

func (r *uploadSizeReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.err != nil {
		return 0, r.err
	}
	if !r.ignore {
		remaining := r.max - r.read
		if remaining < 0 {
			remaining = 0
		}
		if int64(len(p)) > remaining {
			// Probe one byte past the limit to distinguish overflow from EOF at the boundary.
			p = p[:remaining+1]
		}
	}
	n, err := r.reader.Read(p)
	r.read += int64(n)
	switch {
	case !r.ignore && r.read > r.max:
		r.err = ingestoptions.NewUploadFailedError(
			ingestoptions.UploadErrorSourceSizeLimitExceed, r.name,
			fmt.Errorf("source exceeds max size %d", r.max),
		)
		return 0, r.err
	case err != nil && err != io.EOF:
		r.err = ingestoptions.NewUploadFailedError(ingestoptions.UploadErrorSourceNotReadable, r.name, err)
	case err == io.EOF && r.read == 0:
		r.err = ingestoptions.NewUploadFailedError(ingestoptions.UploadErrorSourceIsEmpty, r.name, nil)
	}
	if r.err != nil {
		return n, r.err
	}
	return n, err
}
