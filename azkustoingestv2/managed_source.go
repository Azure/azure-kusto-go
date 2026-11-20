// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package azkustoingestv2

import (
	"bytes"
	"context"
	"io"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/validation"
)

type managedLocalSource struct {
	ingestoptions.LocalSource
	prefix    []byte
	remainder io.Reader
}

func (s *managedLocalSource) Data() (io.ReadCloser, error) {
	if s.remainder != nil {
		return io.NopCloser(io.MultiReader(bytes.NewReader(s.prefix), s.remainder)), nil
	}
	return managedReplayReader{bytes.NewReader(s.prefix)}, nil
}

type managedReplayReader struct {
	*bytes.Reader
}

func (managedReplayReader) Close() error { return nil }

func (c *ManagedStreamingIngestClient) prepareLocalSource(ctx context.Context, source ingestoptions.LocalSource) (*managedLocalSource, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	stream, err := source.Data()
	if err != nil {
		return nil, false, ingestoptions.NewIngestClientError("failed to get source data", err, true)
	}
	if validation.IsNil(stream) {
		return nil, false, ingestoptions.NewIngestClientError("source data cannot be nil", nil, true)
	}

	// Retain only a streaming-sized prefix; larger inputs continue directly into the upload.
	data, err := io.ReadAll(io.LimitReader(stream, ingestoptions.StreamingMaxRequestBodySize+1))
	if err != nil {
		return nil, false, ingestoptions.NewIngestClientError("failed to read source data", err, false)
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	prepared := &managedLocalSource{LocalSource: source, prefix: data}
	useQueued := int64(len(data)) > ingestoptions.StreamingMaxRequestBodySize || c.shouldUseQueuedBySize(int64(len(data)))
	if int64(len(data)) > ingestoptions.StreamingMaxRequestBodySize {
		prepared.remainder = stream
	}
	return prepared, useQueued, nil
}
