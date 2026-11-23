// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package test

import (
	"testing"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

func TestIngestError(t *testing.T) {
	err := ingestoptions.NewIngestError("test error", nil, false)
	if err.Error() != "test error" {
		t.Errorf("unexpected error message: %s", err.Error())
	}
	if err.IsPermanent {
		t.Error("expected non-permanent error")
	}
}

func TestIngestErrorPermanent(t *testing.T) {
	err := ingestoptions.NewIngestError("permanent error", nil, true)
	if !err.IsPermanent {
		t.Error("expected permanent error")
	}
}

func TestIngestClientError(t *testing.T) {
	err := ingestoptions.NewIngestClientError("client error", nil, false)
	if err.Error() != "client error" {
		t.Errorf("unexpected error message: %s", err.Error())
	}
}

func TestIngestRequestError(t *testing.T) {
	err := &ingestoptions.IngestRequestError{
		IngestError: ingestoptions.IngestError{
			Message:     "request error",
			IsPermanent: true,
		},
	}
	if err.Error() != "request error" {
		t.Errorf("unexpected error message: %s", err.Error())
	}
	if !err.IsPermanent {
		t.Error("expected permanent error")
	}
}

func TestIngestSizeLimitExceededError(t *testing.T) {
	err := ingestoptions.NewIngestSizeLimitExceededError(100, 50)
	if err.ActualSize != 100 {
		t.Errorf("expected actual size 100, got %d", err.ActualSize)
	}
	if err.MaxAllowedSize != 50 {
		t.Errorf("expected max size 50, got %d", err.MaxAllowedSize)
	}
	if !err.IsPermanent {
		t.Error("size limit exceeded should be permanent")
	}
}
