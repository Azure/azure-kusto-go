// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package test

import (
	"testing"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

// TestUploadErrorCodes verifies all upload error codes are distinct and correctly defined.
func TestUploadErrorCodes(t *testing.T) {
	t.Parallel()

	codes := []ingestoptions.UploadErrorCode{
		ingestoptions.UploadErrorSourceIsNull,
		ingestoptions.UploadErrorSourceNotFound,
		ingestoptions.UploadErrorSourceNotReadable,
		ingestoptions.UploadErrorSourceIsEmpty,
		ingestoptions.UploadErrorSourceSizeLimitExceed,
		ingestoptions.UploadErrorNoContainers,
		ingestoptions.UploadErrorUploadFailed,
		ingestoptions.UploadErrorContainerNotFound,
		ingestoptions.UploadErrorCompressionFailed,
		ingestoptions.UploadErrorUnknown,
	}

	seen := make(map[ingestoptions.UploadErrorCode]bool)
	for _, code := range codes {
		if seen[code] {
			t.Errorf("duplicate upload error code: %s", code)
		}
		seen[code] = true
		if code == "" {
			t.Error("upload error code should not be empty")
		}
	}
}

// TestUploadFailedError_Creation verifies UploadFailedError construction.
func TestUploadFailedError_Creation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		code     ingestoptions.UploadErrorCode
		fileName string
	}{
		{"source_null", ingestoptions.UploadErrorSourceIsNull, ""},
		{"source_empty", ingestoptions.UploadErrorSourceIsEmpty, "data.csv"},
		{"size_exceeded", ingestoptions.UploadErrorSourceSizeLimitExceed, "large.parquet"},
		{"no_containers", ingestoptions.UploadErrorNoContainers, "data.json"},
		{"upload_failed", ingestoptions.UploadErrorUploadFailed, "blob.csv.gz"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ingestoptions.NewUploadFailedError(tt.code, tt.fileName, nil)
			if err == nil {
				t.Fatal("expected non-nil error")
			}
			if err.ErrorCode != tt.code {
				t.Errorf("expected code %s, got %s", tt.code, err.ErrorCode)
			}
			if err.FileName != tt.fileName {
				t.Errorf("expected fileName %q, got %q", tt.fileName, err.FileName)
			}
			if !err.IsPermanent {
				t.Error("UploadFailedError should be permanent by default")
			}
			errMsg := err.Error()
			if errMsg == "" {
				t.Error("error message should not be empty")
			}
		})
	}
}

// TestInvalidUploadStreamError verifies error for invalid streams.
func TestInvalidUploadStreamError(t *testing.T) {
	t.Parallel()

	err := &ingestoptions.InvalidUploadStreamError{
		IngestError: ingestoptions.IngestError{
			Message:     "cannot open file \"/tmp/missing.csv\": no such file",
			IsPermanent: true,
		},
		FileName: "/tmp/missing.csv",
	}

	if err.FileName != "/tmp/missing.csv" {
		t.Errorf("expected FileName '/tmp/missing.csv', got %q", err.FileName)
	}
	if !err.IsPermanent {
		t.Error("file-not-found should be permanent")
	}
}

// TestIngestSizeLimitExceededError_Enhanced verifies size limit error with specific values.
func TestIngestSizeLimitExceededError_Enhanced(t *testing.T) {
	t.Parallel()

	err := ingestoptions.NewIngestSizeLimitExceededError(20*1024*1024, 10*1024*1024)
	if err.ActualSize != 20*1024*1024 {
		t.Errorf("expected actualSize 20MB, got %d", err.ActualSize)
	}
	if err.MaxAllowedSize != 10*1024*1024 {
		t.Errorf("expected maxAllowedSize 10MB, got %d", err.MaxAllowedSize)
	}
	if !err.IsPermanent {
		t.Error("size limit exceeded should be permanent")
	}
}

// TestSourceBlobNaming verifies blob name generation for different source types.
func TestSourceBlobNaming(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		format     ingestoptions.DataFormat
		compress   ingestoptions.CompressionType
		expectGZ   bool
	}{
		{"csv_uncompressed_gets_gz", ingestoptions.FormatCSV, ingestoptions.CompressionNone, true},
		{"csv_already_gzip_stays_gz", ingestoptions.FormatCSV, ingestoptions.CompressionGZip, true},
		{"parquet_binary_no_gz", ingestoptions.FormatParquet, ingestoptions.CompressionNone, false},
		{"json_uncompressed_gets_gz", ingestoptions.FormatJSON, ingestoptions.CompressionNone, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			blob := mustBlobSource(t, "https://storage/data", tt.format,
				ingestoptions.WithBlobCompression(tt.compress))
			name := blob.Name()
			if name == "" {
				t.Error("blob name should not be empty")
			}

			// For local sources, test GenerateBlobName
			source := ingestoptions.NewStreamSource(nil, tt.format,
				ingestoptions.WithStreamCompression(tt.compress))
			blobName := source.GenerateBlobName()
			hasGZ := len(blobName) > 3 && blobName[len(blobName)-3:] == ".gz"
			if tt.expectGZ && !hasGZ {
				t.Errorf("expected .gz extension in blob name %q", blobName)
			}
			if !tt.expectGZ && hasGZ {
				t.Errorf("expected no .gz extension in blob name %q", blobName)
			}
		})
	}
}
