// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package ingestoptions

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/validation"
	"github.com/google/uuid"
)

// IngestionSource is the base interface for all ingestion sources.
type IngestionSource interface {
	io.Closer
	// Format returns the data format of this source.
	Format() DataFormat
	// CompressionType returns the compression type of this source.
	CompressionType() CompressionType
	// SourceID returns the unique identifier for this source.
	SourceID() uuid.UUID
	// Name returns the name of this source.
	Name() string
}

// LocalSource is a source backed by local data (file or stream).
type LocalSource interface {
	IngestionSource
	// Data returns the data stream for ingestion.
	Data() (io.ReadCloser, error)
	// Size returns the approximate size of the data in bytes, or -1 if unknown.
	Size() int64
	// ShouldCompress returns true if the data should be compressed during ingestion.
	ShouldCompress() bool
	// GenerateBlobName generates a unique blob name for upload.
	GenerateBlobName() string
}

// LocalSourceOption configures both file and stream sources.
type LocalSourceOption interface {
	FileSourceOption
	StreamSourceOption
}

// DontCompress disables automatic compression without changing the source compression type.
func DontCompress() LocalSourceOption {
	return dontCompressOption{}
}

type dontCompressOption struct{}

func (dontCompressOption) applyToFile(s *FileSource)     { s.dontCompress = true }
func (dontCompressOption) applyToStream(s *StreamSource) { s.dontCompress = true }

// BlobSource represents a blob-based ingestion source. This source references
// data that already exists in blob storage.
type BlobSource struct {
	blobPath        string
	format          DataFormat
	compressionType CompressionType
	sourceID        uuid.UUID
	name            string
	// BlobExactSize is the exact size of the blob in bytes, or zero if unknown.
	BlobExactSize int64
}

// NewBlobSource creates a new BlobSource.
func NewBlobSource(blobPath string, format DataFormat, opts ...BlobSourceOption) (*BlobSource, error) {
	if strings.TrimSpace(blobPath) == "" {
		return nil, fmt.Errorf("blobPath cannot be blank")
	}
	s := &BlobSource{
		blobPath:        blobPath,
		format:          format,
		compressionType: CompressionNone,
		sourceID:        uuid.New(),
	}
	for _, opt := range opts {
		opt(s)
	}
	s.name = fmt.Sprintf("blob_%s%s%s", sanitizeUUID(s.sourceID), s.format, s.compressionType.String())
	return s, nil
}

// BlobSourceOption configures a BlobSource.
type BlobSourceOption func(*BlobSource)

// WithBlobCompression sets the compression type for a BlobSource.
func WithBlobCompression(ct CompressionType) BlobSourceOption {
	return func(s *BlobSource) { s.compressionType = ct }
}

// WithBlobSourceID sets the source ID for a BlobSource.
func WithBlobSourceID(id uuid.UUID) BlobSourceOption {
	return func(s *BlobSource) { s.sourceID = id }
}

func (s *BlobSource) Format() DataFormat               { return s.format }
func (s *BlobSource) CompressionType() CompressionType { return s.compressionType }
func (s *BlobSource) SourceID() uuid.UUID              { return s.sourceID }
func (s *BlobSource) Name() string                     { return s.name }
func (s *BlobSource) Close() error                     { return nil }

// BlobPath returns the blob storage path.
func (s *BlobSource) BlobPath() string { return s.blobPath }

// PathForTracing returns the blob path without SAS token for tracing.
func (s *BlobSource) PathForTracing() string {
	parts := strings.SplitN(s.blobPath, "?", 2)
	return parts[0]
}

// Size returns the exact blob size if known, or zero.
func (s *BlobSource) Size() int64 { return s.BlobExactSize }

// FileSource represents a file-based ingestion source.
type FileSource struct {
	path            string
	format          DataFormat
	compressionType CompressionType
	sourceID        uuid.UUID
	name            string
	stream          io.ReadCloser
	dontCompress    bool
}

// NewFileSource creates a new FileSource.
func NewFileSource(path string, format DataFormat, opts ...FileSourceOption) *FileSource {
	s := &FileSource{
		path:     path,
		format:   format,
		sourceID: uuid.New(),
	}
	for _, opt := range opts {
		opt.applyToFile(s)
	}
	if s.compressionType == CompressionNone {
		s.compressionType = detectCompressionFromPath(path)
	}
	s.name = fmt.Sprintf("file_%s%s%s", sanitizeUUID(s.sourceID), s.format, s.compressionType.String())
	return s
}

// FileSourceOption configures a FileSource.
type FileSourceOption interface {
	applyToFile(*FileSource)
}

type fileSourceOptionFunc func(*FileSource)

func (f fileSourceOptionFunc) applyToFile(s *FileSource) {
	f(s)
}

// WithFileCompression sets the compression type for a FileSource.
func WithFileCompression(ct CompressionType) FileSourceOption {
	return fileSourceOptionFunc(func(s *FileSource) { s.compressionType = ct })
}

// WithFileSourceID sets the source ID for a FileSource.
func WithFileSourceID(id uuid.UUID) FileSourceOption {
	return fileSourceOptionFunc(func(s *FileSource) { s.sourceID = id })
}

func (s *FileSource) Format() DataFormat               { return s.format }
func (s *FileSource) CompressionType() CompressionType { return s.compressionType }
func (s *FileSource) SourceID() uuid.UUID              { return s.sourceID }
func (s *FileSource) Name() string                     { return s.name }
func (s *FileSource) Path() string                     { return s.path }

// Data returns an io.ReadCloser for the file.
func (s *FileSource) Data() (io.ReadCloser, error) {
	if s.stream != nil {
		return s.stream, nil
	}
	f, err := os.Open(s.path)
	if err != nil {
		return nil, &InvalidUploadStreamError{
			IngestError: IngestError{
				Message:     fmt.Sprintf("cannot open file %q: %v", s.path, err),
				Cause:       err,
				IsPermanent: os.IsNotExist(err),
			},
			FileName: s.path,
		}
	}
	s.stream = f
	return f, nil
}

// Size returns the file size in bytes, or -1 if it cannot be determined.
func (s *FileSource) Size() int64 {
	info, err := os.Stat(s.path)
	if err != nil {
		return -1
	}
	return info.Size()
}

// ShouldCompress returns true if the file data should be compressed during ingestion.
func (s *FileSource) ShouldCompress() bool {
	return !s.dontCompress && s.compressionType == CompressionNone && !s.format.IsBinaryFormat()
}

// GenerateBlobName generates a unique blob name for upload.
func (s *FileSource) GenerateBlobName() string {
	return GenerateBlobName(s)
}

// Close releases the file handle. Data reopens the file on its next call.
func (s *FileSource) Close() error {
	if s.stream == nil {
		return nil
	}
	stream := s.stream
	s.stream = nil
	return stream.Close()
}

// StreamSource represents a stream-based ingestion source.
type StreamSource struct {
	stream          io.ReadCloser
	format          DataFormat
	compressionType CompressionType
	sourceID        uuid.UUID
	name            string
	leaveOpen       bool
	closed          bool
	dontCompress    bool
}

// NewStreamSource creates a new StreamSource.
func NewStreamSource(stream io.ReadCloser, format DataFormat, opts ...StreamSourceOption) *StreamSource {
	s := &StreamSource{
		stream:          stream,
		format:          format,
		compressionType: CompressionNone,
		sourceID:        uuid.New(),
	}
	for _, opt := range opts {
		opt.applyToStream(s)
	}
	s.name = fmt.Sprintf("stream_%s%s%s", sanitizeUUID(s.sourceID), s.format, s.compressionType.String())
	return s
}

// StreamSourceOption configures a StreamSource.
type StreamSourceOption interface {
	applyToStream(*StreamSource)
}

type streamSourceOptionFunc func(*StreamSource)

func (f streamSourceOptionFunc) applyToStream(s *StreamSource) {
	f(s)
}

// WithStreamCompression sets the compression type for a StreamSource.
func WithStreamCompression(ct CompressionType) StreamSourceOption {
	return streamSourceOptionFunc(func(s *StreamSource) { s.compressionType = ct })
}

// WithStreamSourceID sets the source ID for a StreamSource.
func WithStreamSourceID(id uuid.UUID) StreamSourceOption {
	return streamSourceOptionFunc(func(s *StreamSource) { s.sourceID = id })
}

// WithLeaveOpen controls whether StreamSource.Close closes the underlying reader.
// All ingestion modes call Close when the complete ingestion call finishes.
func WithLeaveOpen(leaveOpen bool) StreamSourceOption {
	return streamSourceOptionFunc(func(s *StreamSource) { s.leaveOpen = leaveOpen })
}

func (s *StreamSource) Format() DataFormat               { return s.format }
func (s *StreamSource) CompressionType() CompressionType { return s.compressionType }
func (s *StreamSource) SourceID() uuid.UUID              { return s.sourceID }
func (s *StreamSource) Name() string                     { return s.name }

// Data returns the underlying data stream.
func (s *StreamSource) Data() (io.ReadCloser, error) {
	return s.stream, nil
}

// Size attempts to return the available bytes. Returns -1 if unknown.
func (s *StreamSource) Size() int64 {
	return -1
}

// ShouldCompress returns true if the stream data should be compressed during ingestion.
func (s *StreamSource) ShouldCompress() bool {
	return !s.dontCompress && s.compressionType == CompressionNone && !s.format.IsBinaryFormat()
}

// GenerateBlobName generates a unique blob name for upload.
func (s *StreamSource) GenerateBlobName() string {
	return GenerateBlobName(s)
}

// GenerateBlobName builds an upload name using the source's effective format and compression.
func GenerateBlobName(source LocalSource) string {
	effectiveCompression := source.CompressionType()
	if source.ShouldCompress() {
		effectiveCompression = CompressionGZip
	}
	ext := effectiveCompression.Extension()
	return fmt.Sprintf("%s_%s%s", sanitizeUUID(source.SourceID()), string(source.Format()), ext)
}

func (s *StreamSource) Close() error {
	if !s.leaveOpen && !s.closed && !validation.IsNil(s.stream) {
		s.closed = true
		return s.stream.Close()
	}
	return nil
}

// detectCompressionFromPath detects compression type from file extension.
func detectCompressionFromPath(path string) CompressionType {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".gz", ".gzip":
		return CompressionGZip
	case ".zip":
		return CompressionZip
	default:
		return CompressionNone
	}
}

func sanitizeUUID(id uuid.UUID) string {
	return strings.ReplaceAll(id.String(), "-", "")
}
