// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package azkustoingestv2

import (
	"net/url"
	"path/filepath"
	"strings"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

func normalizeIngestProperties(source ingestoptions.IngestionSource, props *ingestoptions.IngestRequestProperties) *ingestoptions.IngestRequestProperties {
	var effective ingestoptions.IngestRequestProperties
	if props != nil {
		effective = *props
	}
	effective.Format = resolveIngestFormat(source, effective.Format)
	return &effective
}

func resolveIngestFormat(source ingestoptions.IngestionSource, requested ingestoptions.DataFormat) ingestoptions.DataFormat {
	if requested != ingestoptions.FormatUnknown {
		return requested
	}
	if source == nil {
		return ingestoptions.FormatCSV
	}
	if format := source.Format(); format != ingestoptions.FormatUnknown {
		return format
	}

	var name string
	switch s := source.(type) {
	case *ingestoptions.FileSource:
		name = s.Path()
	case *ingestoptions.BlobSource:
		name = s.BlobPath()
		if u, err := url.Parse(name); err == nil && u.Host != "" {
			name = u.Path
		}
	}
	name = strings.ToLower(name)
	for _, suffix := range []string{".zip", ".gz", ".gzip"} {
		name = strings.TrimSuffix(name, suffix)
	}
	extension := filepath.Ext(name)
	switch extension {
	case ".ss":
		return ingestoptions.FormatSStream
	case ".avro", ".csv", ".json", ".orc", ".parquet", ".psv", ".raw",
		".scsv", ".sohsv", ".tsv", ".tsve", ".txt", ".w3clogfile":
		return ingestoptions.DataFormat(strings.TrimPrefix(extension, "."))
	default:
		return ingestoptions.FormatCSV
	}
}

// Keep the original stream and ownership while applying the request's upload format.
type formattedLocalSource struct {
	ingestoptions.LocalSource
	format ingestoptions.DataFormat
}

func (s formattedLocalSource) Format() ingestoptions.DataFormat {
	return s.format
}

func (s formattedLocalSource) ShouldCompress() bool {
	return s.LocalSource.ShouldCompress() && !s.format.IsBinaryFormat()
}

func (s formattedLocalSource) GenerateBlobName() string {
	return ingestoptions.GenerateBlobName(s)
}
