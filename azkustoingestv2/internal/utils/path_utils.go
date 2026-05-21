// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package utils

import (
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
	"github.com/google/uuid"
)

// GenerateBlobName generates a unique blob name for ingestion.
// Format: {database}__{table}__{guid}__{originalName}{compressionExt}
func GenerateBlobName(database, table, originalName string, compression ingestoptions.CompressionType) string {
	id := uuid.New().String()
	ext := ""
	if compression == ingestoptions.CompressionGZip {
		ext = ".gz"
	}
	name := sanitizeFileName(originalName)
	return fmt.Sprintf("%s__%s__%s__%s%s", database, table, id, name, ext)
}

// GenerateBlobPath creates a full blob path in a container.
func GenerateBlobPath(containerURL, blobName string) string {
	return strings.TrimRight(containerURL, "/") + "/" + blobName
}

// sanitizeFileName removes illegal characters from a file name.
func sanitizeFileName(name string) string {
	// Take just the base name
	name = path.Base(name)
	// Replace problematic characters
	replacer := strings.NewReplacer(
		" ", "_",
		"\\", "_",
		"/", "_",
		":", "_",
		"*", "_",
		"?", "_",
		"\"", "_",
		"<", "_",
		">", "_",
		"|", "_",
	)
	return replacer.Replace(name)
}

// waitDuration returns a channel that receives after the given duration.
func waitDuration(d time.Duration) <-chan time.Time {
	return time.After(d)
}

// GetBlobExtension returns the file extension for the given data format.
func GetBlobExtension(format ingestoptions.DataFormat) string {
	switch format {
	case ingestoptions.FormatCSV:
		return ".csv"
	case ingestoptions.FormatJSON:
		return ".json"
	case ingestoptions.FormatMultiJSON:
		return ".multijson"
	case ingestoptions.FormatAvro:
		return ".avro"
	case ingestoptions.FormatApacheAvro:
		return ".avro"
	case ingestoptions.FormatParquet:
		return ".parquet"
	case ingestoptions.FormatORC:
		return ".orc"
	case ingestoptions.FormatTSV:
		return ".tsv"
	case ingestoptions.FormatSStream:
		return ".ss"
	case ingestoptions.FormatPSV:
		return ".psv"
	case ingestoptions.FormatSCSV:
		return ".scsv"
	case ingestoptions.FormatSOHSV:
		return ".sohsv"
	case ingestoptions.FormatTSVE:
		return ".tsve"
	case ingestoptions.FormatW3CLOGFILE:
		return ".w3clogfile"
	default:
		return ".dat"
	}
}
