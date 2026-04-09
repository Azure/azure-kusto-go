// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package ingestoptions

// CompressionType specifies the compression type for ingestion sources.
type CompressionType int

const (
	// CompressionNone indicates no compression.
	CompressionNone CompressionType = iota
	// CompressionGZip indicates GZip compression.
	CompressionGZip
	// CompressionZip indicates Zip compression.
	CompressionZip
)

// String returns the string representation of the compression type.
func (c CompressionType) String() string {
	switch c {
	case CompressionGZip:
		return "gz"
	case CompressionZip:
		return "zip"
	default:
		return ""
	}
}

// Extension returns the file extension for this compression type.
func (c CompressionType) Extension() string {
	switch c {
	case CompressionGZip:
		return ".gz"
	case CompressionZip:
		return ".zip"
	default:
		return ""
	}
}

// DataFormat represents data formats supported by Kusto ingestion.
type DataFormat string

const (
	FormatUnknown       DataFormat = ""
	FormatCSV           DataFormat = "csv"
	FormatJSON          DataFormat = "json"
	FormatMultiJSON     DataFormat = "multijson"
	FormatAvro          DataFormat = "avro"
	FormatApacheAvro    DataFormat = "apacheavro"
	FormatParquet       DataFormat = "parquet"
	FormatORC           DataFormat = "orc"
	FormatTSV           DataFormat = "tsv"
	FormatSCSV          DataFormat = "scsv"
	FormatSOHSV         DataFormat = "sohsv"
	FormatPSV           DataFormat = "psv"
	FormatTXT           DataFormat = "txt"
	FormatRAW           DataFormat = "raw"
	FormatSingleJSON    DataFormat = "singlejson"
	FormatW3CLOGFILE    DataFormat = "w3clogfile"
	FormatTSVE          DataFormat = "tsve"
	FormatSStream       DataFormat = "sstream"
)

// String returns the string representation of the data format.
func (f DataFormat) String() string {
	return string(f)
}

// IsBinaryFormat returns true if the format is a binary format that should not be compressed.
func (f DataFormat) IsBinaryFormat() bool {
	switch f {
	case FormatAvro, FormatApacheAvro, FormatParquet, FormatORC:
		return true
	default:
		return false
	}
}

// CompressionStrategy defines the interface for compression strategies.
type CompressionStrategy interface {
	// Compress compresses data from src and writes to dst.
	// Returns the number of bytes written.
	Compress(src []byte) ([]byte, error)
}
