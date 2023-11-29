package ingestoptions

import (
	"fmt"

	"github.com/Azure/azure-kusto-go/kusto"
)

// CompressionType is a file's compression type.
type CompressionType int8

// String implements fmt.Stringer.
func (c CompressionType) String() string {
	switch c {
	case GZIP:
		return "gzip"
	case ZIP:
		return "zip"
	}
	return "unknown compression type"
}

//goland:noinspection GoUnusedConst - Part of the API
const (
	// CTUnknown indicates that that the compression type was unset.
	CTUnknown CompressionType = 0
	// CTNone indicates that the file was not compressed.
	CTNone CompressionType = 1
	// GZIP indicates that the file is GZIP compressed.
	GZIP CompressionType = 2
	// ZIP indicates that the file is ZIP compressed.
	ZIP CompressionType = 3
)

// DataFormat indicates what type of encoding format was used for source data.
// Note: This is very similar to ingest.DataFormat, except this supports more formats.
// We are not using a shared list, because this list is used only internally and is for the
// data itself, not the mapping reference.  Structure prevents packages such as filesystem
// from importing ingest, because ingest imports filesystem.  We would end up with recursive imports.
// More info here: https://docs.microsoft.com/en-us/azure/kusto/management/data-ingestion/
type DataFormat int

const (
	// DFUnknown indicates the EncodingType is not set.
	DFUnknown DataFormat = 0
	// AVRO indicates the source is encoded in Apache Avro format.
	AVRO DataFormat = 1
	// ApacheAVRO indicates the source is encoded in Apache avro2json format.
	ApacheAVRO DataFormat = 2
	// CSV indicates the source is encoded in comma seperated values.
	CSV DataFormat = 3
	// JSON indicates the source is encoded as one or more lines, each containing a record in Javascript Object Notation.
	JSON DataFormat = 4
	// MultiJSON indicates the source is encoded in JSON-Array of individual records in Javascript Object Notation. Optionally,
	//multiple documents can be concatenated.
	MultiJSON DataFormat = 5
	// ORC indicates the source is encoded in Apache Optimized Row Columnar format.
	ORC DataFormat = 6
	// Parquet indicates the source is encoded in Apache Parquet format.
	Parquet DataFormat = 7
	// PSV is pipe "|" separated values.
	PSV DataFormat = 8
	// Raw is a text file that has only a single string value.
	Raw DataFormat = 9
	// SCSV is a file containing semicolon ";" separated values.
	SCSV DataFormat = 10
	// SOHSV is a file containing SOH-separated values(ASCII codepoint 1).
	SOHSV DataFormat = 11
	// SStream indicats the source is encoded as a Microsoft Cosmos Structured Streams format
	SStream DataFormat = 12
	// TSV is a file containing tab seperated values ("\t").
	TSV DataFormat = 13
	// TSVE is a file containing escaped-tab seperated values ("\t").
	TSVE DataFormat = 14
	// TXT is a text file with lines delimited by "\n".
	TXT DataFormat = 15
	// W3CLogFile indicates the source is encoded using W3C Extended Log File format.
	W3CLogFile DataFormat = 16
	// SingleJSON indicates the source is a single JSON value -- newlines are regular whitespace.
	SingleJSON DataFormat = 17
)

type dfDescriptor struct {
	camelName        string
	jsonName         string
	detectableExt    string
	validMappingKind bool
}

var dfDescriptions = []dfDescriptor{
	{"", "", "", false},
	{"Avro", "avro", ".avro", true},
	{"ApacheAvro", "avro", "", false},
	{"Csv", "csv", ".csv", true},
	{"Json", "json", ".json", true},
	{"MultiJson", "multijson", "", false},
	{"Orc", "orc", ".orc", true},
	{"Parquet", "parquet", ".parquet", true},
	{"Psv", "psv", ".psv", false},
	{"Raw", "raw", ".raw", false},
	{"Scsv", "scsv", ".scsv", false},
	{"Sohsv", "sohsv", ".sohsv", false},
	{"SStream", "sstream", ".ss", false},
	{"Tsv", "tsv", ".tsv", false},
	{"Tsve", "tsve", ".tsve", false},
	{"Txt", "txt", ".txt", false},
	{"W3cLogFile", "w3clogfile", ".w3clogfile", false},
	{"SingleJson", "singlejson", "", false},
}

// String implements fmt.Stringer.
func (d DataFormat) String() string {
	if d > 0 && int(d) < len(dfDescriptions) {
		return dfDescriptions[d].jsonName
	}

	return ""
}

// CamelCase returns the CamelCase version. This is for internal use, do not use.
// This can be removed in future versions.
func (d DataFormat) CamelCase() string {
	if d > 0 && int(d) < len(dfDescriptions) {
		return dfDescriptions[d].camelName
	}

	return ""
}

func (d DataFormat) KnownOrDefault() kusto.DataFormatForStreaming {
	if d == DFUnknown {
		return CSV
	}

	return d
}

// MarshalJSON implements json.Marshaler.MarshalJSON.
func (d DataFormat) MarshalJSON() ([]byte, error) {
	if d == 0 {
		return nil, fmt.Errorf("DataFormat is an invalid encoding type")
	}

	return []byte(fmt.Sprintf("%q", d.String())), nil
}

// IsValidMappingKind returns true if a dataformat can be used as a MappingKind.
func (d DataFormat) IsValidMappingKind() bool {
	if d > 0 && int(d) < len(dfDescriptions) {
		return dfDescriptions[d].validMappingKind
	}

	return false
}
