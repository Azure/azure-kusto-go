// Package properties provides Kusto REST properties that will need to be serialized and sent to Kusto
// based upon the type of ingestion we are doing.
package properties

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
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

// MarshalJSON implements json.Marshaler.MarshalJSON.
func (c CompressionType) MarshalJSON() ([]byte, error) {
	if c == 0 {
		return nil, fmt.Errorf("CTUnknown is an invalid compression type")
	}
	return []byte(fmt.Sprintf("%q", c.String())), nil
}

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
	// JSON indicates the source is encoded as one or more lines, each containing a record in Javscript Object Notation.
	JSON DataFormat = 4
	// MultiJSON indicates the source is encoded in JSON-Array of individual records in Javscript Object Notation.
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
	// SOHSV is a file containing SOH-separated values(ASCII codepont 1).
	SOHSV DataFormat = 11
	// SingleJSON indicates the source containg a single Javascript Object Notation encoded record, newlines are treated as whitespace
	SingleJSON DataFormat = 12
	// SStream indicats the source is encoded as a Microsoft Cosmos Structured Streams format
	SStream DataFormat = 13
	// TSV is a file containing tab seperated values ("\t").
	TSV DataFormat = 14
	// TSVE is a file containing escaped-tab seperated values ("\t").
	TSVE DataFormat = 15
	// TXT is a text file with lines deliminated by "\n".
	TXT DataFormat = 16
	// WCLogFile indicates the source is encoded using W3C Extended Log File format
	WCLogFile DataFormat = 17
)

type dfDescriptor struct {
	camelName        string
	jsonName         string
	detectableExt    string
	MappingReq       bool
	validMappingKind bool
}

var dfDescriptions = []dfDescriptor{
	{"", "", "", false, false},
	{"Avro", "avro", ".avro", true, true},
	{"ApacheAvro", "avro", "", true, false},
	{"Csv", "csv", ".csv", false, true},
	{"Json", "json", ".json", true, true},
	{"MultiJson", "json", "", true, false},
	{"Orc", "orc", ".orc", false, true},
	{"Parquet", "parquet", ".parquet", false, true},
	{"Psv", "psv", ".psv", false, false},
	{"Raw", "raw", ".raw", false, false},
	{"Scsv", "scsv", ".scsv", false, false},
	{"Sohsv", "sohsv", ".sohsv", false, false},
	{"SingleJson", "json", "", true, false},
	{"SStream", "sstream", ".sstream", false, false},
	{"Tsv", "tsv", ".tsv", false, false},
	{"Tsve", "tsve", ".tsve", false, false},
	{"Txt", "txt", ".txt", false, false},
	{"EcLogFile", "eclogfile", ".eclogfile", false, false},
}

// String implements fmt.Stringer.
func (d DataFormat) String() string {
	ext, err := d.jsonName()
	if err != nil {
		return ""
	}

	return ext
}

// CamelCase returns the CamelCase version. This is for internal use, do not use.
// This can be removed in future versions.
func (d DataFormat) CamelCase() string {
	cc, err := d.camelCase()
	if err != nil {
		return ""
	}

	return cc
}

// MarshalJSON implements json.Marshaler.MarshalJSON.
func (d DataFormat) MarshalJSON() ([]byte, error) {
	if d == 0 {
		return nil, fmt.Errorf("DataFormat is an invalid encoding type")
	}

	return []byte(fmt.Sprintf("%q", d.String())), nil
}

// jsonName returns the file extension that it would use.
func (d DataFormat) jsonName() (string, error) {
	if d > 0 && int(d) < len(dfDescriptions) {
		return dfDescriptions[d].jsonName, nil
	}

	return "", fmt.Errorf("EncodingType(%d) was no one we understand", d)
}

func (d DataFormat) camelCase() (string, error) {
	if d > 0 && int(d) < len(dfDescriptions) {
		return dfDescriptions[d].camelName, nil
	}

	return "", fmt.Errorf("EncodingType(%d) was no one we understand", d)
}

// RequiresMapping indicates whether a data format must be provided with valid mapping file information.
func (d DataFormat) RequiresMapping() bool {
	if d > 0 && int(d) < len(dfDescriptions) {
		return dfDescriptions[d].MappingReq
	}

	return false
}

// IsValidMappingKind returns true if a dataformat can be used as a MappingKind
func (d DataFormat) IsValidMappingKind() bool {
	if d > 0 && int(d) < len(dfDescriptions) {
		return dfDescriptions[d].validMappingKind
	}

	return false
}

// DataFormatDiscovery looks at the file name and tries to discern what the file format is.
func DataFormatDiscovery(fName string) DataFormat {
	name := fName

	u, err := url.Parse(fName)
	if err == nil && u.Scheme != "" {
		name = u.Path
	}

	ext := strings.ToLower(filepath.Ext(strings.TrimSuffix(strings.TrimSuffix(strings.ToLower(name), ".zip"), ".gz")))

	if ext == "" {
		return DFUnknown
	}

	for i := 1; i < len(dfDescriptions); i++ {
		if ext == dfDescriptions[i].detectableExt {
			return DataFormat(i)
		}
	}

	return DFUnknown
}

// All holds the complete set of properties that might be used.
type All struct {
	// Ingestion is a set of properties that are used across all ingestion methods.
	Ingestion Ingestion
	// Source provides options that are used are used when doing an ingestion on a filesystem.
	Source SourceOptions
}

// SourceOptions are options that the user provides about the source file that is going to be uploaded.
type SourceOptions struct {
	// ID allows someone to set the UUID for upload themselves. We aren't providing this option at this time, but here
	// when we do.
	ID uuid.UUID

	// DeleteLocalSource indicates to delete the local file after it has been consumed.
	DeleteLocalSource bool
}

// Ingestion is a JSON serializable set of options that must be provided to the service.
type Ingestion struct {
	// ID is the unqique UUID for this upload.
	ID uuid.UUID `json:"Id"`
	// BlobPath is the URI representing the blob.
	BlobPath string
	// DatabaseName is the name of the Kusto database the data will ingest into.
	DatabaseName string
	// TableName is the name of the Kusto table the the data will ingest into.
	TableName string
	// RawDataSize is the size of the file on the filesystem, if it was provided.
	RawDataSize int64 `json:",omitempty"`
	// RetainBlobOnSuccess indicates if the source blob should be retained or deleted.
	RetainBlobOnSuccess bool `json:",omitempty"`
	// Daniel:
	// FlushImmediately ... I know what flushing means, but in terms of here, do we not return until the Kusto
	// table is updated, does this mean we do....  This is really a duplicate comment on the options in ingest.go
	FlushImmediately bool
	// Daniel:
	// IgnoreSizeLimit
	IgnoreSizeLimit bool `json:",omitempty"`
	ReportLevel     int  `json:",omitempty"`
	ReportMethod    int  `json:",omitempty"`
	// SourceMessageCreationTime is when we created the blob.
	SourceMessageCreationTime time.Time  `json:",omitempty"`
	Additional                Additional `json:"AdditionalProperties"`
}

// Additional is additional properites.
type Additional struct {
	// AuthContext is the authorization string that we get from resources.Manager.AuthContext().
	AuthContext string `json:"authorizationContext,omitempty"`
	// IngestionMapping is a json string that maps the data being imported to the table's columns.
	// See: https://docs.microsoft.com/en-us/azure/kusto/management/data-ingestion/
	IngestionMapping string `json:"ingestionMapping,omitempty"`
	// IngestionMappingRef is a string representing a mapping reference that has been uploaded to the server
	// via a Mgmt() call. See: https://docs.microsoft.com/en-us/azure/kusto/management/create-ingestion-mapping-command
	IngestionMappingRef string `json:"ingestionMappingReference,omitempty"`
	// IngestionMappingType is what the mapping reference is encoded in: csv, json, avro, ...
	IngestionMappingType DataFormat `json:"ingestionMappingType,omitempty"`
	// ValidationPolicy is a JSON encoded string that tells our ingestion action what policies we want on the
	// data being ingested and what to do when that is violated.
	ValidationPolicy string     `json:"validationPolicy,omitempty"`
	Format           DataFormat `json:"format,omitempty"`
	// Tags is a list of tags to associated with the ingested data.
	Tags []string `json:"tags,omitempty"`
	// IngestIfNotExists is a string value that, if specified, prevents ingestion from succeeding if the table already
	// has data tagged with an ingest-by: tag with the same value. This ensures idempotent data ingestion.
	IngestIfNotExists string `json:"ingestIfNotExists,omitempty"`
}

// MarshalJSON implements json.Marshaller. This is for use only by the SDK and may be removed at any time.
func (a Additional) MarshalJSON() ([]byte, error) {
	// TODO(daniel): Have the backend fixed.
	// OK: This is here because in .Net DataFormat and IngestionMappingType are two different enumerators.
	// For some reason, they encode the values in two different ways and do exact string matches on the server.
	// So you must use "csv" and "Csv". For the moment, until we can get a backend change, we have to encode these
	// differently. I don't want to have two enumerators for the same thing, so I've done this hack to get around it.

	type additional2 Additional

	b, err := json.Marshal(additional2(a))
	if err != nil {
		return nil, err
	}

	m := map[string]interface{}{}
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}

	if _, ok := m["ingestionMappingType"]; ok {
		m["ingestionMappingType"] = a.IngestionMappingType.CamelCase()
	}

	return json.Marshal(m)
}

// MarshalJSONString will marshal Ingestion into a base64 encoded string.
func (i Ingestion) MarshalJSONString() (base64String string, err error) {
	i = i.defaults()
	if err := i.validate(); err != nil {
		return "", err
	}

	j, err := json.Marshal(i)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(j), nil
}

// defaults sets default values that can be auto-generated if not set. This is used inside our MarshalJSONString().
func (i Ingestion) defaults() Ingestion {
	if uuidIsZero(i.ID) {
		i.ID = uuid.New()
	}

	if i.SourceMessageCreationTime.IsZero() {
		i.SourceMessageCreationTime = time.Now()
	}

	return i
}

func (i Ingestion) validate() error {
	if uuidIsZero(i.ID) {
		return fmt.Errorf("the ID cannot be an zero value UUID")
	}
	switch "" {
	case i.DatabaseName:
		return fmt.Errorf("the database name cannot be an empty string")
	case i.TableName:
		return fmt.Errorf("the table name cannot be an empty string")
	case i.Additional.AuthContext:
		return fmt.Errorf("the authorization context was an empty string, which is not allowed")
	case i.BlobPath:
		return fmt.Errorf("the BlobPath was not set")
	}
	return nil
}

func uuidIsZero(id uuid.UUID) bool {
	for _, b := range id {
		if b != 0 {
			return false
		}
	}
	return true
}
