// Package properties provides Kusto REST properties that will need to be serialized and sent to Kusto
// based upon the type of ingestion we are doing.
package properties

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
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

// String implements fmt.Stringer.
func (d DataFormat) String() string {
	ext, err := dfToExt(d)
	if err != nil {
		return ""
	}
	return ext
}

// CamelCase returns the CamelCase version. This is for internal use, do not use.
// This can be removed in future versions.
func (d DataFormat) CamelCase() string {
	cc, err := dfToCamel(d)
	if err != nil {
		return ""
	}
	return cc
}

// MarshalJSON implements json.Marshaler.MarshalJSON.
func (d DataFormat) MarshalJSON() ([]byte, error) {
	if d == 0 {
		return nil, fmt.Errorf("DataFormat is an invalid compression type")
	}
	return []byte(fmt.Sprintf("%q", d.String())), nil
}

const (
	// DFUnknown indicates the EncodingType is not set.
	DFUnknown DataFormat = 0
	// CSV indicates the source is encoded in comma seperated values.
	CSV DataFormat = 1
	// JSON indicates the source is encoded in Javscript Object Notation.
	JSON DataFormat = 2
	// AVRO indicates the source is encoded in Apache Avro format.
	AVRO DataFormat = 3
	// Parquet indicates the source is encoded in Apache Parquet format.
	Parquet DataFormat = 4
	// ORC indicates the source is encoded in Apache Optimized Row Columnar format.
	ORC DataFormat = 5
	// PSV is pipe "|" separated values.
	PSV DataFormat = 6
	// Raw is a text file that has only a single string value.
	Raw DataFormat = 7
	// SCSV is a file containing semicolon ";" separated values.
	SCSV DataFormat = 8
	// SOHSV is a file containing SOH-separated values(ASCII codepont 1).
	SOHSV DataFormat = 9
	// TSV is a file containing table seperated values ("\t").
	TSV DataFormat = 10
	// TXT is a text file with lines deliminated by "\n".
	TXT DataFormat = 11
)

func dfToExt(et DataFormat) (string, error) {
	switch et {
	case CSV:
		return "csv", nil
	case JSON:
		return "json", nil
	case AVRO:
		return "avro", nil
	case Parquet:
		return "parquet", nil
	case ORC:
		return "orc", nil
	case PSV:
		return "psv", nil
	case Raw:
		return "raw", nil
	case SCSV:
		return "scsv", nil
	case SOHSV:
		return "sohsv", nil
	case TSV:
		return "tsv", nil
	case TXT:
		return "txt", nil
	default:
		return "", fmt.Errorf("EncodingType(%v) was no one we understand", et)
	}
}

func dfToCamel(et DataFormat) (string, error) {
	switch et {
	case CSV:
		return "Csv", nil
	case JSON:
		return "Json", nil
	case AVRO:
		return "Avro", nil
	case Parquet:
		return "Parquet", nil
	case ORC:
		return "Orc", nil
	case PSV:
		return "Psv", nil
	case Raw:
		return "Raw", nil
	case SCSV:
		return "Scsv", nil
	case SOHSV:
		return "Sohsv", nil
	case TSV:
		return "Tsv", nil
	case TXT:
		return "Txt", nil
	default:
		return "", fmt.Errorf("EncodingType(%v) was no one we understand", et)
	}
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
