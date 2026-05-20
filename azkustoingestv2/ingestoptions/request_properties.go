// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package ingestoptions

import (
	"encoding/json"
	"fmt"
	"strings"
)

// IngestRequestProperties contains properties for an ingestion request.
type IngestRequestProperties struct {
	// Format is the data format.
	Format DataFormat
	// IngestionMappingRef is a reference to a pre-defined ingestion mapping.
	IngestionMappingRef string
	// IngestionMapping is an inline ingestion mapping definition.
	IngestionMapping []ColumnMapping
	// EnableTracking enables operation status tracking.
	EnableTracking bool
	// FlushImmediately requests immediate flushing of the ingestion queue.
	FlushImmediately bool
	// IgnoreFirstRecord indicates whether the first record should be ignored (header row).
	IgnoreFirstRecord bool
	// IgnoreLastRecordIfInvalid indicates whether the last record should be ignored if invalid.
	IgnoreLastRecordIfInvalid bool
	// Tags are custom tags associated with this ingestion operation.
	Tags []string
	// DropByTags are tags used for extent drop-by operations.
	DropByTags []string
	// IngestByTags are tags used for ingest-by deduplication.
	IngestByTags []string
	// IngestIfNotExists is a tag used for idempotent ingestion.
	IngestIfNotExists string
	// ValidationPolicy specifies the validation policy for the ingestion.
	ValidationPolicy *ValidationPolicy
	// CreationTime sets the creation time for the ingested data extents.
	CreationTime string
	// ZipPattern is a regex pattern for selecting files from a zip archive.
	ZipPattern string
	// ExtendSchema allows automatic schema extension on ingestion.
	ExtendSchema bool
	// RecreateSchema recreates the table schema on ingestion.
	RecreateSchema bool
	// SkipBatching disables batching and ingests immediately.
	SkipBatching bool
	// DeleteAfterDownload deletes the blob after download.
	DeleteAfterDownload bool
	// IgnoreSizeLimit ignores the size limit for uploads.
	IgnoreSizeLimit bool
	// AdditionalProperties holds any additional properties as key-value pairs.
	AdditionalProperties map[string]string
}

// Validate checks the properties for consistency errors.
func (p *IngestRequestProperties) Validate() error {
	if p.IngestionMappingRef != "" && len(p.IngestionMapping) > 0 {
		return NewIngestClientError(
			"cannot specify both ingestionMappingReference and inline ingestionMapping",
			nil, true,
		)
	}
	return nil
}

// SynthesizeTags combines Tags, DropByTags, and IngestByTags into a single tags list
// with the appropriate prefixes.
func (p *IngestRequestProperties) SynthesizeTags() []string {
	var result []string
	result = append(result, p.Tags...)
	for _, t := range p.IngestByTags {
		if !strings.HasPrefix(t, "ingest-by:") {
			result = append(result, "ingest-by:"+t)
		} else {
			result = append(result, t)
		}
	}
	for _, t := range p.DropByTags {
		if !strings.HasPrefix(t, "drop-by:") {
			result = append(result, "drop-by:"+t)
		} else {
			result = append(result, t)
		}
	}
	return result
}

// ColumnMapping defines an ingestion column mapping.
type ColumnMapping struct {
	// Name is the column name.
	Name string `json:"Name"`
	// MappingKind is the kind of mapping (e.g., "CsvMapping", "JsonMapping").
	MappingKind string `json:"Kind,omitempty"`
	// DataType is the Kusto data type (e.g., "string", "int", "datetime").
	DataType string `json:"DataType,omitempty"`
	// Properties holds format-specific mapping properties.
	Properties map[string]string `json:"Properties,omitempty"`
}

// SerializeColumnMappings serializes column mappings to a JSON string.
func SerializeColumnMappings(mappings []ColumnMapping) (string, error) {
	if len(mappings) == 0 {
		return "", nil
	}
	data, err := json.Marshal(mappings)
	if err != nil {
		return "", fmt.Errorf("failed to serialize column mappings: %w", err)
	}
	return string(data), nil
}

// ValidationPolicy defines the validation policy for ingestion.
type ValidationPolicy struct {
	// ValidationOptions specifies the validation options.
	ValidationOptions string
	// ValidationImplications specifies what happens on validation failure.
	ValidationImplications string
}

// IngestRequestPropertiesBuilder provides a fluent builder for IngestRequestProperties.
type IngestRequestPropertiesBuilder struct {
	props IngestRequestProperties
}

// NewIngestRequestPropertiesBuilder creates a new builder.
func NewIngestRequestPropertiesBuilder() *IngestRequestPropertiesBuilder {
	return &IngestRequestPropertiesBuilder{
		props: IngestRequestProperties{
			AdditionalProperties: make(map[string]string),
		},
	}
}

// WithFormat sets the data format.
func (b *IngestRequestPropertiesBuilder) WithFormat(format DataFormat) *IngestRequestPropertiesBuilder {
	b.props.Format = format
	return b
}

// WithMappingRef sets the ingestion mapping reference.
func (b *IngestRequestPropertiesBuilder) WithMappingRef(ref string) *IngestRequestPropertiesBuilder {
	b.props.IngestionMappingRef = ref
	return b
}

// WithMapping sets the inline ingestion mapping.
func (b *IngestRequestPropertiesBuilder) WithMapping(mapping []ColumnMapping) *IngestRequestPropertiesBuilder {
	b.props.IngestionMapping = mapping
	return b
}

// WithTracking enables operation tracking.
func (b *IngestRequestPropertiesBuilder) WithTracking(enable bool) *IngestRequestPropertiesBuilder {
	b.props.EnableTracking = enable
	return b
}

// WithFlushImmediately sets flush immediately.
func (b *IngestRequestPropertiesBuilder) WithFlushImmediately(flush bool) *IngestRequestPropertiesBuilder {
	b.props.FlushImmediately = flush
	return b
}

// WithIgnoreFirstRecord sets whether to ignore the first record.
func (b *IngestRequestPropertiesBuilder) WithIgnoreFirstRecord(ignore bool) *IngestRequestPropertiesBuilder {
	b.props.IgnoreFirstRecord = ignore
	return b
}

// WithIgnoreLastRecordIfInvalid sets whether to ignore the last record if invalid.
func (b *IngestRequestPropertiesBuilder) WithIgnoreLastRecordIfInvalid(ignore bool) *IngestRequestPropertiesBuilder {
	b.props.IgnoreLastRecordIfInvalid = ignore
	return b
}

// WithTags sets the ingestion tags.
func (b *IngestRequestPropertiesBuilder) WithTags(tags []string) *IngestRequestPropertiesBuilder {
	b.props.Tags = tags
	return b
}

// WithDropByTags sets the drop-by tags.
func (b *IngestRequestPropertiesBuilder) WithDropByTags(tags []string) *IngestRequestPropertiesBuilder {
	b.props.DropByTags = tags
	return b
}

// WithIngestByTags sets the ingest-by tags.
func (b *IngestRequestPropertiesBuilder) WithIngestByTags(tags []string) *IngestRequestPropertiesBuilder {
	b.props.IngestByTags = tags
	return b
}

// WithIngestIfNotExists sets the ingest-if-not-exists tag.
func (b *IngestRequestPropertiesBuilder) WithIngestIfNotExists(tag string) *IngestRequestPropertiesBuilder {
	b.props.IngestIfNotExists = tag
	return b
}

// WithCreationTime sets the creation time for ingested extents.
func (b *IngestRequestPropertiesBuilder) WithCreationTime(ct string) *IngestRequestPropertiesBuilder {
	b.props.CreationTime = ct
	return b
}

// WithZipPattern sets the zip file pattern.
func (b *IngestRequestPropertiesBuilder) WithZipPattern(pattern string) *IngestRequestPropertiesBuilder {
	b.props.ZipPattern = pattern
	return b
}

// WithExtendSchema enables automatic schema extension.
func (b *IngestRequestPropertiesBuilder) WithExtendSchema(extend bool) *IngestRequestPropertiesBuilder {
	b.props.ExtendSchema = extend
	return b
}

// WithRecreateSchema enables schema recreation.
func (b *IngestRequestPropertiesBuilder) WithRecreateSchema(recreate bool) *IngestRequestPropertiesBuilder {
	b.props.RecreateSchema = recreate
	return b
}

// WithSkipBatching disables batching.
func (b *IngestRequestPropertiesBuilder) WithSkipBatching(skip bool) *IngestRequestPropertiesBuilder {
	b.props.SkipBatching = skip
	return b
}

// WithAdditionalProperty sets an additional property.
func (b *IngestRequestPropertiesBuilder) WithAdditionalProperty(key, value string) *IngestRequestPropertiesBuilder {
	b.props.AdditionalProperties[key] = value
	return b
}

// Build returns the constructed IngestRequestProperties after validation.
func (b *IngestRequestPropertiesBuilder) Build() (*IngestRequestProperties, error) {
	result := b.props
	if err := result.Validate(); err != nil {
		return nil, err
	}
	return &result, nil
}
