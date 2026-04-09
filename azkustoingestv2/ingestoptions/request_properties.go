// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package ingestoptions

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
	// AdditionalProperties holds any additional properties as key-value pairs.
	AdditionalProperties map[string]string
}

// ColumnMapping defines an ingestion column mapping.
type ColumnMapping struct {
	// Name is the column name.
	Name string `json:"Name"`
	// MappingKind is the kind of mapping (e.g., "CsvMapping", "JsonMapping").
	MappingKind string `json:"Kind,omitempty"`
	// Ordinal is the column ordinal (for CSV mappings).
	Ordinal int `json:"Ordinal,omitempty"`
	// ConstantValue is a constant value for the column.
	ConstantValue string `json:"ConstValue,omitempty"`
	// Path is the JSON path (for JSON mappings).
	Path string `json:"Properties,omitempty"`
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

// WithTags sets the ingestion tags.
func (b *IngestRequestPropertiesBuilder) WithTags(tags []string) *IngestRequestPropertiesBuilder {
	b.props.Tags = tags
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

// WithAdditionalProperty sets an additional property.
func (b *IngestRequestPropertiesBuilder) WithAdditionalProperty(key, value string) *IngestRequestPropertiesBuilder {
	b.props.AdditionalProperties[key] = value
	return b
}

// Build returns the constructed IngestRequestProperties.
func (b *IngestRequestPropertiesBuilder) Build() *IngestRequestProperties {
	result := b.props
	return &result
}
