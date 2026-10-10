// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package test

import (
	"testing"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

func TestIngestServiceError(t *testing.T) {
	err := ingestoptions.NewIngestServiceError("service error", nil, 500, "")
	if err.IsPermanent {
		t.Error("service error should be non-permanent")
	}
	if err.FailureCode != 500 {
		t.Errorf("expected failure code 500, got %d", err.FailureCode)
	}
}

func TestUploadFailedError(t *testing.T) {
	err := ingestoptions.NewUploadFailedError(ingestoptions.UploadErrorSourceIsNull, "test.csv", nil)
	if !err.IsPermanent {
		t.Error("upload failed error should be permanent")
	}
	if err.ErrorCode != ingestoptions.UploadErrorSourceIsNull {
		t.Errorf("expected error code SOURCE_IS_NULL, got %s", err.ErrorCode)
	}
	if err.FileName != "test.csv" {
		t.Errorf("expected file name test.csv, got %s", err.FileName)
	}
}

func TestRequestPropertiesValidation_MappingConflict(t *testing.T) {
	props := &ingestoptions.IngestRequestProperties{
		IngestionMappingRef: "myMapping",
		IngestionMapping:    []ingestoptions.ColumnMapping{{Name: "col1"}},
	}
	err := props.Validate()
	if err == nil {
		t.Error("expected error for mapping ref + inline mapping conflict")
	}
}

func TestRequestPropertiesValidation_OK(t *testing.T) {
	props := &ingestoptions.IngestRequestProperties{
		IngestionMappingRef: "myMapping",
		Format:              ingestoptions.FormatCSV,
	}
	err := props.Validate()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestSynthesizeTags(t *testing.T) {
	props := &ingestoptions.IngestRequestProperties{
		Tags:         []string{"custom-tag"},
		IngestByTags: []string{"myTag"},
		DropByTags:   []string{"drop1", "drop-by:drop2"},
	}
	tags := props.SynthesizeTags()

	expected := map[string]bool{
		"custom-tag":      true,
		"ingest-by:myTag": true,
		"drop-by:drop1":   true,
		"drop-by:drop2":   true,
	}

	if len(tags) != len(expected) {
		t.Errorf("expected %d tags, got %d: %v", len(expected), len(tags), tags)
	}
	for _, tag := range tags {
		if !expected[tag] {
			t.Errorf("unexpected tag: %s", tag)
		}
	}
}

func TestSerializeColumnMappings(t *testing.T) {
	mappings := []ingestoptions.ColumnMapping{
		{
			Name:     "col1",
			DataType: "string",
			Properties: map[string]string{
				"Path": "$.field1",
			},
		},
	}
	result, err := ingestoptions.SerializeColumnMappings(mappings)
	if err != nil {
		t.Fatal(err)
	}
	if result == "" {
		t.Error("expected non-empty JSON")
	}
	// Verify it's valid JSON
	if result[0] != '[' {
		t.Errorf("expected JSON array, got %q", result[:1])
	}
}

func TestSerializeColumnMappings_Empty(t *testing.T) {
	result, err := ingestoptions.SerializeColumnMappings(nil)
	if err != nil {
		t.Fatal(err)
	}
	if result != "" {
		t.Errorf("expected empty string for nil mappings, got %q", result)
	}
}

func TestBuilderWithValidation(t *testing.T) {
	builder := ingestoptions.NewIngestRequestPropertiesBuilder()
	_, err := builder.
		WithFormat(ingestoptions.FormatCSV).
		WithMappingRef("ref").
		WithMapping([]ingestoptions.ColumnMapping{{Name: "col"}}).
		Build()
	if err == nil {
		t.Error("expected error for conflicting mapping ref and inline mapping")
	}
}

func TestBuilderSuccess(t *testing.T) {
	props, err := ingestoptions.NewIngestRequestPropertiesBuilder().
		WithFormat(ingestoptions.FormatJSON).
		WithMappingRef("myMapping").
		WithTracking(true).
		WithTags([]string{"tag1"}).
		WithDropByTags([]string{"dt1"}).
		WithIngestByTags([]string{"it1"}).
		WithZipPattern("*.csv").
		WithExtendSchema(true).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	if props.Format != ingestoptions.FormatJSON {
		t.Errorf("expected JSON format, got %s", props.Format)
	}
	if !props.EnableTracking {
		t.Error("expected tracking enabled")
	}
	if !props.ExtendSchema {
		t.Error("expected extend schema enabled")
	}
}
