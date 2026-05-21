// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package utils

import (
	"encoding/json"
	"fmt"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

// BuildIngestionMessage creates the ingestion message payload that is sent
// to the DM service as part of the queued ingestion flow.
func BuildIngestionMessage(
	blobPath string,
	sourceID string,
	rawSize int64,
	database string,
	table string,
	props *ingestoptions.IngestRequestProperties,
) ([]byte, error) {
	msg := map[string]interface{}{
		"Id":                    sourceID,
		"BlobPath":              blobPath,
		"RawDataSize":           rawSize,
		"DatabaseName":          database,
		"TableName":             table,
		"RetainBlobOnSuccess":   true,
		"FlushImmediately":      false,
		"ReportLevel":           2,
		"ReportMethod":          0,
	}

	if props != nil {
		if props.Format != ingestoptions.FormatUnknown {
			msg["Format"] = props.Format.String()
		}
		if props.IngestionMappingRef != "" {
			msg["IngestionMappingReference"] = props.IngestionMappingRef
		}
		if props.FlushImmediately {
			msg["FlushImmediately"] = true
		}
		if props.IgnoreFirstRecord {
			msg["IgnoreFirstRecord"] = true
		}
		if props.IngestIfNotExists != "" {
			msg["IngestIfNotExists"] = props.IngestIfNotExists
		}
		if len(props.Tags) > 0 {
			msg["Tags"] = props.Tags
		}
		if len(props.DropByTags) > 0 {
			msg["DropByTags"] = props.DropByTags
		}
		if len(props.IngestByTags) > 0 {
			msg["IngestByTags"] = props.IngestByTags
		}
		if props.CreationTime != "" {
			msg["CreationTime"] = props.CreationTime
		}
		if props.ValidationPolicy != nil {
			msg["ValidationPolicy"] = fmt.Sprintf(
				`{"ValidationOptions":%q,"ValidationImplications":%q}`,
				props.ValidationPolicy.ValidationOptions,
				props.ValidationPolicy.ValidationImplications,
			)
		}
		for k, v := range props.AdditionalProperties {
			msg[k] = v
		}
	}

	return json.Marshal(msg)
}

// ParseIngestResponseJSON parses an ingest response from JSON.
func ParseIngestResponseJSON(data []byte) (*ingestoptions.IngestResponse, error) {
	var resp ingestoptions.IngestResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse ingest response: %w", err)
	}
	return &resp, nil
}
