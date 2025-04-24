package properties

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestIngestionJSONMarshal(t *testing.T) {
	ingestion := Ingestion{
		ID:                        uuid.MustParse("9854e507-5060-4fed-be22-e909780245fb"),
		BlobPath:                  "https://test.blob.core.windows.net/test/test.csv",
		DatabaseName:              "NetDefaultDB",
		TableName:                 "TestTable",
		RawDataSize:               1337,
		RetainBlobOnSuccess:       true,
		FlushImmediately:          true,
		IgnoreSizeLimit:           true,
		ReportLevel:               FailureAndSuccess,
		ReportMethod:              ReportStatusToTable,
		SourceMessageCreationTime: time.Unix(0, 0).UTC(),
		Additional: Additional{
			AuthContext:          "e30=",
			IngestionMapping:     "Map",
			IngestionMappingRef:  "MapRef",
			IngestionMappingType: ApacheAVRO,
			ValidationPolicy:     "{}",
			Format:               ApacheAVRO,
			IgnoreFirstRecord:    true,
			Tags:                 []string{"blue", "green"},
			IngestIfNotExists:    "yellow",
			CreationTime:         time.Unix(0, 0).UTC(),
		},
		TableEntryRef: StatusTableDescription{
			TableConnectionString: "connString",
			PartitionKey:          "10f76b1f-0c57-4844-a354-a08d7b9ee627",
			RowKey:                "00000000-0000-0000-0000-000000000000",
		},
		ApplicationForTracing:   "app;test",
		ClientVersionForTracing: "TraceValue",
	}

	expected := map[string]any{
		"Id":                        "9854e507-5060-4fed-be22-e909780245fb",
		"BlobPath":                  "https://test.blob.core.windows.net/test/test.csv",
		"DatabaseName":              "NetDefaultDB",
		"TableName":                 "TestTable",
		"RawDataSize":               float64(1337),
		"RetainBlobOnSuccess":       true,
		"FlushImmediately":          true,
		"IgnoreSizeLimit":           true,
		"ReportLevel":               float64(2),
		"ReportMethod":              float64(1),
		"SourceMessageCreationTime": "1970-01-01T00:00:00Z",
		"AdditionalProperties": map[string]any{
			"authorizationContext":      "e30=",
			"ingestionMapping":          "Map",
			"ingestionMappingReference": "MapRef",
			"ingestionMappingType":      "ApacheAvro",
			"validationPolicy":          "{}",
			"format":                    "avro",
			"ignoreFirstRecord":         true,
			"tags":                      "[\"blue\",\"green\"]",
			"ingestIfNotExists":         "yellow",
			"creationTime":              "1970-01-01T00:00:00Z",
		},
		"IngestionStatusInTable": map[string]any{
			"TableConnectionString": "connString",
			"PartitionKey":          "10f76b1f-0c57-4844-a354-a08d7b9ee627",
			"RowKey":                "00000000-0000-0000-0000-000000000000",
		},
		"ApplicationForTracing":   "app;test",
		"ClientVersionForTracing": "TraceValue",
	}

	j, err := json.Marshal(ingestion)
	assert.NoError(t, err)

	var actual map[string]any
	err = json.Unmarshal(j, &actual)
	assert.Equal(t, expected, actual)
}
