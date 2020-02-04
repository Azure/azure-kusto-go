package azkustoingest

import (
	"fmt"
	"github.com/satori/go.uuid"
	"strconv"
	"time"
)

type IngestionProperties struct {
	DatabaseName        string
	TableName           string
	FlushImmediately    bool
	IngestionMappingRef string
	ReportLevel         int
	ReportMethod        int
}

type AdditionalProperties struct {
	AuthContext          string `json:"authorizationContext,omitempty"`
	IngestionMapping     string `json:"ingestionMapping,omitempty"`
	IngestionMappingRef  string `json:"ingestionMappingReference,omitempty"`
	IngestionMappingType int    `json:"ingestionMappingType,omitempty"`
	ValidationPolicy     string `json:"ValidationPolicy,omitempty"`
	Format               string `json:"format,omitempty"`
	Tags                 bool   `json:"tags,omitempty"`
	IngestIfNotExists    string `json:"ingestIfNotExists,omitempty"`
}

type ingestionBlobInfo struct {
	Id                        string               `json:"Id"`
	BlobPath                  string               `json:"BlobPath"`
	RawDataSize               int                  `json:"RawDataSize,omitempty"`
	DatabaseName              string               `json:"DatabaseName"`
	TableName                 string               `json:"TableName"`
	RetainBlobOnSuccess       bool                 `json:"RetainBlobOnSuccess,omitempty"`
	FlushImmediately          bool                 `json:"FlushImmediately,omitempty"`
	IgnoreSizeLimit           bool                 `json:"IgnoreSizeLimit,omitempty"`
	ReportLevel               int                  `json:"ReportLevel,omitempty"`
	ReportMethod              int                  `json:"ReportMethod,omitempty"`
	SourceMessageCreationTime time.Time            `json:"RawDataSize,omitempty"`
	AdditionalProperties      AdditionalProperties `json:"AdditionalProperties"`
}

func newIngestionBlobInfo(source map[string]string, props IngestionProperties, auth string) *ingestionBlobInfo {
	sourceId, found := source["Id"]

	if !found {
		sourceId = fmt.Sprint(uuid.NewV4())
	}

	ibi := &ingestionBlobInfo{
		Id:                        sourceId,
		BlobPath:                  source["path"],
		DatabaseName:              props.DatabaseName,
		TableName:                 props.TableName,
		RetainBlobOnSuccess:       true,
		FlushImmediately:          props.FlushImmediately,
		IgnoreSizeLimit:           false,
		ReportLevel:               props.ReportLevel,
		ReportMethod:              props.ReportMethod,
		SourceMessageCreationTime: time.Now().UTC(),
		AdditionalProperties: AdditionalProperties{
			AuthContext: auth,
			IngestionMappingRef: props.IngestionMappingRef,
		},
	}

	if s, sizeExists := source["size"]; sizeExists {
		sourceSize, _ := strconv.ParseInt(s, 10, 32)
		ibi.RawDataSize = int(sourceSize)
	}

	return ibi
}
