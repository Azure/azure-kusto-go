package azkustoingest

import (
	"fmt"
	"github.com/satori/go.uuid"
	"time"
)

type IngestionBlobInfo struct {
	properties map[string]interface{}
}

func NewIngestionBlobInfo(source map[string]string, props map[string]string, auth string) (*IngestionBlobInfo) {
	var properties = make(map[string]interface{})

	properties["BlobPath"] = source["path"]
	properties["BlobPath"] = source["path"]
	properties["RawDataSize"] = source["size"]
	properties["DatabaseName"] = props["database"]
	properties["TableName"] = props["table"]
	properties["RetainBlobOnSuccess"] = true
	properties["FlushImmediately"] = props["flushImmediately"]
	properties["IgnoreSizeLimit"] = false
	properties["ReportLevel"] = props["reportLevel"]
	properties["ReportMethod"] = props["reportMethod"]
	properties["SourceMessageCreationTime"] = fmt.Sprint(time.Now().UTC())

	if source_id, found := props["strID"]; found {
		properties["Id"] = source_id
	} else {
		properties["Id"] = fmt.Sprint(uuid.Must(uuid.NewV4()))
	}

	return &IngestionBlobInfo{
		properties: properties,
	}
}