package query

import (
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/google/uuid"
	"time"
)

type QueryProperties struct {
	TableId int
	Key     string
	Value   map[string]interface{}
}

type QueryCompletionInformation struct {
	Timestamp        time.Time
	ClientRequestId  string
	ActivityId       uuid.UUID
	SubActivityId    uuid.UUID
	ParentActivityId uuid.UUID
	Level            int
	LevelName        string
	StatusCode       int
	StatusCodeName   string
	EventType        int
	EventTypeName    string
	Payload          string
}

var errorTableUninitialized = errors.ES(errors.OpUnknown, errors.KInternal, "Table uninitialized")

const QueryPropertiesKind = "QueryProperties"
const QueryCompletionInformationKind = "QueryCompletionInformation"

func (d *DataSet) QueryProperties() ([]QueryProperties, error) {
	if d.SecondaryResults == nil {
		return nil, errorTableUninitialized
	}

	if d.queryProperties != nil {
		return d.queryProperties, nil
	}

	for _, t := range d.SecondaryResults {
		if t.Kind() == QueryPropertiesKind {
			rows := t.(FullTable).Rows()
			d.queryProperties = make([]QueryProperties, len(rows))
			for i, r := range rows {
				err := r.ToStruct(&d.queryProperties[i])
				if err != nil {
					return nil, err
				}
			}

			return d.queryProperties, nil
		}
	}

	errorTableUninitialized.Op = d.op()
	return nil, errorTableUninitialized
}

func (d *DataSet) QueryCompletionInformation() ([]QueryCompletionInformation, error) {
	if d.SecondaryResults == nil {
		return nil, errorTableUninitialized
	}

	if d.queryCompletionInformation != nil {
		return d.queryCompletionInformation, nil
	}

	for _, t := range d.SecondaryResults {
		if t.Kind() == QueryCompletionInformationKind {
			rows := t.(FullTable).Rows()
			d.queryCompletionInformation = make([]QueryCompletionInformation, len(rows))
			for i, r := range rows {
				err := r.ToStruct(&d.queryCompletionInformation[i])
				if err != nil {
					return nil, err
				}
			}

			return d.queryCompletionInformation, nil
		}
	}

	errorTableUninitialized.Op = d.op()
	return nil, errorTableUninitialized
}
