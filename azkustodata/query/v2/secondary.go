package v2

import (
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
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

const QueryPropertiesKind = "QueryProperties"
const QueryCompletionInformationKind = "QueryCompletionInformation"

func (d *streamingDataset) setQueryProperties(queryProperties []QueryProperties) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.queryProperties = queryProperties
}

func (d *streamingDataset) setQueryCompletionInformation(queryCompletionInformation []QueryCompletionInformation) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.queryCompletionInformation = queryCompletionInformation
}

func (d *baseDataset) parseSecondaryTable(t query.Table) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	switch t.Kind() {
	case QueryPropertiesKind:
		if d.queryProperties != nil {
			return errors.ES(errors.OpUnknown, errors.KInternal, "query properties already initialized")
		}
		rows, err := t.GetAllRows()
		if err != nil {
			return errors.GetCombinedError(err...)
		}

		st, errs := query.ToStructs[QueryProperties](rows)
		if errs != nil {
			return errs
		}

		d.queryProperties = st

	case QueryCompletionInformationKind:
		if d.queryCompletionInformation != nil {
			return errors.ES(errors.OpUnknown, errors.KInternal, "query properties already initialized")
		}
		rows, err := t.GetAllRows()
		if err != nil {
			return errors.GetCombinedError(err...)
		}

		st, errs := query.ToStructs[QueryCompletionInformation](rows)
		if errs != nil {
			return errs
		}

		d.queryCompletionInformation = st
	default:
		return errors.ES(errors.OpUnknown, errors.KInternal, "unknown secondary table %s", t.Name())
	}
	return nil
}