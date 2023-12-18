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

func (d *baseDataset) parseSecondaryTable(t query.Table) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	var errs []error

	switch t.Kind() {
	case QueryPropertiesKind:
		if d.queryProperties != nil {
			return errors.ES(errors.OpUnknown, errors.KInternal, "query properties already initialized")
		}
		rows, err := t.GetAllRows()
		if err != nil {
			errs = append(errs, err...)
		}
		if len(rows) > 0 {
			st, errs := query.ToStructs[QueryProperties](rows)
			d.queryProperties = st
			if errs != nil {
				return errors.GetCombinedError(errs...)
			}
		}
		if errs != nil {
			return errors.GetCombinedError(errs...)
		}
		return nil

	case QueryCompletionInformationKind:
		if d.queryCompletionInformation != nil {
			return errors.ES(errors.OpUnknown, errors.KInternal, "query completion already initialized")
		}
		rows, err := t.GetAllRows()
		if err != nil {
			errs = append(errs, err...)
		}
		if len(rows) > 0 {
			st, errs := query.ToStructs[QueryCompletionInformation](rows)
			d.queryCompletionInformation = st
			if errs != nil {
				return errors.GetCombinedError(errs...)
			}
		}
		if errs != nil {
			return errors.GetCombinedError(errs...)
		}
		return nil

	default:
		return errors.ES(errors.OpUnknown, errors.KInternal, "unknown secondary table %s", t.Name())
	}
}
