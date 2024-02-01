package query

import "github.com/Azure/azure-kusto-go/azkustodata/errors"

// dataTable is a basic implementation of Table, to be used by specific implementations.
// It contains all the rows and errors for the table.
type dataTable struct {
	baseTable
	rows  []Row
	error error
}

func NewDataTable(ds Dataset, ordinal int64, id string, name string, kind string, columns []Column, rows []Row, errs ...error) Table {
	t := &dataTable{
		baseTable: *NewTable(ds, ordinal, id, name, kind, columns).(*baseTable),
		rows:      rows,
		error:     errors.TryCombinedError(errs...),
	}

	for _, r := range rows {
		if rr, ok := r.(*row); ok {
			rr.table = t
		}
	}

	return t
}

func (t *dataTable) ToFullTable() (FullTable, error) {
	if t.error != nil {
		return nil, t.error
	}
	return NewFullTable(&t.baseTable, t.rows), nil
}
