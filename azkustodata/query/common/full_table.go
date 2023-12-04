package common

import (
	"github.com/Azure/azure-kusto-go/azkustodata/query"
)

type fullTable struct {
	baseTable
	rows   []query.Row
	errors []error
}

func NewFullTable(ds query.Dataset, ordinal int64, id string, name string, kind string, columns []query.Column, rows []query.Row, errors []error) (query.Table, error) {
	t := &fullTable{
		baseTable: baseTable{
			dataSet: ds,
			ordinal: ordinal,
			id:      id,
			name:    name,
			kind:    kind,
			columns: columns,
		},
		rows:   rows,
		errors: errors,
	}

	for _, r := range rows {
		if rr, ok := r.(*row); ok {
			rr.table = t
		}
	}

	return t, nil
}

func (t *fullTable) Consume() ([]query.Row, []error) {
	return t.rows, nil
}
