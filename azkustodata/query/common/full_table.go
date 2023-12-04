package common

import (
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"github.com/Azure/azure-kusto-go/azkustodata/query/v2"
	"github.com/Azure/azure-kusto-go/azkustodata/types"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
)

type fullTable struct {
	baseTable
	rows []query.Row
}

func NewFullTable(dataSet query.Dataset, dt *v2.DataTable) (query.Table, error) {
	t := &fullTable{
		baseTable: baseTable{
			dataSet: dataSet,
			id:      dt.TableId,
			name:    dt.TableName,
			kind:    dt.TableKind,
			columns: make([]query.Column, len(dt.Columns)),
		},
		rows: make([]query.Row, len(dt.Rows)),
	}

	op := t.Op()

	for i, c := range dt.Columns {

		t.columns[i] = column{
			ordinal:   i,
			name:      c.ColumnName,
			kustoType: types.Column(c.ColumnType),
		}

		if !t.columns[i].Type().Valid() {
			return nil, errors.ES(op, errors.KClientArgs, "column[%d] if of type %q, which is not valid", i, c.ColumnType)
		}
	}

	for i, r := range dt.Rows {
		values := make(value.Values, len(r))
		for j, v := range r {
			parsed := value.Default(t.columns[j].Type())
			if v != nil {
				err := parsed.Unmarshal(v)
				if err != nil {
					return nil, errors.ES(op, errors.KInternal, "unable to unmarshal column %s into a %s value: %s", t.columns[j].Name, t.columns[j].Type, err)
				}
			}
			values[j] = parsed
		}
		t.rows[i] = NewRow(t, i, values)
	}

	return t, nil
}

func (t *fullTable) Consume() ([]query.Row, []error) {
	return t.rows, nil
}
