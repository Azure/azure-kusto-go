package v2

import (
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"github.com/Azure/azure-kusto-go/azkustodata/types"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
	"strconv"
)

// NewDataTable creates a new query.Table from a DataTable frame.
func NewDataTable(d query.Dataset, dt *DataTable) (query.Table, error) {
	op := errors.OpUnknown
	if d != nil {
		op = d.Op()
	}

	columns := make([]query.Column, len(dt.Columns))

	for i, c := range dt.Columns {
		columns[i] = query.NewColumn(i, c.ColumnName, types.Column(c.ColumnType))
		if !columns[i].Type().Valid() {
			return nil, errors.ES(op, errors.KClientArgs, "column[%d] if of type %q, which is not valid", i, c.ColumnType)
		}
	}

	rows := make([]query.Row, 0, len(dt.Rows))
	errs := make([]error, 0, len(dt.Rows))

	for i, raw := range dt.Rows {
		if raw.Errors != nil {
			for _, e := range raw.Errors {
				errs = append(errs, &e)
			}
			continue
		}
		r := raw.Row
		values := make(value.Values, len(r))
		for j, v := range r {
			parsed := value.Default(columns[j].Type())
			if v != nil {
				err := parsed.Unmarshal(v)
				if err != nil {
					return nil, errors.ES(op, errors.KInternal, "unable to unmarshal column %s into a %s value: %s", columns[j].Name(), columns[j].Type(), err)
				}
			}
			values[j] = parsed
		}
		rows = append(rows, query.NewRow(nil, i, values))
	}

	return query.NewDataTable(d, int64(dt.TableId), strconv.Itoa(dt.TableId), dt.TableName, dt.TableKind, columns, rows, errs...), nil
}

func parseColumns(th *TableHeader, columns []query.Column, op errors.Op) *errors.Error {
	for i, c := range th.Columns {
		columns[i] = query.NewColumn(i, c.ColumnName, types.Column(c.ColumnType))
		if !columns[i].Type().Valid() {
			return errors.ES(op, errors.KClientArgs, "column[%d] if of type %q, which is not valid", i, c.ColumnType)
		}
	}
	return nil
}

func parseRow(r []interface{}, t table) (query.Row, *errors.Error) {
	values := make(value.Values, len(r))
	columns := t.Columns()
	for j, v := range r {
		parsed := value.Default(columns[j].Type())
		err := parsed.Unmarshal(v)

		if err != nil {
			return nil, errors.ES(t.Op(), errors.KInternal, "unable to unmarshal column %s into A %s value: %s", columns[j].Name(), columns[j].Type(), err)
		}
		values[j] = parsed
	}
	row := query.NewRow(t, t.RowCount(), values)
	return row, nil
}

// table is an interface that represents a table that can be added to a dataset.
// It is implemented by both streamingTable and fragmentedTable.
type table interface {
	query.Table
	RowCount() int
	addRawRows(rows RawRows)
	close([]OneApiError)
}
