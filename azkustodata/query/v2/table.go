package v2

import (
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"github.com/Azure/azure-kusto-go/azkustodata/types"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
	"strconv"
)

func NewFullTable(d query.Dataset, dt *DataTable) (query.Table, error) {
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

	rows := make([]query.Row, len(dt.Rows))

	for i, r := range dt.Rows {
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
		rows[i] = query.NewRow(nil, i, values)
	}

	return query.NewFullTable(d, int64(dt.TableId), strconv.Itoa(dt.TableId), dt.TableName, dt.TableKind, columns, rows, nil), nil
}
