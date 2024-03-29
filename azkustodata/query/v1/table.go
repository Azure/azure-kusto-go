package v1

import (
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"github.com/Azure/azure-kusto-go/azkustodata/types"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
)

func NewTable(d query.BaseDataset, dt *RawTable, index *TableIndexRow) (query.Table, error) {
	var id string
	var kind string
	var name string
	var ordinal int64

	if index != nil {
		id = index.Id
		kind = index.Kind
		name = index.Name
		ordinal = index.Ordinal
	} else {
		// this case exists for the index table itself
		id = ""
		kind = ""
		name = dt.TableName
		ordinal = 0
	}

	op := d.Op()

	columns := make([]query.Column, len(dt.Columns))

	for i, c := range dt.Columns {
		columns[i] = query.NewColumn(i, c.ColumnName, types.Column(c.ColumnType))
		if !columns[i].Type().Valid() {
			return nil, errors.ES(op, errors.KClientArgs, "column[%d] if of type %q, which is not valid", i, c.ColumnType)
		}
	}

	baseTable := query.NewBaseTable(d, ordinal, id, name, kind, columns)

	rows := make([]query.Row, 0, len(dt.Rows))

	for i, r := range dt.Rows {
		if r.Errors != nil && len(r.Errors) > 0 {
			for _, e := range r.Errors {
				err := errors.ES(op, errors.KInternal, "row %d has an error: %s", i, e)
				return nil, err
			}
		}

		if r.Row == nil {
			continue
		}

		values := make(value.Values, len(r.Row))
		for j, v := range r.Row {
			parsed := value.Default(columns[j].Type())
			if v != nil {
				err := parsed.Unmarshal(v)
				if err != nil {
					return nil, errors.ES(op, errors.KInternal, "unable to unmarshal column %s into a %s value: %s", columns[j].Name(), columns[j].Type(), err)
				}
			}
			values[j] = parsed
		}
		rows = append(rows, query.NewRowFromParts(baseTable.Columns(), baseTable.ColumnByName, i, values))
	}
	return query.NewTable(baseTable, rows), nil
}
