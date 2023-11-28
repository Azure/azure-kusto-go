package query

import (
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/types"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
)

type fullTable struct {
	baseTable
	rows []Row
}

func NewFullTable(dataSet *DataSet, dt *DataTable) (FullTable, error) {
	t := &fullTable{
		baseTable: baseTable{
			dataSet: dataSet,
			id:      dt.TableId,
			name:    dt.TableName,
			kind:    dt.TableKind,
			columns: make([]Column, len(dt.Columns)),
		},
		rows: make([]Row, len(dt.Rows)),
	}

	op := t.op()

	for i, c := range dt.Columns {

		t.columns[i] = Column{
			Ordinal: i,
			Name:    c.ColumnName,
			Type:    types.Column(c.ColumnType),
		}

		if !t.columns[i].Type.Valid() {
			return nil, errors.ES(op, errors.KClientArgs, "column[%d] if of type %q, which is not valid", i, c.ColumnType)
		}
	}

	for i, r := range dt.Rows {
		values := make(value.Values, len(r))
		for j, v := range r {
			parsed := value.Default(t.columns[j].Type)
			if v != nil {
				err := parsed.Unmarshal(v)
				if err != nil {
					return nil, errors.ES(op, errors.KInternal, "unable to unmarshal column %s into a %s value: %s", t.columns[j].Name, t.columns[j].Type, err)
				}
			}
			values[j] = parsed
		}
		t.rows[i] = *NewRow(t, i, values)
	}

	return t, nil
}

func (t *fullTable) Rows() []Row {
	return t.rows
}

func (t *fullTable) Consume() ([]Row, []error) {
	return t.rows, nil
}

type FullTable interface {
	Table
	Rows() []Row
}
