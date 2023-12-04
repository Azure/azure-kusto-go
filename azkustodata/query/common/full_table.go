package common

import (
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	v1 "github.com/Azure/azure-kusto-go/azkustodata/query/v1"
	"github.com/Azure/azure-kusto-go/azkustodata/query/v2"
	"github.com/Azure/azure-kusto-go/azkustodata/types"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
	"strconv"
)

type fullTable struct {
	baseTable
	rows   []query.Row
	errors []error
}

func NewFullTableV2(dataSet query.Dataset, dt *v2.DataTable) (query.Table, error) {
	t := &fullTable{
		baseTable: baseTable{
			dataSet: dataSet,
			id:      strconv.Itoa(dt.TableId),
			ordinal: int64(dt.TableId),
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

func NewFullTableV1(dataSet query.Dataset, dt *v1.RawTable, index *v1.TableIndexRow) (query.Table, error) {
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

	t := &fullTable{
		baseTable: baseTable{
			dataSet: dataSet,
			id:      id,
			ordinal: ordinal,
			name:    name,
			kind:    kind,
			columns: make([]query.Column, len(dt.Columns)),
		},
		rows:   make([]query.Row, 0, len(dt.Rows)),
		errors: make([]error, 0, len(dt.Rows)),
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
		if r.Errors != nil && len(r.Errors) > 0 {
			for _, e := range r.Errors {
				t.errors = append(t.errors, errors.ES(op, errors.KInternal, "row[%d] error: %s", i, e))
			}
		}

		if r.Row == nil {
			continue
		}

		values := make(value.Values, len(r.Row))
		for j, v := range r.Row {
			parsed := value.Default(t.columns[j].Type())
			if v != nil {
				err := parsed.Unmarshal(v)
				if err != nil {
					return nil, errors.ES(op, errors.KInternal, "unable to unmarshal column %s into a %s value: %s", t.columns[j].Name, t.columns[j].Type, err)
				}
			}
			values[j] = parsed
		}
		t.rows = append(t.rows, NewRow(t, i, values))
	}

	return t, nil
}

func (t *fullTable) Consume() ([]query.Row, []error) {
	return t.rows, nil
}
