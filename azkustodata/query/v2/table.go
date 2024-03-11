package v2

import (
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"strconv"
)

func newBaseTable(dataset query.BaseDataset, th TableHeader) (query.BaseTable, error) {
	columns := make([]query.Column, len(th.Columns()))
	err := parseColumns(th, columns, dataset.Op())
	if err != nil {
		return nil, err
	}

	return query.NewBaseTable(dataset, int64(th.TableId()), strconv.Itoa(th.TableId()), th.TableName(), th.TableKind(), columns), nil
}

func newTable(dataset query.BaseDataset, dt DataTable) (query.Table, error) {
	base, err := newBaseTable(dataset, dt)
	if err != nil {
		return nil, err
	}

	rows := make([]query.Row, 0, len(dt.Rows()))

	for i, raw := range dt.Rows() {
		r, err := parseRow(raw, base, i)
		if err != nil {
			return nil, err
		}
		rows = append(rows, r)
	}

	return query.NewTable(base, rows), nil
}

type iterativeWrapper struct {
	table query.Table
}

func (f iterativeWrapper) Id() string { return f.table.Id() }

func (f iterativeWrapper) Index() int64 { return f.table.Index() }

func (f iterativeWrapper) Name() string { return f.table.Name() }

func (f iterativeWrapper) Columns() []query.Column { return f.table.Columns() }

func (f iterativeWrapper) Kind() string { return f.table.Kind() }

func (f iterativeWrapper) ColumnByName(name string) query.Column {
	return f.table.ColumnByName(name)
}

func (f iterativeWrapper) Op() errors.Op { return f.table.Op() }

func (f iterativeWrapper) IsPrimaryResult() bool { return f.table.IsPrimaryResult() }

func (f iterativeWrapper) ToTable() (query.Table, error) { return f.table, nil }

func (f iterativeWrapper) Rows() <-chan query.RowResult {
	ch := make(chan query.RowResult, len(f.table.Rows()))
	go func() {
		defer close(ch)
		for _, row := range f.table.Rows() {
			ch <- query.RowResultSuccess(row)
		}
	}()
	return ch
}

func (f iterativeWrapper) SkipToEnd() []error {
	return nil
}
