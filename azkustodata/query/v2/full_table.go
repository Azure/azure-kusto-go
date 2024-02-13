package v2

import (
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
)

func newFullTable(dataset query.Dataset, dt DataTable) (query.FullTable, error) {
	base, err := newTable(dataset, dt)
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

	return query.NewFullTable(base, rows), nil
}

type iterativeWrapper struct {
	fullTable query.FullTable
}

func (f iterativeWrapper) Id() string { return f.fullTable.Id() }

func (f iterativeWrapper) Index() int64 { return f.fullTable.Index() }

func (f iterativeWrapper) Name() string { return f.fullTable.Name() }

func (f iterativeWrapper) Columns() []query.Column { return f.fullTable.Columns() }

func (f iterativeWrapper) Kind() string { return f.fullTable.Kind() }

func (f iterativeWrapper) ColumnByName(name string) query.Column {
	return f.fullTable.ColumnByName(name)
}

func (f iterativeWrapper) Op() errors.Op { return f.fullTable.Op() }

func (f iterativeWrapper) IsPrimaryResult() bool { return f.fullTable.IsPrimaryResult() }

func (f iterativeWrapper) ToFullTable() (query.FullTable, error) { return f.fullTable, nil }

func (f iterativeWrapper) Rows() <-chan query.RowResult {
	ch := make(chan query.RowResult, len(f.fullTable.Rows()))
	go func() {
		defer close(ch)
		for _, row := range f.fullTable.Rows() {
			ch <- query.RowResultSuccess(row)
		}
	}()
	return ch
}

func (f iterativeWrapper) SkipToEnd() []error {
	return nil
}
